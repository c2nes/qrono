use std::fmt::Display;
use std::io::{BufWriter, ErrorKind, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{mpsc, Arc};
use std::thread::JoinHandle;
use std::{io, thread};

use bytes::{Buf, BytesMut};
use log::{error, info, trace, warn};
use parking_lot::Mutex;
use slab::Slab;

use protocol::Error;

use crate::io::ReadInto;
use crate::promise::{QronoFuture, QronoPromise};
use crate::redis::protocol;
use crate::redis::protocol::Value;
use crate::redis::request::{Request, Response};
use crate::result::IgnoreErr;
use crate::scheduler::{Scheduler, State, Task, TaskContext, TaskError, TaskFuture, TaskHandle};
use crate::service::Qrono;

pub struct RedisServer {
    qrono: Arc<Qrono>,
    scheduler: Scheduler,
}

struct Client {
    qrono: Arc<Qrono>,
    conn: TcpStream,

    responses: Sender<Response>,
    response_writer: Arc<TaskHandle<ResponseWriter>>,
    response_writer_future: Option<TaskFuture<ResponseWriter>>,

    client_id: usize,
    client_conns: Arc<Mutex<Slab<TcpStream>>>,
    shutdown: Arc<AtomicBool>,
}

impl Client {
    fn new(
        qrono: Arc<Qrono>,
        scheduler: &Scheduler,
        conn: TcpStream,
        shutdown: Arc<AtomicBool>,
        client_conns: Arc<Mutex<Slab<TcpStream>>>,
    ) -> io::Result<Client> {
        let (tx, rx) = mpsc::channel::<Response>();
        let writer = ResponseWriter::new(&conn, rx)?;
        let (task_handle, task_future) = scheduler.register(writer);
        let response_writer = Arc::new(task_handle);
        let client_id = client_conns.lock().insert(conn.try_clone()?);

        Ok(Client {
            qrono,
            conn,
            responses: tx,
            response_writer,
            response_writer_future: Some(task_future),
            client_conns,
            client_id,
            shutdown,
        })
    }

    fn respond_now(&self, resp: Response) {
        self.responses.send(resp).unwrap();
        self.response_writer.schedule().ignore_err();
    }

    fn respond_later<Resp, F: FnOnce(QronoFuture<Resp>) -> Response>(
        &self,
        factory: F,
    ) -> QronoPromise<Resp> {
        let response_writer = Arc::clone(&self.response_writer);
        let (mut resp_promise, resp_future) = QronoFuture::new();
        resp_promise.on_complete(move || response_writer.schedule().ignore_err());
        self.responses.send(factory(resp_future)).unwrap();
        resp_promise
    }

    fn route(&self, request: Request) {
        match request {
            Request::Enqueue(queue, req) => {
                let resp = self.respond_later(Response::Enqueue);
                self.qrono.enqueue(&queue, req, resp);
            }
            Request::Dequeue(queue, req) => {
                let resp = self.respond_later(Response::Dequeue);
                self.qrono.dequeue(&queue, req, resp);
            }
            Request::Requeue(queue, req) => {
                let resp = self.respond_later(Response::Requeue);
                self.qrono.requeue(&queue, req, resp);
            }
            Request::Release(queue, req) => {
                let resp = self.respond_later(Response::Release);
                self.qrono.release(&queue, req, resp);
            }
            Request::Info(queue, req) => {
                let resp = self.respond_later(Response::Info);
                self.qrono.info(&queue, req, resp);
            }
            Request::Peek(queue, req) => {
                let resp = self.respond_later(Response::Peek);
                self.qrono.peek(&queue, req, resp);
            }
            Request::Delete(queue, req) => {
                let resp = self.respond_later(Response::Delete);
                self.qrono.delete(&queue, req, resp);
            }
            Request::Compact(queue, req) => {
                let resp = self.respond_later(Response::Compact);
                self.qrono.compact(&queue, req, resp);
            }
            Request::Ping(msg) => {
                self.respond_now(Response::Value(match msg {
                    Some(msg) => Value::BulkString(msg),
                    None => Value::SimpleString("PONG".to_string()),
                }));
            }
        }
    }

    fn try_run(&mut self) -> io::Result<()> {
        // Socket setup
        self.conn.set_nodelay(true)?;

        // Request read loop
        let mut buf = BytesMut::with_capacity(8 * 1024);
        'read: while !self.response_writer_future.as_ref().unwrap().is_complete() {
            // Ensure we have space for more data, growing the buffer if necessary.
            buf.reserve(1024);

            let n = self.conn.read_into(&mut buf)?;
            if n == 0 {
                break;
            }

            while buf.has_remaining() {
                match Value::try_from(&mut buf) {
                    Ok(value) => {
                        let req = match Request::parse(value) {
                            Ok(req) => req,
                            Err(resp) => {
                                self.respond_now(resp);
                                continue;
                            }
                        };

                        self.route(req);
                    }
                    Err(Error::UnexpectedEof) => break,
                    Err(Error::ProtocolError(msg)) => {
                        self.respond_now(Response::Error(format!(
                            "ERR protocol error, closing connection: {msg}"
                        )));
                        break 'read;
                    }
                }
            }
        }

        self.response_writer.cancel();
        let (mut writer, result) = self.response_writer_future.take().unwrap().take();
        if let Err(TaskError::Failed(err)) = result {
            error!("Response writer failed: {err}")
        } else if let Err(err) = writer.finish() {
            error!("Error writing data to client: {err}")
        }

        Ok(())
    }

    fn run(&mut self) {
        let addr = self
            .conn
            .peer_addr()
            .ok()
            .map(|addr| addr.to_string())
            .unwrap_or_else(|| "<unknown>".to_string());

        match self.try_run() {
            Err(err) if !self.shutdown.load(Ordering::Relaxed) => {
                error!("Connection {addr} closed due to unexpected error, {err:?}")
            }
            _ => info!("Connection {addr} closed"),
        }
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        self.client_conns.lock().remove(self.client_id);
    }
}

struct ResponseWriter {
    conn: TcpStream,
    writer: BufWriter<TcpStream>,
    responses: Receiver<Response>,
    next: Option<Response>,
    buf: Vec<u8>,
}

impl ResponseWriter {
    fn new(conn: &TcpStream, responses: Receiver<Response>) -> io::Result<ResponseWriter> {
        let conn = conn.try_clone()?;
        let writer = BufWriter::new(conn.try_clone()?);
        Ok(ResponseWriter {
            conn,
            writer,
            responses,
            next: None,
            buf: vec![],
        })
    }

    fn shutdown_connection(&self) {
        if let Err(err) = self.conn.shutdown(Shutdown::Both) {
            match err.kind() {
                ErrorKind::NotConnected => (), /* The connection is already closed. */
                _ => error!("Error shutting down connection: {}", err),
            }
        }
    }

    fn check_result<T>(&self, result: io::Result<T>) -> io::Result<T> {
        if let Err(err) = &result {
            warn!("Closing connection. Error writing data to client: {}", err);
            // Shutting down the connection ensures the read loop is woken
            self.shutdown_connection();
        }
        result
    }

    fn next(&mut self) -> &mut Option<Response> {
        if self.next.is_none() {
            self.next = self.responses.try_recv().ok()
        }
        &mut self.next
    }

    fn write_responses(&mut self, count: usize) -> io::Result<()> {
        let mut flush = false;
        for _ in 0..count {
            match self.next().take() {
                Some(resp) if resp.is_ready() => {
                    self.buf.clear();
                    resp.take().put(&mut self.buf);
                    let res = self.writer.write_all(&self.buf[..]);
                    self.check_result(res)?;
                    flush = true;
                }
                next => {
                    self.next = next;
                    break;
                }
            }
        }
        if flush {
            let res = self.writer.flush();
            self.check_result(res)?;
        }
        Ok(())
    }

    fn finish(&mut self) -> io::Result<()> {
        let send_error = false;
        while let Some(resp) = self.next().take() {
            self.buf.clear();
            if !send_error && resp.is_ready() {
                resp.take().put(&mut self.buf);
            } else {
                Value::StaticError("ERR server shutting down").put(&mut self.buf);
            }
            self.writer.write_all(&self.buf[..])?;
        }

        self.writer.flush()
    }
}

impl Task for ResponseWriter {
    type Value = ();
    type Error = io::Error;

    fn run(&mut self, _ctx: &TaskContext<Self>) -> Result<State<()>, io::Error> {
        self.write_responses(128)?;
        if let Some(resp) = self.next() {
            if resp.is_ready() {
                return Ok(State::Runnable);
            }
        }
        Ok(State::Idle)
    }
}

impl RedisServer {
    pub fn new(qrono: Arc<Qrono>, scheduler: &Scheduler) -> Self {
        let scheduler = scheduler.clone();
        Self { qrono, scheduler }
    }

    pub fn start<A: ToSocketAddrs + Display>(self, addr: A) -> io::Result<ServerHandle> {
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = shutdown.clone();

        let client_conns: Arc<Mutex<Slab<TcpStream>>> = Arc::new(Mutex::new(Slab::new()));
        let addr = addr.to_socket_addrs()?.collect::<Vec<_>>();
        let listener = TcpListener::bind(&addr[..])?;
        let join_handle = thread::spawn(move || {
            thread::scope(|scope| {
                for conn in listener.incoming() {
                    if shutdown.load(Ordering::Relaxed) {
                        trace!("Shutdown signal received");
                        drop(conn);
                        shutdown.store(true, Ordering::Relaxed);
                        for (_, conn) in client_conns.lock().iter() {
                            if let Err(err) = conn.shutdown(Shutdown::Read) {
                                match err.kind() {
                                    ErrorKind::NotConnected => (), /* The connection is already closed. */
                                    _ => error!("Error shutting down connection: {}", err),
                                }
                            }
                        }
                        trace!("Shutdown complete");
                        break;
                    }

                    let conn = conn?;
                    let addr = conn.peer_addr()?;
                    let mut client = Client::new(
                        self.qrono.clone(),
                        &self.scheduler,
                        conn,
                        Arc::clone(&shutdown),
                        Arc::clone(&client_conns),
                    )?;
                    info!("Accepted RESP2 connection from {}", &addr);

                    thread::Builder::new()
                        .name(format!("QronoClient[{}]", &addr))
                        .spawn_scoped(scope, move || client.run())
                        .unwrap();
                }
                Ok(())
            })
        });
        Ok(ServerHandle {
            join_handle,
            shutdown: shutdown_clone,
            addr,
        })
    }
}

pub struct ServerHandle {
    join_handle: JoinHandle<io::Result<()>>,
    shutdown: Arc<AtomicBool>,
    addr: Vec<SocketAddr>,
}

impl ServerHandle {
    pub fn shutdown(self) -> io::Result<()> {
        self.shutdown.store(true, Ordering::Relaxed);
        let _ = std::net::TcpStream::connect(&self.addr[..])?;
        self.join_handle.join().unwrap()
    }
}
