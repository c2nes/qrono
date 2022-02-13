use std::io::{ErrorKind, Write};
use std::net::{Shutdown, TcpListener, TcpStream};

use std::path::PathBuf;

use std::sync::{mpsc, Arc};
use std::time::Instant;
use std::{fs, io, thread};

use bytes::{Buf, BytesMut};
use log::{error, info, warn};

use qrono::id_generator::IdGenerator;
use qrono::io::ReadBufUninitialized;

use qrono::promise::Future;
use qrono::redis::protocol::Error::{Incomplete, ProtocolError};
use qrono::redis::protocol::{RedisBuf, Value};
use qrono::redis::request::{Request, Response};
use qrono::scheduler::{Scheduler, StaticPool};

use qrono::service::Qrono;
use qrono::timer;
use qrono::working_set::WorkingSet;
use rayon::ThreadPoolBuilder;
use structopt::StructOpt;

#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

// TODO:
//  - Error handling (audit unwrap calls)
//  - Working item timeouts and TTLs

// Done
//  - Compact working set
//  - Configurable storage paths
//  - Compact segments
//  - Reload queues on startup
//  - Deleting queues

// Goal of compaction: Remove tombstoned entries.
// Equivalently, reduce the on-disk data to only unreleased entries.
// Unreleased entries include pending and working items.
// All pending items are > LAST. All released items are < LAST.
// All working items are < LAST.

fn shutdown_connection(conn: &TcpStream) {
    if let Err(err) = conn.shutdown(Shutdown::Both) {
        match err.kind() {
            ErrorKind::NotConnected => (), /* The connection is already closed. */
            _ => error!("Error shutting down connection: {}", err),
        }
    }
}

fn handle_client(qrono: Qrono, scheduler: Scheduler, mut conn: TcpStream) -> io::Result<()> {
    conn.set_nodelay(true).unwrap();

    let (resp_tx, resp_rx) = mpsc::channel::<Response>();

    let schedule = {
        let mut resp_head: Option<Response> = None;
        let mut writer = std::io::BufWriter::new(conn.try_clone()?);
        let mut buf = Vec::new();

        let conn = conn.try_clone()?;
        let handle_io_result = move |res| {
            if let Err(err) = &res {
                warn!("Closing connection. Error writing data to client: {}", err);
                shutdown_connection(&conn);
                return false;
            }

            true
        };

        let (handle, _) = scheduler.register_fn(move || {
            buf.clear();

            let mut flush = false;
            for _ in 0..128 {
                if let Some(future) = &resp_head {
                    if future.is_ready() {
                        let val = resp_head.take().unwrap().take();
                        buf.put_value(&val);
                        if !handle_io_result(writer.write_all(&buf[..])) {
                            return false;
                        }
                        buf.clear();
                        flush = true;
                    } else {
                        if flush {
                            handle_io_result(writer.flush());
                        }

                        return false;
                    }
                }

                assert!(resp_head.is_none());

                resp_head = match resp_rx.try_recv() {
                    Ok(response) => Some(response),
                    Err(_) => {
                        handle_io_result(writer.flush());
                        return false;
                    }
                };
            }

            handle_io_result(writer.flush())
        });

        Arc::new(handle)
    };

    let mut frame = BytesMut::with_capacity(8 * 1024);

    loop {
        // Ensure we have space for more data, growing the buffer if necessary.
        frame.reserve(1024);

        let n = conn.read_buf(&mut frame)?;
        if n == 0 {
            break;
        }

        while frame.has_remaining() {
            match Value::from_bytes(&frame) {
                Ok((value, n)) => {
                    frame.advance(n);

                    let req = match Request::parse(&value) {
                        Ok(req) => req,
                        Err(resp) => {
                            resp_tx.send(resp).unwrap();
                            schedule.schedule();
                            continue;
                        }
                    };

                    let schedule = Arc::clone(&schedule);

                    match req {
                        Request::Enqueue(queue, req) => {
                            let (mut promise, resp) = Future::new();
                            promise.on_complete(move || schedule.schedule());
                            resp_tx.send(Response::Enqueue(resp)).unwrap();
                            qrono.enqueue(queue, req, promise);
                        }
                        Request::Dequeue(queue, req) => {
                            let (mut promise, resp) = Future::new();
                            promise.on_complete(move || schedule.schedule());
                            resp_tx.send(Response::Dequeue(resp)).unwrap();
                            qrono.dequeue(queue, req, promise);
                        }
                        Request::Requeue(queue, req) => {
                            let (mut promise, resp) = Future::new();
                            promise.on_complete(move || schedule.schedule());
                            resp_tx.send(Response::Requeue(resp)).unwrap();
                            qrono.requeue(queue, req, promise);
                        }
                        Request::Release(queue, req) => {
                            let (mut promise, resp) = Future::new();
                            promise.on_complete(move || schedule.schedule());
                            resp_tx.send(Response::Release(resp)).unwrap();
                            qrono.release(queue, req, promise);
                        }
                        Request::Info(queue, req) => {
                            let (mut promise, resp) = Future::new();
                            promise.on_complete(move || schedule.schedule());
                            resp_tx.send(Response::Info(resp)).unwrap();
                            qrono.info(queue, req, promise);
                        }
                        Request::Peek(queue, req) => {
                            let (mut promise, resp) = Future::new();
                            promise.on_complete(move || schedule.schedule());
                            resp_tx.send(Response::Peek(resp)).unwrap();
                            qrono.peek(queue, req, promise);
                        }
                        Request::Delete(queue, req) => {
                            let (mut promise, resp) = Future::new();
                            promise.on_complete(move || schedule.schedule());
                            resp_tx.send(Response::Delete(resp)).unwrap();
                            qrono.delete(queue, req, promise);
                        }
                        Request::Compact(queue, req) => {
                            let (mut promise, resp) = Future::new();
                            promise.on_complete(move || schedule.schedule());
                            resp_tx.send(Response::Compact(resp)).unwrap();
                            qrono.compact(queue, req, promise);
                        }
                        Request::Ping(msg) => {
                            resp_tx
                                .send(Response::Value(match msg {
                                    Some(msg) => Value::BulkString(msg),
                                    None => Value::SimpleString("PING".to_string()),
                                }))
                                .unwrap();
                            schedule.schedule();
                        }
                    }
                }
                Err(e) => match e {
                    Incomplete => break,
                    ProtocolError(_err) => {
                        resp_tx
                            .send(Response::Error("ERR protocol error".to_owned()))
                            .unwrap();

                        schedule.schedule();
                    }
                },
            }
        }
    }
    Ok(())
}

#[derive(Debug, StructOpt)]
#[structopt(name = "qrono")]
struct Opts {
    /// Listen address
    #[structopt(long, default_value = "0.0.0.0:16389")]
    listen: String,

    /// Data directory
    #[structopt(long, parse(from_os_str), default_value = "/tmp/qrono")]
    data: PathBuf,

    /// Number of CPU worker threads to use
    #[structopt(long)]
    workers: Option<usize>,

    /// Use Rayon thread pools.
    #[structopt(long)]
    rayon: bool,

    /// Number of working set stripes.
    #[structopt(long, default_value = "1")]
    working_set_stripes: usize,
}

fn main() -> io::Result<()> {
    env_logger::init();

    let opts: Opts = Opts::from_args();

    let start = Instant::now();
    info!("Starting...");

    fs::create_dir_all(&opts.data)?;

    let scheduler = if opts.rayon {
        Scheduler::new(
            ThreadPoolBuilder::new()
                .num_threads(opts.workers.unwrap_or(0))
                .build()
                .unwrap(),
        )
    } else {
        Scheduler::new(StaticPool::new(opts.workers.unwrap_or_else(|| {
            let n = num_cpus::get();
            info!("Using {} scheduler threads.", n);
            n
        })))
    };

    let timer = timer::Scheduler::new();
    timer.start();

    let deletion_scheduler = Scheduler::new(StaticPool::new(1));
    let id_generator = IdGenerator::new(opts.data.join("id"), scheduler.clone()).unwrap();
    let working_set_scheduler = Scheduler::new(StaticPool::new(1));
    let working_set_dir = opts.data.join("working");
    let working_set_stripes = (0..opts.working_set_stripes)
        .map(|_| (working_set_dir.clone(), working_set_scheduler.clone()))
        .collect::<Vec<_>>();
    let working_set = WorkingSet::new(working_set_stripes).unwrap();
    let qrono = Qrono::new(
        scheduler.clone(),
        timer,
        id_generator,
        working_set,
        opts.data.join("queues"),
        deletion_scheduler,
    );
    let listener = TcpListener::bind(&opts.listen)?;
    info!("Ready. Started in {:?}.", Instant::now() - start);
    for conn in listener.incoming() {
        let qrono = qrono.clone();
        let scheduler = scheduler.clone();
        let conn = conn?;
        let addr = conn.peer_addr()?;
        info!("Accepted connection from {}", &addr);
        thread::Builder::new()
            .name(format!("QronoClient[{}]", &addr))
            .spawn(move || handle_client(qrono, scheduler, conn))
            .unwrap();
    }

    Ok(())
}
