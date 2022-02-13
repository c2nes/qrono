use std::io::{ErrorKind, Write};
use std::net::{Shutdown, TcpListener, TcpStream};

use std::path::PathBuf;

use std::slice::Iter;
use std::str::Utf8Error;
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};
use std::{fs, io, str, thread};

use bytes::{Buf, Bytes, BytesMut};
use log::{error, info, warn};
use qrono::data::{Item, Timestamp};
use qrono::id_generator::IdGenerator;
use qrono::io::ReadBufUninitialized;
use qrono::ops::{
    CompactReq, CompactResp, DeadlineReq, DeleteReq, DeleteResp, DequeueReq, DequeueResp,
    EnqueueReq, EnqueueResp, IdPattern, InfoReq, InfoResp, PeekReq, PeekResp, ReleaseReq,
    ReleaseResp, RequeueReq, RequeueResp,
};
use qrono::promise::Future;
use qrono::redis::Error::{Incomplete, ProtocolError};
use qrono::redis::{RedisBuf, Value};
use qrono::scheduler::{Scheduler, StaticPool};

use qrono::service::Result as QronoResult;
use qrono::service::{Error, Qrono};
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

struct RawRequest<'a>(Iter<'a, Value>);

impl<'a> RawRequest<'a> {
    fn len(&self) -> usize {
        self.0.len()
    }

    fn next(&mut self) -> Result<&'a Bytes, Response> {
        match &self.0.next().unwrap() {
            Value::BulkString(value) => Ok(value),
            _ => Err(Response::Error(
                "ERR Protocol error: bulk string expected".to_string(),
            )),
        }
    }
}

impl<'a> TryFrom<&'a Value> for RawRequest<'a> {
    type Error = Response;

    fn try_from(value: &'a Value) -> Result<Self, Self::Error> {
        match value {
            Value::Array(args) => Ok(RawRequest(args.iter())),
            _ => Err(Response::Error(
                "ERR Protocol error: array expected".to_string(),
            )),
        }
    }
}

enum Request<'a> {
    Enqueue(&'a str, EnqueueReq),
    Dequeue(&'a str, DequeueReq),
    Requeue(&'a str, RequeueReq),
    Release(&'a str, ReleaseReq),
    Info(&'a str, InfoReq),
    Peek(&'a str, PeekReq),
    Delete(&'a str, DeleteReq),
    Compact(&'a str, CompactReq),
    Ping(Option<Bytes>),
}

impl<'a> Request<'a> {
    fn parse(value: &Value) -> Result<Request, Response> {
        let mut args = RawRequest::try_from(value)?;

        if args.len() == 0 {
            return Err(Response::Error(
                "ERR Protocol error: empty command array".to_string(),
            ));
        }

        let cmd = args.next()?;

        if cmd.is_empty() {
            return Err(Response::Error(
                "ERR Protocol error: command name empty".to_string(),
            ));
        }

        match cmd[0] {
            b'e' | b'E' if cmd.eq_ignore_ascii_case(b"ENQUEUE") => Self::parse_enqueue_req(args),
            b'd' | b'D' if cmd.eq_ignore_ascii_case(b"DEQUEUE") => Self::parse_dequeue_req(args),
            b'r' | b'R' if cmd.eq_ignore_ascii_case(b"REQUEUE") => Self::parse_requeue_req(args),
            b'r' | b'R' if cmd.eq_ignore_ascii_case(b"RELEASE") => Self::parse_release_req(args),
            b'i' | b'I' if cmd.eq_ignore_ascii_case(b"INFO") => Self::parse_info_req(args),
            b'p' | b'P' if cmd.eq_ignore_ascii_case(b"PEEK") => Self::parse_peek_req(args),
            b'p' | b'P' if cmd.eq_ignore_ascii_case(b"PING") => Self::parse_ping_req(args),
            b'd' | b'D' if cmd.eq_ignore_ascii_case(b"DELETE") => Self::parse_delete_req(args),
            b'c' | b'C' if cmd.eq_ignore_ascii_case(b"COMPACT") => Self::parse_compact_req(args),
            _ => Err(Response::Error("ERR command not supported".to_string())),
        }
    }

    fn parse_queue_name(name: &[u8]) -> Result<&str, Response> {
        str::from_utf8(name)
            .map_err(|_| Response::Error("ERR queue name must be valid UTF8".to_string()))
    }

    fn parse_deadline_req(args: &mut RawRequest) -> Result<DeadlineReq, Response> {
        if args.len() < 2 {
            return Ok(DeadlineReq::Now);
        }

        let keyword = args.next()?;
        let value = args.next()?;

        let value = match qrono::redis::parse_signed(value) {
            Ok(value) => value,
            Err(_) => {
                let msg = format!("ERR invalid deadline value, {:?}", value);
                return Err(Response::Error(msg));
            }
        };

        if keyword.eq_ignore_ascii_case(b"DEADLINE") {
            Ok(DeadlineReq::Absolute(Timestamp::from_millis(value)))
        } else if keyword.eq_ignore_ascii_case(b"DELAY") {
            if value >= 0 {
                Ok(DeadlineReq::Relative(Duration::from_millis(value as u64)))
            } else {
                Err(Response::Error(
                    "ERR delay must be non-negative".to_string(),
                ))
            }
        } else {
            Err(Response::Error(format!(
                "ERR unexpected keyword {:?}, expected \"DEADLINE\" or \"DELAY\"",
                keyword
            )))
        }
    }

    fn parse_id_pattern(id: &Bytes) -> Result<IdPattern, Response> {
        match qrono::redis::parse_unsigned(id) {
            Ok(id) => Ok(IdPattern::Id(id)),
            Err(_) => {
                if id.eq_ignore_ascii_case(b"ANY") {
                    Ok(IdPattern::Any)
                } else {
                    Err(Response::Error("ERR expected ID or \"ANY\"".to_string()))
                }
            }
        }
    }

    fn err_invalid_argument_count(n: usize) -> Response {
        Response::Error(format!("ERR invalid number of arguments, {}", n))
    }

    // ENQUEUE <queue> <value> [DEADLINE <deadline>]
    fn parse_enqueue_req(mut args: RawRequest) -> Result<Request, Response> {
        match args.len() {
            2 | 4 => {}
            n => return Err(Self::err_invalid_argument_count(n)),
        };

        let queue = str::from_utf8(args.next()?)?;
        let value = args.next()?.clone();
        let deadline = Self::parse_deadline_req(&mut args)?;

        Ok(Request::Enqueue(queue, EnqueueReq { value, deadline }))
    }

    // DEQUEUE <queue> [TIMEOUT <milliseconds>] [COUNT <count>]
    fn parse_dequeue_req(mut args: RawRequest) -> Result<Request, Response> {
        match args.len() {
            1 | 3 | 5 => {} // Ok
            n => {
                return Err(Self::err_invalid_argument_count(n));
            }
        }

        let queue = Self::parse_queue_name(args.next()?)?;
        let mut req = DequeueReq {
            timeout: Default::default(),
            count: 1,
        };

        while args.len() > 0 {
            let keyword = args.next()?;
            let arg = args.next()?;

            if keyword.eq_ignore_ascii_case(b"TIMEOUT") {
                match qrono::redis::parse_unsigned(arg) {
                    Ok(millis) => req.timeout = Duration::from_millis(millis),
                    Err(_) => {
                        return Err(Response::Error(format!("ERR invalid timeout, {:?}", arg)))
                    }
                }
            } else if keyword.eq_ignore_ascii_case(b"COUNT") {
                match qrono::redis::parse_unsigned(arg) {
                    Ok(count) if count > 0 => req.count = count,
                    _ => return Err(Response::Error(format!("ERR invalid count, {:?}", arg))),
                }
            } else {
                return Err(Response::Error(format!(
                    "ERR invalid argument, {:?}",
                    keyword
                )));
            }
        }

        Ok(Request::Dequeue(queue, req))
    }

    // REQUEUE <queue> <id> [DEADLINE <deadline>]
    fn parse_requeue_req(mut args: RawRequest) -> Result<Request, Response> {
        match args.len() {
            2 | 4 => {}
            n => return Err(Self::err_invalid_argument_count(n)),
        };

        let queue = str::from_utf8(args.next()?)?;
        let id = Self::parse_id_pattern(args.next()?)?;
        let deadline = Self::parse_deadline_req(&mut args)?;

        Ok(Request::Requeue(queue, RequeueReq { id, deadline }))
    }

    // RELEASE <queue> <id>
    fn parse_release_req(mut args: RawRequest) -> Result<Request, Response> {
        match args.len() {
            2 => {}
            n => return Err(Self::err_invalid_argument_count(n)),
        };

        let queue = str::from_utf8(args.next()?)?;
        let id = Self::parse_id_pattern(args.next()?)?;

        Ok(Request::Release(queue, ReleaseReq { id }))
    }

    // PEEK <queue>
    fn parse_peek_req(mut args: RawRequest) -> Result<Request, Response> {
        match args.len() {
            1 => {}
            n => return Err(Self::err_invalid_argument_count(n)),
        };

        let queue = str::from_utf8(args.next()?)?;

        Ok(Request::Peek(queue, PeekReq))
    }

    // INFO <queue>
    fn parse_info_req(mut args: RawRequest) -> Result<Request, Response> {
        match args.len() {
            1 => {}
            n => return Err(Self::err_invalid_argument_count(n)),
        };

        let queue = str::from_utf8(args.next()?)?;

        Ok(Request::Info(queue, InfoReq))
    }

    // DELETE <queue>
    fn parse_delete_req(mut args: RawRequest) -> Result<Request, Response> {
        match args.len() {
            1 => {}
            n => return Err(Self::err_invalid_argument_count(n)),
        };

        let queue = str::from_utf8(args.next()?)?;

        Ok(Request::Delete(queue, DeleteReq))
    }

    // COMPACT <queue>
    fn parse_compact_req(mut args: RawRequest) -> Result<Request, Response> {
        match args.len() {
            1 => {}
            n => return Err(Self::err_invalid_argument_count(n)),
        };

        let queue = str::from_utf8(args.next()?)?;

        Ok(Request::Compact(queue, CompactReq))
    }

    // PING [<msg>]
    fn parse_ping_req(mut args: RawRequest) -> Result<Request, Response> {
        match args.len() {
            0 => Ok(Request::Ping(None)),
            1 => Ok(Request::Ping(Some(args.next()?.clone()))),
            n => Err(Self::err_invalid_argument_count(n)),
        }
    }
}

enum Response {
    Enqueue(Future<QronoResult<EnqueueResp>>),
    Dequeue(Future<QronoResult<DequeueResp>>),
    Requeue(Future<QronoResult<RequeueResp>>),
    Release(Future<QronoResult<ReleaseResp>>),
    Info(Future<QronoResult<InfoResp>>),
    Peek(Future<QronoResult<PeekResp>>),
    Delete(Future<QronoResult<DeleteResp>>),
    Compact(Future<QronoResult<CompactResp>>),
    Error(String),
    Value(Value),
}

impl Response {
    fn convert<T, F>(result: QronoResult<T>, converter: F) -> Value
    where
        F: Fn(T) -> Value,
    {
        match result {
            Ok(resp) => converter(resp),
            Err(err) => match err {
                Error::NoSuchQueue => Value::Error("ERR no such queue".into()),
                Error::NoItemReady => Value::NullArray,
                Error::ItemNotDequeued => Value::Error("ERR item not dequeued".into()),
                Error::Internal(_) | Error::IOError(_, _) => Value::Error("ERR internal".into()),
            },
        }
    }

    fn encode_item(item: Item) -> Value {
        Value::Array(vec![
            Value::Integer(item.id as i64),
            Value::Integer(item.deadline.millis()),
            Value::Integer(item.stats.enqueue_time.millis()),
            Value::Integer(item.stats.requeue_time.millis()),
            Value::Integer(item.stats.dequeue_count as i64),
            Value::BulkString(item.value),
        ])
    }

    fn take(self) -> Value {
        match self {
            Response::Enqueue(future) => Self::convert(future.take(), |v| {
                Value::Array(vec![
                    Value::Integer(v.id as i64),
                    Value::Integer(v.deadline.millis()),
                ])
            }),
            Response::Dequeue(future) => {
                let res = future.take();
                if let Err(Error::NoSuchQueue) = res {
                    return Value::NullArray;
                }

                Self::convert(res, |items| {
                    Value::Array(items.into_iter().map(Self::encode_item).collect::<Vec<_>>())
                })
            }
            Response::Peek(future) => {
                let res = future.take();
                if let Err(Error::NoSuchQueue) = res {
                    return Value::NullArray;
                }

                Self::convert(res, Self::encode_item)
            }
            Response::Requeue(future) => {
                Self::convert(future.take(), |v| Value::Integer(v.deadline.millis()))
            }
            Response::Release(future) | Response::Delete(future) | Response::Compact(future) => {
                Self::convert(future.take(), |_| Value::SimpleString("OK".into()))
            }
            Response::Info(future) => Self::convert(future.take(), |v| {
                Value::Array(vec![
                    Value::Integer(v.pending as i64),
                    Value::Integer(v.dequeued as i64),
                ])
            }),
            Response::Error(msg) => Value::Error(msg),
            Response::Value(val) => val,
        }
    }

    fn is_ready(&self) -> bool {
        match self {
            Response::Enqueue(future) => future.is_complete(),
            Response::Dequeue(future) => future.is_complete(),
            Response::Requeue(future) => future.is_complete(),
            Response::Release(future) => future.is_complete(),
            Response::Info(future) => future.is_complete(),
            Response::Peek(future) => future.is_complete(),
            Response::Delete(future) => future.is_complete(),
            Response::Compact(future) => future.is_complete(),
            Response::Value(_) => true,
            Response::Error(_) => true,
        }
    }
}

impl From<Utf8Error> for Response {
    fn from(_: Utf8Error) -> Self {
        Self::Error("ERR Protocol error: invalid UTF8".to_string())
    }
}

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
