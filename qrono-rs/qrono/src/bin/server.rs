use std::io::{ErrorKind, Write};
use std::net::{Shutdown, TcpListener, TcpStream};

use std::path::PathBuf;
use std::sync::{mpsc, Arc};
use std::time::Instant;
use std::{fs, io, str, thread};

use bytes::{Buf, Bytes, BytesMut};
use log::{error, info, warn};
use qrono::data::Timestamp;
use qrono::id_generator::IdGenerator;
use qrono::io::ReadBufUninitialized;
use qrono::ops::{
    CompactReq, CompactResp, DeleteReq, DeleteResp, DequeueReq, DequeueResp, EnqueueReq,
    EnqueueResp, IdPattern, InfoReq, InfoResp, PeekReq, PeekResp, ReleaseReq, ReleaseResp,
    RequeueReq, RequeueResp,
};
use qrono::promise::Future;
use qrono::redis::Error::{Incomplete, ProtocolError};
use qrono::redis::{PutValue, Value};
use qrono::scheduler::{Scheduler, StaticPool};

use qrono::service::Result as QronoResult;
use qrono::service::{Error, Qrono};
use qrono::working_set::WorkingSet;
use rayon::ThreadPoolBuilder;
use structopt::StructOpt;

#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

// TODO:
//  - Configurable storage paths
//  - Error handling (audit unwrap calls)
//  - Compact working set

// Done
//  - Compact segments
//  - Reload queues on startup
//  - Deleting queues

// Goal of compaction: Remove tombstoned entries.
// Equivalently, reduce the on-disk data to only unreleased entries.
// Unreleased entries include pending and working items.
// All pending items are > LAST. All released items are < LAST.
// All working items are < LAST.

enum Command {
    Enqueue,
    Dequeue,
    Requeue,
    Release,
    Info,
    Peek,
    Ping,
    Delete,
    Compact,
}

impl Command {
    fn parse(value: Value) -> Result<(Command, Vec<Bytes>), qrono::redis::Error> {
        let args = match value {
            Value::Array(args) => args,
            _ => {
                return Err(qrono::redis::Error::ProtocolError(
                    "expected command array".to_string(),
                ))
            }
        };

        if args.is_empty() {
            return Err(qrono::redis::Error::ProtocolError(
                "empty command array".to_string(),
            ));
        }

        let nargs = args.len() - 1;
        let mut args = args.into_iter();
        let cmd = match args.next().unwrap() {
            Value::BulkString(cmd) => cmd,
            _ => {
                return Err(qrono::redis::Error::ProtocolError(
                    "command array should contain bulk strings".to_string(),
                ));
            }
        };

        if cmd.is_empty() {
            return Err(qrono::redis::Error::ProtocolError(
                "command name empty".to_string(),
            ));
        }

        let cmd = match cmd[0] {
            b'e' | b'E' if cmd.eq_ignore_ascii_case(b"ENQUEUE") => Command::Enqueue,
            b'd' | b'D' if cmd.eq_ignore_ascii_case(b"DEQUEUE") => Command::Dequeue,
            b'r' | b'R' if cmd.eq_ignore_ascii_case(b"REQUEUE") => Command::Requeue,
            b'r' | b'R' if cmd.eq_ignore_ascii_case(b"RELEASE") => Command::Release,
            b'i' | b'I' if cmd.eq_ignore_ascii_case(b"INFO") => Command::Info,
            b'p' | b'P' if cmd.eq_ignore_ascii_case(b"PEEK") => Command::Peek,
            b'p' | b'P' if cmd.eq_ignore_ascii_case(b"PING") => Command::Ping,
            b'd' | b'D' if cmd.eq_ignore_ascii_case(b"DELETE") => Command::Delete,
            b'c' | b'C' if cmd.eq_ignore_ascii_case(b"COMPACT") => Command::Compact,
            _ => {
                return Err(qrono::redis::Error::ProtocolError(
                    "ERR command not supported".to_string(),
                ))
            }
        };

        let mut byte_args = Vec::with_capacity(nargs);
        for arg in args {
            byte_args.push(match arg {
                Value::BulkString(cmd) => cmd,
                _ => {
                    return Err(qrono::redis::Error::ProtocolError(
                        "command array should contain bulk strings".to_string(),
                    ));
                }
            });
        }

        Ok((cmd, byte_args))
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

    fn take(self) -> Value {
        match self {
            Response::Enqueue(future) => Self::convert(future.take(), |v| {
                Value::Array(vec![
                    Value::Integer(v.id as i64),
                    Value::Integer(v.deadline.millis()),
                ])
            }),
            Response::Dequeue(future) | Response::Peek(future) => {
                let res = future.take();
                if let Err(Error::NoSuchQueue) = res {
                    return Value::NullArray;
                }

                Self::convert(res, |v| {
                    Value::Array(vec![
                        Value::Integer(v.id as i64),
                        Value::Integer(v.deadline.millis()),
                        Value::Integer(v.stats.enqueue_time.millis()),
                        Value::Integer(v.stats.requeue_time.millis()),
                        Value::Integer(v.stats.dequeue_count as i64),
                        Value::BulkString(v.value),
                    ])
                })
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
        let mut buf = BytesMut::new();

        let conn = conn.try_clone()?;
        let handle_io_result = move |res| {
            if let Err(err) = &res {
                warn!("Closing connection. Error writing data to client: {}", err);
                shutdown_connection(&conn);
            }

            res
        };

        let (handle, _) = scheduler.register_fn(move || {
            buf.clear();

            let mut flush = false;
            for _ in 0..128 {
                if let Some(future) = &resp_head {
                    if future.is_ready() {
                        let val = resp_head.take().unwrap().take();
                        buf.put_redis_value(val);
                        if handle_io_result(writer.write_all(&buf[..])).is_err() {
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

            if handle_io_result(writer.flush()).is_err() {
                return false;
            }

            true
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

                    let (cmd, args) = match Command::parse(value) {
                        Ok(v) => v,
                        Err(err) => match err {
                            Incomplete => panic!("BUG"),
                            ProtocolError(msg) => {
                                resp_tx.send(Response::Error(msg)).unwrap();
                                schedule.schedule();
                                continue;
                            }
                        },
                    };

                    match cmd {
                        Command::Enqueue => {
                            // ENQUEUE <queue> <value> [DEADLINE <deadline>]
                            let (queue, req) = match args.len() {
                                2 => {
                                    let mut args = args.into_iter();
                                    let queue = args.next().unwrap().to_vec(); // TODO: Avoid copy
                                    let queue = String::from_utf8(queue).unwrap();
                                    let value = args.next().unwrap();
                                    let req = EnqueueReq {
                                        value,
                                        deadline: None,
                                    };
                                    (queue, req)
                                }
                                4 => {
                                    let mut args = args.into_iter();
                                    let queue = args.next().unwrap().to_vec(); // TODO: Avoid copy
                                    let queue = String::from_utf8(queue).unwrap();
                                    let value = args.next().unwrap();
                                    let keyword = args.next().unwrap();
                                    if !keyword.eq_ignore_ascii_case(b"DEADLINE") {
                                        resp_tx
                                            .send(Response::Error(
                                                "ERR unexpected keyword, expected \"DEADLINE\""
                                                    .into(),
                                            ))
                                            .unwrap();
                                        schedule.schedule();
                                        continue;
                                    }
                                    let deadline = args.next().unwrap();
                                    let deadline = match qrono::redis::parse_signed(&deadline) {
                                        Ok(deadline) => deadline,
                                        Err(err) => {
                                            resp_tx.send(Response::Value(err.into())).unwrap();
                                            schedule.schedule();
                                            continue;
                                        }
                                    };
                                    let req = EnqueueReq {
                                        value,
                                        deadline: Some(Timestamp::from_millis(deadline)),
                                    };
                                    (queue, req)
                                }
                                _ => {
                                    resp_tx
                                        .send(Response::Error(
                                            "incorrect number of arguments to ENQUEUE".to_string(),
                                        ))
                                        .unwrap();
                                    schedule.schedule();
                                    continue;
                                }
                            };

                            let schedule = Arc::clone(&schedule);
                            let (resp, mut promise) = Future::new();
                            promise.on_complete(move || schedule.schedule());
                            resp_tx.send(Response::Enqueue(resp)).unwrap();
                            qrono.enqueue(&queue, req, promise);
                        }
                        Command::Dequeue => {
                            if args.len() != 1 {
                                resp_tx
                                    .send(Response::Error(
                                        "incorrect number of arguments".to_string(),
                                    ))
                                    .unwrap();
                                schedule.schedule();
                                continue;
                            }

                            let queue = str::from_utf8(&args[0]).unwrap();
                            let schedule = Arc::clone(&schedule);
                            let (resp, mut promise) = Future::new();
                            promise.on_complete(move || schedule.schedule());
                            resp_tx.send(Response::Dequeue(resp)).unwrap();
                            qrono.dequeue(queue, DequeueReq, promise);
                        }
                        Command::Requeue => {
                            // REQUEUE <queue> <id> [DEADLINE <deadline>]
                            let (queue, req) = match args.len() {
                                2 => {
                                    let mut args = args.into_iter();
                                    let queue = args.next().unwrap().to_vec(); // TODO: Avoid copy
                                    let queue = String::from_utf8(queue).unwrap();
                                    let id = args.next().unwrap();
                                    let id = match qrono::redis::parse_unsigned(&id) {
                                        Ok(id) => IdPattern::Id(id),
                                        Err(err) => {
                                            if id.eq_ignore_ascii_case(b"ANY") {
                                                IdPattern::Any
                                            } else {
                                                resp_tx.send(Response::Value(err.into())).unwrap();
                                                schedule.schedule();
                                                continue;
                                            }
                                        }
                                    };
                                    let req = RequeueReq { id, deadline: None };
                                    (queue, req)
                                }
                                4 => {
                                    let mut args = args.into_iter();
                                    let queue = args.next().unwrap().to_vec(); // TODO: Avoid copy
                                    let queue = String::from_utf8(queue).unwrap();
                                    let id = args.next().unwrap();
                                    let id = match qrono::redis::parse_unsigned(&id) {
                                        Ok(id) => IdPattern::Id(id),
                                        Err(err) => {
                                            if id.eq_ignore_ascii_case(b"ANY") {
                                                IdPattern::Any
                                            } else {
                                                resp_tx.send(Response::Value(err.into())).unwrap();
                                                schedule.schedule();
                                                continue;
                                            }
                                        }
                                    };
                                    let keyword = args.next().unwrap();
                                    if !keyword.eq_ignore_ascii_case(b"DEADLINE") {
                                        resp_tx
                                            .send(Response::Error(
                                                "ERR unexpected keyword, expected \"DEADLINE\""
                                                    .into(),
                                            ))
                                            .unwrap();
                                        schedule.schedule();
                                        continue;
                                    }
                                    let deadline = args.next().unwrap();
                                    let deadline = match qrono::redis::parse_signed(&deadline) {
                                        Ok(deadline) => deadline,
                                        Err(err) => {
                                            resp_tx.send(Response::Value(err.into())).unwrap();
                                            schedule.schedule();
                                            continue;
                                        }
                                    };
                                    let req = RequeueReq {
                                        id,
                                        deadline: Some(Timestamp::from_millis(deadline)),
                                    };
                                    (queue, req)
                                }
                                _ => {
                                    resp_tx
                                        .send(Response::Error(
                                            "incorrect number of arguments".to_string(),
                                        ))
                                        .unwrap();
                                    schedule.schedule();
                                    continue;
                                }
                            };

                            let schedule = Arc::clone(&schedule);
                            let (resp, mut promise) = Future::new();
                            promise.on_complete(move || schedule.schedule());
                            resp_tx.send(Response::Requeue(resp)).unwrap();
                            qrono.requeue(&queue, req, promise);
                        }
                        Command::Release => {
                            // RELEASE <queue> <id>
                            let (queue, req) = match args.len() {
                                2 => {
                                    let mut args = args.into_iter();
                                    let queue = args.next().unwrap().to_vec(); // TODO: Avoid copy
                                    let queue = String::from_utf8(queue).unwrap();
                                    let id = args.next().unwrap();
                                    let id = match qrono::redis::parse_unsigned(&id) {
                                        Ok(id) => IdPattern::Id(id),
                                        Err(err) => {
                                            if id.eq_ignore_ascii_case(b"ANY") {
                                                IdPattern::Any
                                            } else {
                                                resp_tx.send(Response::Value(err.into())).unwrap();
                                                schedule.schedule();
                                                continue;
                                            }
                                        }
                                    };
                                    let req = ReleaseReq { id };
                                    (queue, req)
                                }
                                _ => {
                                    resp_tx
                                        .send(Response::Error(
                                            "incorrect number of arguments".to_string(),
                                        ))
                                        .unwrap();
                                    schedule.schedule();
                                    continue;
                                }
                            };

                            let schedule = Arc::clone(&schedule);
                            let (resp, mut promise) = Future::new();
                            promise.on_complete(move || schedule.schedule());
                            resp_tx.send(Response::Release(resp)).unwrap();
                            qrono.release(&queue, req, promise);
                        }
                        Command::Peek => {
                            if args.len() != 1 {
                                resp_tx
                                    .send(Response::Error(
                                        "incorrect number of arguments to ENQUEUE".to_string(),
                                    ))
                                    .unwrap();
                                schedule.schedule();
                                continue;
                            }

                            let queue = str::from_utf8(&args[0]).unwrap();
                            let schedule = Arc::clone(&schedule);
                            let (resp, mut promise) = Future::new();
                            promise.on_complete(move || schedule.schedule());
                            resp_tx.send(Response::Peek(resp)).unwrap();
                            qrono.peek(&queue, PeekReq, promise);
                        }
                        Command::Ping => {
                            resp_tx
                                .send(Response::Value(Value::SimpleString("PONG".to_string())))
                                .unwrap();
                            schedule.schedule();
                        }
                        Command::Info => {
                            if args.len() != 1 {
                                resp_tx
                                    .send(Response::Error(
                                        "incorrect number of arguments".to_string(),
                                    ))
                                    .unwrap();
                                schedule.schedule();
                                continue;
                            }

                            let queue = str::from_utf8(&args[0]).unwrap();
                            let schedule = Arc::clone(&schedule);
                            let (resp, mut promise) = Future::new();
                            promise.on_complete(move || schedule.schedule());
                            resp_tx.send(Response::Info(resp)).unwrap();
                            qrono.info(&queue, InfoReq, promise);
                        }
                        Command::Delete => {
                            if args.len() != 1 {
                                resp_tx
                                    .send(Response::Error(
                                        "incorrect number of arguments".to_string(),
                                    ))
                                    .unwrap();
                                schedule.schedule();
                                continue;
                            }

                            let queue = str::from_utf8(&args[0]).unwrap();
                            let schedule = Arc::clone(&schedule);
                            let (resp, mut promise) = Future::new();
                            promise.on_complete(move || schedule.schedule());
                            resp_tx.send(Response::Delete(resp)).unwrap();
                            qrono.delete(&queue, DeleteReq, promise);
                        }
                        Command::Compact => {
                            if args.len() != 1 {
                                resp_tx
                                    .send(Response::Error(
                                        "incorrect number of arguments".to_string(),
                                    ))
                                    .unwrap();
                                schedule.schedule();
                                continue;
                            }

                            let queue = str::from_utf8(&args[0]).unwrap();
                            let schedule = Arc::clone(&schedule);
                            let (resp, mut promise) = Future::new();
                            promise.on_complete(move || schedule.schedule());
                            resp_tx.send(Response::Compact(resp)).unwrap();
                            qrono.compact(&queue, CompactReq, promise);
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
