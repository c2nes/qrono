use crate::bytes::Bytes;
use crate::data::{Item, Timestamp};
use crate::error::QronoError;
use crate::ops::{
    CompactReq, CompactResp, DeadlineReq, DeleteReq, DeleteResp, DequeueReq, DequeueResp,
    EnqueueReq, EnqueueResp, IdPattern, InfoReq, InfoResp, PeekReq, PeekResp, ReleaseReq,
    ReleaseResp, RequeueReq, RequeueResp, ValueReq,
};
use crate::redis::protocol::Value;
use crate::result::QronoResult;
use qrono_promise::Future;
use std::string::FromUtf8Error;
use std::time::Duration;
use std::vec::IntoIter;

struct RawRequest(IntoIter<Value>);

impl RawRequest {
    fn len(&self) -> usize {
        self.0.len()
    }

    fn next(&mut self) -> Result<Vec<u8>, Response> {
        match self.0.next().unwrap() {
            Value::BulkString(value) => Ok(value),
            _ => Err(Response::Error(
                "ERR Protocol error: bulk string expected".to_string(),
            )),
        }
    }
}

impl TryFrom<Value> for RawRequest {
    type Error = Response;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Array(args) => Ok(RawRequest(args.into_iter())),
            _ => Err(Response::Error(
                "ERR Protocol error: array expected".to_string(),
            )),
        }
    }
}

pub enum Request {
    Enqueue(String, EnqueueReq),
    Dequeue(String, DequeueReq),
    Requeue(String, RequeueReq),
    Release(String, ReleaseReq),
    Info(String, InfoReq),
    Peek(String, PeekReq),
    Delete(String, DeleteReq),
    Compact(String, CompactReq),
    Ping(Option<Vec<u8>>),
}

impl Request {
    pub fn parse(value: Value) -> Result<Request, Response> {
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

    fn parse_queue_name(name: Vec<u8>) -> Result<String, Response> {
        String::from_utf8(name)
            .map_err(|_| Response::Error("ERR queue name must be valid UTF8".to_string()))
    }

    fn parse_deadline_req(args: &mut RawRequest) -> Result<DeadlineReq, Response> {
        if args.len() < 2 {
            return Ok(DeadlineReq::Now);
        }

        let keyword = args.next()?;
        let value = args.next()?;

        let value = match crate::redis::protocol::parse_signed(&value) {
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

    fn parse_id_pattern(id: Vec<u8>) -> Result<IdPattern, Response> {
        match crate::redis::protocol::parse_unsigned(&id) {
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

        let queue = Self::parse_queue_name(args.next()?)?;
        let value = ValueReq::Bytes(Bytes::from(args.next()?));
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
                match crate::redis::protocol::parse_unsigned(&arg) {
                    Ok(millis) => req.timeout = Duration::from_millis(millis),
                    Err(_) => {
                        return Err(Response::Error(format!("ERR invalid timeout, {:?}", arg)))
                    }
                }
            } else if keyword.eq_ignore_ascii_case(b"COUNT") {
                match crate::redis::protocol::parse_unsigned(&arg) {
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

        let queue = Self::parse_queue_name(args.next()?)?;
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

        let queue = Self::parse_queue_name(args.next()?)?;
        let id = Self::parse_id_pattern(args.next()?)?;

        Ok(Request::Release(queue, ReleaseReq { id }))
    }

    // PEEK <queue>
    fn parse_peek_req(mut args: RawRequest) -> Result<Request, Response> {
        match args.len() {
            1 => {}
            n => return Err(Self::err_invalid_argument_count(n)),
        };

        let queue = Self::parse_queue_name(args.next()?)?;

        Ok(Request::Peek(queue, PeekReq))
    }

    // INFO <queue>
    fn parse_info_req(mut args: RawRequest) -> Result<Request, Response> {
        match args.len() {
            1 => {}
            n => return Err(Self::err_invalid_argument_count(n)),
        };

        let queue = Self::parse_queue_name(args.next()?)?;

        Ok(Request::Info(queue, InfoReq))
    }

    // DELETE <queue>
    fn parse_delete_req(mut args: RawRequest) -> Result<Request, Response> {
        match args.len() {
            1 => {}
            n => return Err(Self::err_invalid_argument_count(n)),
        };

        let queue = Self::parse_queue_name(args.next()?)?;

        Ok(Request::Delete(queue, DeleteReq))
    }

    // COMPACT <queue>
    fn parse_compact_req(mut args: RawRequest) -> Result<Request, Response> {
        match args.len() {
            1 => {}
            n => return Err(Self::err_invalid_argument_count(n)),
        };

        let queue = Self::parse_queue_name(args.next()?)?;

        Ok(Request::Compact(queue, CompactReq))
    }

    // PING [<msg>]
    fn parse_ping_req(mut args: RawRequest) -> Result<Request, Response> {
        match args.len() {
            0 => Ok(Request::Ping(None)),
            1 => Ok(Request::Ping(Some(args.next()?))),
            n => Err(Self::err_invalid_argument_count(n)),
        }
    }
}

pub enum Response {
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
                QronoError::NoSuchQueue => Value::Error("ERR no such queue".into()),
                QronoError::NoItemReady => Value::NullArray,
                QronoError::ItemNotDequeued => Value::Error("ERR item not dequeued".into()),
                QronoError::Internal => Value::Error("ERR internal error, see logs".into()),
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
            Value::BulkStringBytes(item.value),
        ])
    }

    pub fn take(self) -> Value {
        match self {
            Response::Enqueue(future) => Self::convert(future.take(), |v| {
                Value::Array(vec![
                    Value::Integer(v.id as i64),
                    Value::Integer(v.deadline.millis()),
                ])
            }),
            Response::Dequeue(future) => {
                let res = future.take();
                if let Err(QronoError::NoSuchQueue) = res {
                    return Value::NullArray;
                }

                Self::convert(res, |items| {
                    Value::Array(items.into_iter().map(Self::encode_item).collect::<Vec<_>>())
                })
            }
            Response::Peek(future) => {
                let res = future.take();
                if let Err(QronoError::NoSuchQueue) = res {
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

    pub fn is_ready(&self) -> bool {
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

impl From<FromUtf8Error> for Response {
    fn from(_: FromUtf8Error) -> Self {
        Self::Error("ERR Protocol error: invalid UTF8".to_string())
    }
}
