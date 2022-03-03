use crate::bytes::Bytes;
use crate::data::{Item, Timestamp, ID};
use std::time::Duration;

trait Request {
    type Response;
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum DeadlineReq {
    Now,
    Relative(Duration),
    Absolute(Timestamp),
}

impl DeadlineReq {
    pub fn resolve(self, now: Timestamp) -> Timestamp {
        match self {
            DeadlineReq::Now => now,
            DeadlineReq::Relative(delay) => now + delay,
            DeadlineReq::Absolute(timestamp) => timestamp,
        }
    }
}

#[derive(Debug)]
pub struct EnqueueReq {
    pub value: Bytes,
    pub deadline: DeadlineReq,
}

#[derive(Debug)]
pub struct EnqueueResp {
    pub id: ID,
    pub deadline: Timestamp,
}

#[derive(Debug)]
pub struct DequeueReq {
    pub timeout: Duration,
    pub count: u64,
}

pub type DequeueResp = Vec<Item>;

#[derive(Debug)]
pub struct RequeueReq {
    pub id: IdPattern,
    pub deadline: DeadlineReq,
}

#[derive(Debug)]
pub struct RequeueResp {
    pub deadline: Timestamp,
}

#[derive(Debug)]
pub struct ReleaseReq {
    pub id: IdPattern,
}

#[derive(Debug)]
pub enum IdPattern {
    Any,
    Id(ID),
}

pub type ReleaseResp = ();

#[derive(Debug)]
pub struct PeekReq;

pub type PeekResp = Item;

#[derive(Debug)]
pub struct InfoReq;

#[derive(Debug, Clone)]
pub struct InfoResp {
    pub pending: u64,
    pub dequeued: u64,
}

#[derive(Debug)]
pub struct DeleteReq;

pub type DeleteResp = ();

#[derive(Debug)]
pub struct CompactReq;

pub type CompactResp = ();
