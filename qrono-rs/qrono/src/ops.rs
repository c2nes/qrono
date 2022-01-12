use crate::data::{Item, Timestamp, ID};
use bytes::Bytes;

#[derive(Debug)]
pub struct EnqueueReq {
    pub value: Bytes,
    pub deadline: Option<Timestamp>,
}

#[derive(Debug)]
pub struct EnqueueResp {
    pub id: ID,
    pub deadline: Timestamp,
}

#[derive(Debug)]
pub struct DequeueReq;

pub type DequeueResp = Item;

#[derive(Debug)]
pub struct RequeueReq {
    pub id: IdPattern,
    pub deadline: Option<Timestamp>,
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
