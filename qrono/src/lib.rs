extern crate core;

use qrono_channel as channel;

pub mod alloc;
pub mod bytes;
pub mod data;
mod encoding;
pub mod error;
pub mod grpc;
pub mod hash;
pub mod http;
pub mod id_generator;
mod io;
pub mod ops;
mod path;
pub mod promise;
mod queue;
pub mod redis;
pub mod result;
pub mod scheduler;
pub mod segment;
pub mod service;
pub mod timer;
pub mod wait_group;
pub mod wal;
pub mod working_set;

#[cfg(test)]
#[global_allocator]
static ALLOC: alloc::QronoAllocator = alloc::QronoAllocator::new();
