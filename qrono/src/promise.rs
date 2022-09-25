use crate::result::QronoResult;

pub use qrono_promise::{Future, Promise, TransferableFuture};

pub type QronoFuture<T> = Future<QronoResult<T>>;
pub type QronoPromise<T> = Promise<QronoResult<T>>;
