pub use qrono_promise::{Future, Promise, TransferableFuture};

pub type QronoFuture<T> = qrono_promise::Future<crate::result::QronoResult<T>>;
pub type QronoPromise<T> = qrono_promise::Promise<crate::result::QronoResult<T>>;
