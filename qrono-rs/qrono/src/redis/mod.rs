pub mod protocol;
pub mod request;
pub mod server;

// int_log10 is vendored from the standard library and contains methods we do not use.
#[allow(dead_code)]
#[allow(clippy::unusual_byte_groupings)]
mod int_log10;
