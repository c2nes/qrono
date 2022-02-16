/// ResultIgnoreErr provides the `ignore_err` method which can be used to explicitly
/// ignore errors from a `Result<T, E>`.
pub trait IgnoreErr: Sized {
    /// Ignore the result and any errors therein.
    fn ignore_err(self) {}
}

impl<T, E> IgnoreErr for Result<T, E> {}
