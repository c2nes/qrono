use crossbeam::utils::Backoff;
use std::cell::UnsafeCell;
use std::panic::UnwindSafe;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{AcqRel, Acquire};
use std::thread::Thread;
use std::{ptr, thread};

pub struct Future<T>(*mut Inner<T>);

#[must_use = "dropping an incomplete promise will panic"]
pub struct Promise<T>(*mut Inner<T>);

struct State;
impl State {
    const PENDING: usize = 0;
    const WAITING: usize = 1;
    const DROPPED: usize = 2;
    const DROPPING: usize = 3;
}

struct Inner<T> {
    state: AtomicUsize,
    data: UnsafeCell<Data<T>>,
    value: UnsafeCell<Option<T>>,
}

unsafe impl<T: Send> Send for Future<T> {}
unsafe impl<T: Send> Sync for Future<T> {}
impl<T> UnwindSafe for Future<T> {}

unsafe impl<T: Send> Send for Promise<T> {}
unsafe impl<T: Send> Sync for Promise<T> {}
impl<T> UnwindSafe for Promise<T> {}

enum Data<T> {
    None,
    Transferring(Box<dyn FnOnce(T) + Send>),
    Waiting(Thread),
}

impl<T> Inner<T> {
    unsafe fn wait(&self) {
        self.store_data(Data::Waiting(thread::current()));

        // We do not use a swap here because it is possible for us to race with
        // the Promise completing and accidentally overwrite a DROPPED state.
        // We could handle this edge case in drop_promise instead, but this
        // approach is more explicit and allows drop_promise to reject WAITING
        // as an invalid state when dropping.
        if self
            .state
            .compare_exchange(State::PENDING, State::WAITING, AcqRel, Acquire)
            .is_ok()
        {
            let mut state = State::WAITING;
            let backoff = Backoff::new();
            while state == State::WAITING {
                if backoff.is_completed() {
                    thread::park();
                } else {
                    backoff.snooze();
                }
                state = self.state.load(Acquire);
            }
        }
    }

    #[inline(always)]
    unsafe fn store_value(&self, v: T) {
        drop(ptr::replace(self.value.get(), Some(v)));
    }

    #[inline(always)]
    unsafe fn take_value(&self) -> T {
        ptr::replace(self.value.get(), None).expect("promise dropped without completing")
    }

    #[inline(always)]
    unsafe fn store_data(&self, data: Data<T>) {
        drop(ptr::replace(self.data.get(), data));
    }

    #[inline(always)]
    unsafe fn take_data(&self) -> Data<T> {
        ptr::replace(self.data.get(), Data::None)
    }

    unsafe fn is_complete(&self) -> bool {
        self.state.load(Acquire) != State::PENDING
    }

    unsafe fn take(&self) -> T {
        // Wait for Promise to drop and take value or panic if the Promise is
        // dropped without completing.
        if self.state.load(Acquire) == State::PENDING {
            self.wait();
        }

        self.take_value()
    }

    unsafe fn try_take(&self) -> Option<T> {
        if self.state.load(Acquire) == State::PENDING {
            None
        } else {
            Some(self.take_value())
        }
    }

    unsafe fn transfer<F: FnOnce(T) + Send + 'static>(&self, f: F) {
        // If Promise is already dropped then call the function immediately.
        // Otherwise, store the transfer function and let the Future drop.
        // If the Promise is dropped while we are storing the transfer function
        // then future_drop will handle calling it.
        if self.state.load(Acquire) != State::PENDING {
            return f(self.take_value());
        }

        self.store_data(Data::Transferring(Box::new(f)));
    }

    unsafe fn complete(&self, v: T) {
        match self.state.load(Acquire) {
            State::DROPPED | State::DROPPING => {
                // The Future has been dropped. Either Future::transfer was called or
                // the Future was discarded without being used.
                match self.take_data() {
                    Data::Transferring(f) => f(v),
                    Data::None => {
                        // The Future was discarded without being used
                        // so we can simply drop v.
                    }
                    _ => panic!("transfer function expected"),
                }
            }
            State::PENDING | State::WAITING => {
                // A WAITING thread will be signaled in drop_promise.
                self.store_value(v);
            }
            state => panic!("invalid state, {}", state),
        }
    }

    unsafe fn drop_promise(&self) {
        // Promise should be dropped when Promise::complete is called. If the
        // Promise is dropped without Promise::complete being called then we
        // will panic (see the assertions below).
        //
        // In the usual case where Promise::complete is called there are a few
        // conditions for us to handle here. There may be a waiter for us to
        // signal, the Future may have been dropped (for one of two reasons
        // discussed below), or the Future may be PENDING in which case our
        // work is done.
        //
        // In the case the Future is dropped then either Future::transfer was
        // called or the Future was discarded without being used. In the first
        // case we need handle a race with Future::transfer by calling the
        // transfer function ourselves if Promise::complete did not do so.
        match self.state.swap(State::DROPPING, AcqRel) {
            State::WAITING => {
                // Future::take may wake up at any point and complete. When it does the Future
                // will drop and call self.drop_shared.
                match self.take_data() {
                    Data::Waiting(t) => t.unpark(),
                    _ => panic!("waiter expected"),
                };

                if self.state.swap(State::DROPPED, AcqRel) == State::DROPPED {
                    self.drop_shared()
                }
            }
            state @ State::DROPPED | state @ State::DROPPING => {
                // Check that we didn't race with Future::transfer.
                match self.take_data() {
                    Data::Transferring(f) => {
                        // We raced with Future::transfer. Retrieve the value and call the fn.
                        f(self.take_value());
                    }
                    Data::None => {}
                    _ => panic!("transfer function expected"),
                };

                // TODO: We need to ensure drop_shared is called even if we panic
                if state == State::DROPPED
                    || self.state.swap(State::DROPPED, AcqRel) == State::DROPPED
                {
                    self.drop_shared();
                }
            }
            _ => {
                if self.state.swap(State::DROPPED, AcqRel) == State::DROPPED {
                    self.drop_shared()
                }
            }
        }
    }

    unsafe fn drop_future(&self) {
        // When a Future is dropped then either Future::take was called,
        // Future::transfer was called, or neither of these was called and the
        // Future was discarded without being used.
        //
        // For Future::take to return the Promise must first be dropped, so we
        // know that in this case the state will be DROPPED.
        //
        // Otherwise, the state may be PENDING or DROPPED (but never WAITING!)
        // in either of the two other cases.
        //
        // If the state is PENDING then the Promise is incomplete and we don't
        // need to do anything further. If this Future was dropped as a result
        // of calling Future::transfer then the Promise will handle invoking
        // the transfer function. Otherwise, the Future was discarded without
        // being used and there is nothing for us to do.
        //
        // If the state is DROPPED then the Promise was completed, and any of
        // the three situations described above could have precipitated this
        // Future being dropped. If Future::take was called then we will find
        // `self.data` empty. This is also the case if the Future is discarded
        // without being used. If Future::transfer was called and the state is
        // DROPPED then one of two things could have occurred:
        //
        // - Future::transfer observed the completed Promise, retrieved the
        //   value and invoked the transfer function itself. In this case
        //   self.data will be empty and there is nothing we need to do.
        // - Future::transfer and Promise::complete raced. Future::transfer
        //   observed the Promise to be incomplete and stored the transfer
        //   function in self.data. Promise::complete observed the Future to
        //   be PENDING and stored the value in self.value. In this case the
        //   transfer function will still be present in self.data. We handle
        //   this case by retrieving the transfer function and value ourselves
        //   and invoking the former with the latter.
        match self.state.swap(State::DROPPING, AcqRel) {
            state @ State::DROPPED | state @ State::DROPPING => {
                if let Data::Transferring(_) = &*self.data.get() {
                    match self.take_data() {
                        Data::Transferring(f) => {
                            f(self.take_value());
                        }
                        _ => panic!("invalid state"),
                    }
                }

                if state == State::DROPPED
                    || self.state.swap(State::DROPPED, AcqRel) == State::DROPPED
                {
                    self.drop_shared();
                }
            }
            State::PENDING => {
                // The promise has not been completed and either Future::transfer
                // was called or the Future was discarded.
                if self.state.swap(State::DROPPED, AcqRel) == State::DROPPED {
                    self.drop_shared();
                }
            }
            state => panic!("invalid state, {}", state),
        }
    }

    unsafe fn drop_shared(&self) {
        drop(Box::from_raw(self as *const Self as *mut Self));
    }
}

impl<T> Promise<T> {
    pub fn complete(self, v: T) {
        unsafe { (&*self.0).complete(v) };
    }
}

impl<T> Drop for Promise<T> {
    fn drop(&mut self) {
        unsafe { (&*self.0).drop_promise() };
    }
}

impl<T> Future<T> {
    pub fn new() -> (Future<T>, Promise<T>) {
        let ptr = Box::into_raw(
            Inner {
                state: AtomicUsize::new(State::PENDING),
                data: UnsafeCell::new(Data::None),
                value: UnsafeCell::new(None),
            }
            .into(),
        );

        (Future(ptr), Promise(ptr))
    }

    pub fn completed(val: T) -> Future<T> {
        let (future, promise) = Self::new();
        promise.complete(val);
        future
    }

    pub fn is_complete(&self) -> bool {
        unsafe { (&*self.0).is_complete() }
    }

    pub fn take(self) -> T {
        unsafe { (&*self.0).take() }
    }

    pub fn try_take(self) -> Result<T, Self> {
        match unsafe { (&*self.0).try_take() } {
            Some(v) => Ok(v),
            None => Err(self),
        }
    }

    pub fn transfer<F: FnOnce(T) + Send + 'static>(self, f: F) {
        unsafe { (&*self.0).transfer(f) }
    }

    pub fn map<U: Send + 'static, F: FnOnce(T) -> U + Send + 'static>(self, f: F) -> Future<U> {
        let (rx, tx) = Future::new();
        let f = move |v| tx.complete(f(v));
        self.transfer(f);
        rx
    }
}

impl<T> Drop for Future<T> {
    fn drop(&mut self) {
        unsafe { (&*self.0).drop_future() };
    }
}

#[cfg(test)]
mod test {
    use super::Future;
    use crate::ops::EnqueueResp;
    use std::mem::size_of;
    use std::thread::Thread;
    use std::time::Duration;

    #[test]
    fn complete_then_map() {
        let (rx, tx) = Future::new();
        tx.complete("Hello, world!");
        let actual = rx.map(|v| (v, "Goodbye, world!")).take();
        assert_eq!(("Hello, world!", "Goodbye, world!"), actual);
    }

    #[test]
    fn map_then_complete() {
        let (rx, tx) = Future::new();
        let actual = rx.map(|v| (v, "Goodbye, world!"));
        tx.complete("Hello, world!");
        assert_eq!(("Hello, world!", "Goodbye, world!"), actual.take());
    }

    #[test]
    fn complete_then_take() {
        let (rx, tx) = Future::new();
        tx.complete("Hello, world!");
        let actual = rx.take();
        assert_eq!("Hello, world!", actual);
    }

    #[test]
    fn drop_promise() {
        let (rx, tx) = Future::<()>::new();
        rx.transfer(|_| {});

        // Dropping an incomplete Promise is not permitted and will panic.
        let res = std::panic::catch_unwind(|| drop(tx));
        assert!(res.is_err());
    }

    #[test]
    fn dropped_promise_panics_future() {
        // Intentionally drop the Promise and ensure Future::transfer panics.
        let (rx, tx) = Future::<()>::new();
        let _ = std::panic::catch_unwind(|| drop(tx));
        let res = std::panic::catch_unwind(|| rx.transfer(|_| {}));
        assert!(res.is_err());

        // Intentionally drop the Promise and ensure Future::take panics.
        let (rx, tx) = Future::<()>::new();
        let _ = std::panic::catch_unwind(|| drop(tx));
        let res = std::panic::catch_unwind(|| rx.take());
        assert!(res.is_err());
    }

    #[test]
    fn take_blocking() {
        let (rx, tx) = Future::new();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            tx.complete("Hello, world!");
        });
        let actual = rx.take();
        assert_eq!("Hello, world!", actual);
    }

    #[test]
    fn sizes() {
        dbg!(size_of::<super::Inner<EnqueueResp>>());
        dbg!(size_of::<super::Inner<()>>());
        dbg!(size_of::<Option<()>>());
        dbg!(size_of::<super::Data<()>>());
        dbg!(size_of::<Thread>());
        dbg!(size_of::<Box<dyn FnOnce(()) + Send>>());
    }
}
