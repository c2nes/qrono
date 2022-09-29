use crate::transfer;
use crate::transfer::{Receiver, Sender};
use parking_lot::Mutex;
use std::mem;

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

pub use transferable::Future as TransferableFuture;

enum Completable<T> {
    Cell(Sender<T>),
    Handler(Box<dyn FnOnce(T) + Send>),
    StdFuture(Sender<T>, Arc<Mutex<Option<Waker>>>),
}

impl<T> Completable<T> {
    fn complete(self, val: T) {
        match self {
            Completable::Cell(sender) => sender.send(val),
            Completable::Handler(handler) => handler(val),
            Completable::StdFuture(sender, waker) => {
                sender.send(val);
                if let Some(waker) = waker.lock().take() {
                    waker.wake();
                }
            }
        }
    }

    fn is_cancelled(&self) -> bool {
        match self {
            Completable::Cell(sender) | Completable::StdFuture(sender, _) => sender.is_orphaned(),
            Completable::Handler(_) => false,
        }
    }
}

enum Callbacks {
    None,
    One(Box<dyn FnOnce() + Send + 'static>),
    Some(Vec<Box<dyn FnOnce() + Send + 'static>>),
}

impl Callbacks {
    fn add<F: FnOnce() + Send + 'static>(&mut self, callback: F) {
        match self {
            Callbacks::None => *self = Callbacks::One(Box::new(callback)),
            Callbacks::One(_) => {
                let other = match mem::replace(self, Callbacks::None) {
                    Callbacks::One(other) => other,
                    _ => panic!(),
                };
                *self = Callbacks::Some(vec![other, Box::new(callback)])
            }
            Callbacks::Some(callbacks) => callbacks.push(Box::new(callback)),
        }
    }

    fn call(self) {
        match self {
            Callbacks::None => {}
            Callbacks::One(callback) => callback(),
            Callbacks::Some(callbacks) => {
                for callback in callbacks {
                    callback();
                }
            }
        }
    }
}

#[must_use = "dropping an incomplete Promise<T> will cause the corresponding Future<T> to panic"]
pub struct Promise<T> {
    completable: Completable<T>,
    callbacks: Callbacks,
}

impl<T> Promise<T> {
    pub fn complete(self, val: T) {
        self.completable.complete(val);
        self.callbacks.call();
    }

    pub fn is_cancelled(&self) -> bool {
        self.completable.is_cancelled()
    }

    /// Register the callback to be invoked when this Promise is completed.
    pub fn on_complete<F: FnOnce() + Send + 'static>(&mut self, callback: F) {
        self.callbacks.add(callback);
    }
}

pub struct Future<T> {
    cell: Receiver<T>,
}

impl<T> Future<T> {
    pub fn new() -> (Promise<T>, Future<T>) {
        let (tx, rx) = transfer::pair();
        (
            Promise {
                completable: Completable::Cell(tx),
                callbacks: Callbacks::None,
            },
            Future { cell: rx },
        )
    }

    pub fn new_std() -> (Promise<T>, impl std::future::Future<Output = T>) {
        let (tx, rx) = transfer::pair();
        let waker = Arc::new(Mutex::new(None));
        let promise = Promise {
            completable: Completable::StdFuture(tx, Arc::clone(&waker)),
            callbacks: Callbacks::None,
        };
        let future = StdFuture {
            cell: Some(rx),
            waker,
        };
        (promise, future)
    }

    pub fn completed(val: T) -> Future<T> {
        Future {
            cell: transfer::ready(val),
        }
    }

    pub fn transfer<F: FnOnce(T) + Send + 'static>(f: F) -> Promise<T>
    where
        T: Send + 'static,
    {
        Promise {
            completable: Completable::Handler(Box::new(f)),
            callbacks: Callbacks::None,
        }
    }

    pub fn transferable() -> (Promise<T>, TransferableFuture<T>)
    where
        T: Send + 'static,
    {
        TransferableFuture::new()
    }

    pub fn map<U: Send + 'static, F: FnOnce(T) -> U + Send + 'static>(
        f: F,
    ) -> (Promise<T>, Future<U>)
    where
        T: Send + 'static,
    {
        let (tx1, rx1) = transfer::pair();
        let tx0 = Future::transfer(move |val| {
            tx1.send(f(val));
        });
        (tx0, Future { cell: rx1 })
    }

    pub fn is_complete(&self) -> bool {
        self.cell.is_sent()
    }

    pub fn take(self) -> T {
        self.cell.receive()
    }

    pub fn try_take(self) -> Result<T, Self> {
        match self.cell.try_receive() {
            Ok(val) => Ok(val),
            Err(cell) => Err(Future { cell }),
        }
    }
}

struct StdFuture<T> {
    cell: Option<Receiver<T>>,
    waker: Arc<Mutex<Option<Waker>>>,
}

impl<T> Unpin for StdFuture<T> {}

impl<T> std::future::Future for StdFuture<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let cell = self.cell.take();
        match cell.expect("already polled").try_receive() {
            Ok(value) => Poll::Ready(value),
            Err(cell) => {
                // Not ready
                let mut locked = self.waker.lock();

                // Double check for race condition
                if cell.is_sent() {
                    return Poll::Ready(cell.receive());
                }

                *locked = Some(cx.waker().clone());
                drop(locked);
                self.cell = Some(cell);
                Poll::Pending
            }
        }
    }
}

mod transferable {
    use crate::Promise;
    use parking_lot::{Condvar, Mutex};
    use std::mem;
    use std::sync::Arc;

    pub struct Future<T> {
        inner: Arc<Inner<T>>,
    }

    struct Inner<T> {
        state: Mutex<State<T>>,
        ready: Condvar,
    }

    impl<T> Inner<T> {
        fn new() -> Self {
            Self {
                state: Default::default(),
                ready: Default::default(),
            }
        }

        fn complete(&self, val: T) {
            let mut state = self.state.lock();
            state.store_value(val);
            self.ready.notify_one();
        }
    }

    enum State<T> {
        None,
        Value(T),
        Transfer(Box<dyn FnOnce(T) + Send>),
    }

    impl<T> State<T> {
        fn store_value(&mut self, val: T) {
            match mem::take(self) {
                Self::None => *self = Self::Value(val),
                Self::Value(_) => panic!(),
                Self::Transfer(f) => f(val),
            }
        }

        fn transfer<F: FnOnce(T) + Send + 'static>(&mut self, callback: F) {
            match mem::take(self) {
                Self::None => *self = Self::Transfer(Box::new(callback)),
                Self::Value(val) => callback(val),
                Self::Transfer(_) => panic!(),
            }
        }

        fn try_take(&mut self) -> Option<T> {
            match mem::take(self) {
                State::None => None,
                State::Value(val) => Some(val),
                State::Transfer(_) => panic!(),
            }
        }

        fn is_complete(&self) -> bool {
            matches!(self, State::Value(_))
        }
    }

    impl<T> Default for State<T> {
        fn default() -> Self {
            Self::None
        }
    }

    impl<T> Future<T> {
        pub(super) fn new() -> (Promise<T>, Future<T>)
        where
            T: Send + 'static,
        {
            let inner = Arc::new(Inner::new());
            let promise = {
                let inner = inner.clone();
                super::Future::transfer(move |val| inner.complete(val))
            };
            (promise, Self { inner })
        }

        pub fn is_complete(&self) -> bool {
            self.inner.state.lock().is_complete()
        }

        pub fn take(self) -> T {
            let mut state = self.inner.state.lock();
            loop {
                match state.try_take() {
                    Some(val) => return val,
                    None => self.inner.ready.wait(&mut state),
                }
            }
        }

        pub fn try_take(self) -> Result<T, Self> {
            if let Some(val) = self.inner.state.lock().try_take() {
                return Ok(val);
            }
            Err(self)
        }

        pub fn transfer<F: FnOnce(T) + Send + 'static>(self, callback: F) {
            self.inner.state.lock().transfer(callback)
        }
    }
}

#[cfg(test)]
mod test {
    use super::Future;
    use parking_lot::Mutex;
    use std::panic::AssertUnwindSafe;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::Relaxed;
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn complete_then_take() {
        let (tx, rx) = Future::new();
        tx.complete("Hello, world!");
        let actual = rx.take();
        assert_eq!("Hello, world!", actual);
    }

    #[test]
    fn dropped_promise_panics_future() {
        // Intentionally drop the Promise and ensure Future::take panics.
        let (tx, rx) = Future::<()>::new();
        drop(tx);
        let res = std::panic::catch_unwind(AssertUnwindSafe(|| rx.take()));
        assert!(res.is_err());
    }

    #[test]
    fn map_then_complete() {
        let (tx, rx) = Future::map(|v| (v, "Goodbye, world!"));
        tx.complete("Hello, world!");
        assert_eq!(("Hello, world!", "Goodbye, world!"), rx.take());
    }

    #[test]
    fn drop_receiver() {
        let (tx, rx) = Future::new();
        drop(rx);
        assert!(tx.is_cancelled());
        // Should not panic
        tx.complete("Hello, world!");
    }

    #[test]
    fn take_blocking() {
        let (tx, rx) = Future::new();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            tx.complete("Hello, world!");
        });
        let actual = rx.take();
        assert_eq!("Hello, world!", actual);
    }

    #[test]
    fn single_callback() {
        let (mut tx, _) = Future::new();
        let counter = Arc::new(AtomicUsize::new(0));
        {
            let counter = Arc::clone(&counter);
            tx.on_complete(move || {
                counter.fetch_add(1, Relaxed);
            });
        }

        tx.complete("Hello, world!");
        assert_eq!(1, counter.load(Relaxed));
    }

    #[test]
    fn multiple_callbacks() {
        let (mut tx, _) = Future::new();
        let counter = Arc::new(AtomicUsize::new(0));
        for _ in 0..10 {
            let counter = Arc::clone(&counter);
            tx.on_complete(move || {
                counter.fetch_add(1, Relaxed);
            });
        }

        tx.complete("Hello, world!");
        assert_eq!(10, counter.load(Relaxed));
    }

    #[test]
    fn transfer_before_completion() {
        let (tx, rx) = Future::transferable();
        let dest = Arc::new(Mutex::new(None));
        {
            let dest = Arc::clone(&dest);
            rx.transfer(move |v| {
                dest.lock().replace(v);
            });
        }
        tx.complete("Hello, world!");
        assert_eq!(Some("Hello, world!"), *dest.lock());
    }

    #[test]
    fn transfer_after_completion() {
        let (tx, rx) = Future::transferable();
        tx.complete("Hello, world!");
        let dest = Arc::new(Mutex::new(None));
        {
            let dest = Arc::clone(&dest);
            rx.transfer(move |v| {
                dest.lock().replace(v);
            });
        }
        assert_eq!(Some("Hello, world!"), *dest.lock());
    }

    #[test]
    fn completed() {
        let rx = Future::completed("Hello, world!");
        assert!(rx.is_complete());
        assert_eq!("Hello, world!", rx.take());
    }
}
