use parking_lot_core::{ParkResult, DEFAULT_PARK_TOKEN, DEFAULT_UNPARK_TOKEN};
use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ptr;
use std::ptr::NonNull;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release};

type State = u8;

const SENT_BIT: State = 1 << 0;
const DROPPED_BIT: State = 1 << 1;
const PARKED_BIT: State = 1 << 2;

pub struct Sender<T> {
    inner: NonNull<Inner<T>>,
    phantom: PhantomData<Inner<T>>,
    // Copy of state, as read by send(). Used by Drop.
    state: State,
}

unsafe impl<T: Send> Send for Sender<T> {}
unsafe impl<T: Send> Sync for Sender<T> {}

pub struct Receiver<T> {
    inner: NonNull<Inner<T>>,
    phantom: PhantomData<Inner<T>>,
    // Copy of state, as read by receive(). Used by Drop.
    state: State,
}

unsafe impl<T: Send> Send for Receiver<T> {}
unsafe impl<T: Send> Sync for Receiver<T> {}

struct Inner<T> {
    state: AtomicU8,
    value: MaybeUninit<UnsafeCell<T>>,
}

impl<T> Inner<T> {
    fn is_orphaned(&self) -> bool {
        // Receiver has dropped
        self.state.load(Relaxed) & DROPPED_BIT != 0
    }

    fn is_sent(&self) -> bool {
        // Receiver has dropped
        self.state.load(Relaxed) & SENT_BIT != 0
    }

    unsafe fn send(&self, val: T) -> State {
        UnsafeCell::raw_get(self.value.as_ptr()).write(val);

        match self
            .state
            .compare_exchange_weak(0, DROPPED_BIT | SENT_BIT, Release, Relaxed)
        {
            Ok(state) => state,
            Err(_) => self.send_slow(),
        }
    }

    unsafe fn send_slow(&self) -> State {
        let prev = self.state.fetch_add(DROPPED_BIT | SENT_BIT, Release);
        if prev & DROPPED_BIT != 0 {
            // Receiver is already dropped. Make sure to drop the sent value.
            ptr::drop_in_place(self.value.assume_init_ref().get());
        } else if prev & PARKED_BIT != 0 {
            let key = self as *const _ as usize;
            parking_lot_core::unpark_one(key, |_res| DEFAULT_UNPARK_TOKEN);
        }

        prev
    }

    unsafe fn take_value(&self) -> T {
        self.value.as_ptr().read().into_inner()
    }
}

impl<T> Sender<T> {
    pub fn is_orphaned(&self) -> bool {
        unsafe { self.inner.as_ref() }.is_orphaned()
    }

    pub fn send(mut self, val: T) {
        let state = unsafe { self.inner.as_ref().send(val) };
        self.state = state | SENT_BIT;
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        if self.state & SENT_BIT != 0 {
            if self.state & DROPPED_BIT != 0 {
                drop(unsafe { Box::from_raw(self.inner.as_ptr()) });
            }
        } else {
            let inner = unsafe { self.inner.as_mut() };
            let prev = inner.state.fetch_add(DROPPED_BIT, Relaxed);

            if prev & DROPPED_BIT != 0 {
                drop(unsafe { Box::from_raw(inner) });
            } else if prev & PARKED_BIT != 0 {
                let key = self.inner.as_ptr() as usize;
                unsafe { parking_lot_core::unpark_one(key, |_res| DEFAULT_UNPARK_TOKEN) };
            }
        }
    }
}

impl<T> Receiver<T> {
    pub fn is_sent(&self) -> bool {
        unsafe { self.inner.as_ref() }.is_sent()
    }

    pub fn try_receive(mut self) -> Result<T, Self> {
        unsafe {
            let inner = self.inner.as_mut();
            self.state = inner.state.load(Acquire);
            if self.state & SENT_BIT != 0 {
                Ok(inner.take_value())
            } else {
                Err(self)
            }
        }
    }

    pub fn receive(mut self) -> T {
        unsafe {
            let inner = self.inner.as_mut();
            self.state = inner.state.load(Acquire);
            if self.state & SENT_BIT != 0 {
                inner.take_value()
            } else {
                self.receive_slow()
            }
        }
    }

    unsafe fn receive_slow(mut self) -> T {
        if self.state & DROPPED_BIT != 0 {
            panic!("Sender<T> dropped without sending");
        }

        let inner = self.inner.as_ref();
        inner.state.fetch_or(PARKED_BIT, Relaxed);
        let key = self.inner.as_ptr() as usize;
        let validate = || inner.state.load(Relaxed) == PARKED_BIT;
        let before_sleep = || {};
        let timed_out = |_key, _last_thread| {};
        match parking_lot_core::park(
            key,
            validate,
            before_sleep,
            timed_out,
            DEFAULT_PARK_TOKEN,
            None,
        ) {
            ParkResult::Unparked(_) => {}
            ParkResult::Invalid => {}
            ParkResult::TimedOut => {}
        }

        self.state = inner.state.load(Acquire);
        if self.state & SENT_BIT != 0 {
            inner.take_value()
        } else {
            panic!("Sender<T> dropped without sending");
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        // If a value was received then self.state will have been initialized.
        // If a value was received then the sender must have dropped or sent a value.
        // If that is the case, then either DROPPED_BIT or SENT_BIT will be set.

        unsafe {
            let inner = self.inner.as_mut();
            // If self.state has the dropped bit sent then we also read any value present.
            if self.state & DROPPED_BIT != 0 {
                drop(Box::from_raw(inner));
                return;
            }

            let prev = inner.state.fetch_add(DROPPED_BIT, Acquire);
            if self.state & SENT_BIT == 0 && prev & SENT_BIT != 0 {
                ptr::drop_in_place(inner.value.as_mut_ptr());
            }
            if prev & DROPPED_BIT != 0 {
                drop(Box::from_raw(inner))
            }
        }
    }
}

pub fn pair<T>() -> (Sender<T>, Receiver<T>) {
    let inner = Box::new(Inner {
        state: 0.into(),
        value: MaybeUninit::uninit(),
    });
    let inner = unsafe { NonNull::new_unchecked(Box::into_raw(inner)) };

    (
        Sender {
            inner,
            phantom: PhantomData,
            state: 0,
        },
        Receiver {
            inner,
            phantom: PhantomData,
            state: 0,
        },
    )
}

pub fn ready<T>(val: T) -> Receiver<T> {
    let inner = Box::new(Inner {
        state: (DROPPED_BIT | SENT_BIT).into(),
        value: MaybeUninit::new(UnsafeCell::new(val)),
    });

    let inner = unsafe { NonNull::new_unchecked(Box::into_raw(inner)) };

    Receiver {
        inner,
        phantom: Default::default(),
        state: 0,
    }
}

#[cfg(test)]
mod test {
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::time::Duration;

    #[test]
    pub fn receive_non_blocking() {
        let (tx, rx) = super::pair();
        tx.send("Hello, world!");
        assert_eq!("Hello, world!", rx.receive());
    }

    #[test]
    pub fn receive_blocking() {
        let (tx, rx) = super::pair();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            tx.send("Hello, world!");
        });
        assert_eq!("Hello, world!", rx.receive());
    }

    #[test]
    pub fn check_sent() {
        let (tx, rx) = super::pair();
        tx.send("Hello, world!");
        assert!(rx.is_sent());
    }

    #[test]
    pub fn not_sent_on_tx_drop() {
        let (tx, rx) = super::pair::<()>();
        drop(tx);
        assert!(!rx.is_sent());
    }

    #[test]
    pub fn panic_on_tx_drop_non_blocking() {
        let (tx, rx) = super::pair::<()>();
        drop(tx);

        assert!(catch_unwind(AssertUnwindSafe(|| rx.receive())).is_err());
    }

    #[test]
    pub fn panic_on_tx_drop_blocking() {
        let (tx, rx) = super::pair::<()>();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            drop(tx);
        });

        assert!(catch_unwind(AssertUnwindSafe(|| rx.receive())).is_err());
    }

    #[test]
    pub fn try_receive() {
        let (tx, rx) = super::pair();
        let res = rx.try_receive();
        assert!(res.is_err());
        let rx = res.err().unwrap();
        tx.send("Hello, world!");
        let res = rx.try_receive();
        assert_eq!(Some("Hello, world!"), res.ok());
    }

    #[test]
    pub fn ready_future() {
        let rx = super::ready("Hello, world!");
        assert!(rx.is_sent());
        assert_eq!("Hello, world!", rx.receive());
    }
}
