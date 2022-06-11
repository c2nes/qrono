use parking_lot::{Condvar, Mutex};
use slab::Slab;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

pub type ID = usize;
pub type Deadline = Instant;
pub type Callback = Box<dyn FnOnce() + Send>;

#[derive(Default)]
struct Inner {
    deadlines: Slab<Deadline>,
    callbacks: BTreeMap<(Deadline, ID), Callback>,
}

impl Inner {
    fn schedule(&mut self, deadline: Deadline, callback: Callback) -> ID {
        let id = self.deadlines.insert(deadline) as ID;
        self.callbacks.insert((deadline, id), callback);
        id
    }

    fn cancel(&mut self, id: ID) {
        let deadline = self.deadlines.remove(id);
        self.callbacks.remove(&(deadline, id));
    }

    fn next_deadline(&self) -> Option<Deadline> {
        self.callbacks.keys().next().map(|(deadline, _)| *deadline)
    }

    fn take_ready_callbacks(&mut self, now: Instant) -> Vec<Callback> {
        // We use collect() here to terminate the iterator chain and
        // drop the immutable borrow of self so we can borrow it
        // mutably below.
        #[allow(clippy::needless_collect)]
        let ready_keys = self
            .callbacks
            .keys()
            .take_while(|(deadline, _)| *deadline <= now)
            .cloned()
            .collect::<Vec<_>>();

        ready_keys
            .into_iter()
            .map(|key| self.callbacks.remove(&key).unwrap())
            .collect::<Vec<_>>()
    }
}

// TODO: Add a way to stop the scheduler.
// TODO: Prevent the scheduler from being started twice.

#[derive(Default, Clone)]
pub struct Scheduler {
    inner: Arc<Mutex<Inner>>,
    notify: Arc<Condvar>,
}

impl Scheduler {
    pub fn new() -> Scheduler {
        Default::default()
    }

    pub fn schedule<F: FnOnce() + Send + 'static>(&self, deadline: Instant, callback: F) -> ID {
        let mut locked = self.inner.lock();
        match locked.callbacks.keys().next() {
            Some((head, _)) if deadline > *head => {}
            _ => {
                self.notify.notify_one();
            }
        }
        locked.schedule(deadline, Box::new(callback))
    }

    pub fn cancel(&self, id: ID) {
        self.inner.lock().cancel(id)
    }

    pub fn start(&self) {
        let inner = Arc::clone(&self.inner);
        let notify = Arc::clone(&self.notify);

        std::thread::spawn(move || loop {
            let ready_callbacks = {
                let mut locked = inner.lock();
                match locked.next_deadline() {
                    Some(deadline) => {
                        notify.wait_until(&mut locked, deadline);
                    }
                    None => {
                        notify.wait(&mut locked);
                    }
                }

                locked.take_ready_callbacks(now())
            };

            for callback in ready_callbacks {
                callback();
            }
        });
    }
}

#[cfg(not(test))]
fn now() -> Instant {
    Instant::now()
}

#[cfg(test)]
mod mock_time {
    use once_cell::sync::Lazy;
    use parking_lot::{Mutex, MutexGuard};
    use std::ops::Add;
    use std::sync;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, Instant};

    static BASE: Lazy<Instant> = Lazy::new(Instant::now);
    static LOCK: Mutex<ClockImpl> = Mutex::new(ClockImpl {});
    static OFFSET_NANOS: AtomicU64 = AtomicU64::new(0);

    pub trait Clock {
        fn advance(&self, offset: Duration);
    }

    struct ClockImpl {}

    impl Clock for ClockImpl {
        fn advance(&self, offset: Duration) {
            OFFSET_NANOS.fetch_add(offset.as_nanos() as u64, Ordering::Relaxed);
        }
    }

    // Tow issues:
    //   1) Tests run concurrently and fight over the offset.
    //   2) Using a thread local means the test thread and the scheduler thread
    //      do not have the same time.

    pub fn now() -> Instant {
        let offset = OFFSET_NANOS.load(Ordering::Relaxed);
        BASE.add(Duration::from_nanos(offset))
    }

    pub fn acquire() -> MutexGuard<'static, impl Clock> {
        let clock = LOCK.lock();
        OFFSET_NANOS.store(0, Ordering::Relaxed);
        clock
    }
}

#[cfg(test)]
use mock_time::now;

#[cfg(test)]
mod tests {
    use super::mock_time::Clock;
    use super::{now, Scheduler};
    use log::{info, LevelFilter};
    use qrono_channel::batch::channel;
    use std::sync::mpsc;
    use std::sync::mpsc::{Sender, TryRecvError};
    use std::time::{Duration, Instant};

    #[derive(Eq, PartialEq, Debug)]
    enum MockResult {
        Call(&'static str),
        Drop(&'static str),
    }

    struct MockCallback {
        chan: Sender<MockResult>,
        name: &'static str,
        sent: bool,
    }

    impl MockCallback {
        fn new(chan: &Sender<MockResult>, name: &'static str) -> impl FnOnce() + Send {
            let mut callback = MockCallback {
                chan: chan.clone(),
                name,
                sent: false,
            };
            move || callback.call()
        }

        fn call(&mut self) {
            self.chan.send(MockResult::Call(self.name)).unwrap();
            self.sent = true;
        }
    }

    impl Drop for MockCallback {
        fn drop(&mut self) {
            if !self.sent {
                self.chan.send(MockResult::Drop(self.name)).unwrap();
            }
        }
    }

    #[test]
    fn cancel_drops() {
        let clock = super::mock_time::acquire();
        let (tx, rx) = mpsc::channel();
        let scheduler = Scheduler::new();
        scheduler.start();
        scheduler.schedule(now(), MockCallback::new(&tx, "init"));
        assert_eq!(MockResult::Call("init"), rx.recv().unwrap());

        let id = scheduler.schedule(now() + Duration::from_secs(5), MockCallback::new(&tx, "1"));

        scheduler.cancel(id);
        assert_eq!(MockResult::Drop("1"), rx.recv().unwrap())
    }

    #[test]
    fn scheduled_run_on_start() {
        let clock = super::mock_time::acquire();
        let scheduler = Scheduler::new();
        let (tx, rx) = mpsc::channel();

        // These schedules should run in order of timestamp once the scheduler is started.
        scheduler.schedule(
            now() + Duration::from_millis(2),
            MockCallback::new(&tx, "2"),
        );
        scheduler.schedule(
            now() + Duration::from_millis(1),
            MockCallback::new(&tx, "1"),
        );
        scheduler.schedule(
            now() + Duration::from_millis(4),
            MockCallback::new(&tx, "4"),
        );
        scheduler.schedule(
            now() + Duration::from_millis(6),
            MockCallback::new(&tx, "6"),
        );
        scheduler.schedule(
            now() + Duration::from_millis(3),
            MockCallback::new(&tx, "3"),
        );
        scheduler.schedule(
            now() + Duration::from_millis(5),
            MockCallback::new(&tx, "5"),
        );

        // Advance the clock past all of the deadlines and ensure all of the callbacks
        // execute in order when we start the scheduler.
        clock.advance(Duration::from_millis(100));
        scheduler.start();

        assert_eq!(MockResult::Call("1"), rx.recv().unwrap());
        assert_eq!(MockResult::Call("2"), rx.recv().unwrap());
        assert_eq!(MockResult::Call("3"), rx.recv().unwrap());
        assert_eq!(MockResult::Call("4"), rx.recv().unwrap());
        assert_eq!(MockResult::Call("5"), rx.recv().unwrap());
        assert_eq!(MockResult::Call("6"), rx.recv().unwrap());
    }

    #[test]
    fn cancel_middle_schedule() {
        let clock = super::mock_time::acquire();
        let (tx, rx) = mpsc::channel();
        let scheduler = Scheduler::new();
        scheduler.start();

        scheduler.schedule(now(), MockCallback::new(&tx, "init"));
        assert_eq!(MockResult::Call("init"), rx.recv().unwrap());

        // These schedules should run in order of timestamp
        scheduler.schedule(
            now() + Duration::from_millis(2),
            MockCallback::new(&tx, "2"),
        );
        scheduler.schedule(
            now() + Duration::from_millis(1),
            MockCallback::new(&tx, "1"),
        );
        let id = scheduler.schedule(
            now() + Duration::from_millis(4),
            MockCallback::new(&tx, "4"),
        );
        scheduler.schedule(
            now() + Duration::from_millis(6),
            MockCallback::new(&tx, "6"),
        );
        scheduler.schedule(
            now() + Duration::from_millis(3),
            MockCallback::new(&tx, "3"),
        );
        scheduler.schedule(
            now() + Duration::from_millis(5),
            MockCallback::new(&tx, "5"),
        );

        // Dropped immediately
        scheduler.cancel(id);
        assert_eq!(MockResult::Drop("4"), rx.recv().unwrap());

        // Once the clock advances, the remaining callbacks should execute in order.
        clock.advance(Duration::from_millis(10));
        assert_eq!(MockResult::Call("1"), rx.recv().unwrap());
        assert_eq!(MockResult::Call("2"), rx.recv().unwrap());
        assert_eq!(MockResult::Call("3"), rx.recv().unwrap());
        assert_eq!(MockResult::Call("5"), rx.recv().unwrap());
        assert_eq!(MockResult::Call("6"), rx.recv().unwrap());
    }

    #[test]
    fn run_in_order() {
        let clock = super::mock_time::acquire();
        let (tx, rx) = mpsc::channel();
        let scheduler = Scheduler::new();
        scheduler.start();
        scheduler.schedule(now(), MockCallback::new(&tx, "init"));
        assert_eq!(MockResult::Call("init"), rx.recv().unwrap());

        // These schedules should run in order once the clock advances.
        scheduler.schedule(
            now() + Duration::from_millis(2),
            MockCallback::new(&tx, "2"),
        );
        scheduler.schedule(
            now() + Duration::from_millis(1),
            MockCallback::new(&tx, "1"),
        );
        scheduler.schedule(
            now() + Duration::from_millis(4),
            MockCallback::new(&tx, "4"),
        );
        scheduler.schedule(
            now() + Duration::from_millis(6),
            MockCallback::new(&tx, "6"),
        );
        scheduler.schedule(
            now() + Duration::from_millis(3),
            MockCallback::new(&tx, "3"),
        );
        scheduler.schedule(
            now() + Duration::from_millis(5),
            MockCallback::new(&tx, "5"),
        );

        clock.advance(Duration::from_micros(3500));
        assert_eq!(MockResult::Call("1"), rx.recv().unwrap());
        assert_eq!(MockResult::Call("2"), rx.recv().unwrap());
        assert_eq!(MockResult::Call("3"), rx.recv().unwrap());
        assert_eq!(Err(TryRecvError::Empty), rx.try_recv());

        clock.advance(Duration::from_micros(3500));
        assert_eq!(MockResult::Call("4"), rx.recv().unwrap());
        assert_eq!(MockResult::Call("5"), rx.recv().unwrap());
        assert_eq!(MockResult::Call("6"), rx.recv().unwrap());
        assert_eq!(Err(TryRecvError::Empty), rx.try_recv());
    }
}
