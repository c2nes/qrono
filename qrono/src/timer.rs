use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use parking_lot::{Condvar, Mutex};
use slab::Slab;

pub type ID = usize;
pub type Deadline = Instant;
pub type Callback = Box<dyn FnOnce() + Send>;

#[derive(Default)]
struct Inner {
    deadlines: Slab<Deadline>,
    callbacks: BTreeMap<(Deadline, ID), Callback>,

    #[cfg(test)]
    waiter_observed_schedule_count: Option<usize>,
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

        for (_, id) in &ready_keys {
            self.deadlines.remove(*id as usize);
        }

        ready_keys
            .into_iter()
            .map(|key| self.callbacks.remove(&key).unwrap())
            .collect::<Vec<_>>()
    }
}

// TODO: Add a way to stop the scheduler.

/// A scheduler allows callbacks to be scheduled for execution in the future.
#[derive(Clone)]
pub struct Scheduler {
    inner: Arc<Mutex<Inner>>,
    notify: Arc<Condvar>,
    #[cfg(test)]
    notify_waiting: Arc<Condvar>,
}

impl Scheduler {
    pub fn new() -> Scheduler {
        let scheduler = Scheduler::new_unstarted();
        scheduler.start();
        scheduler
    }

    fn new_unstarted() -> Scheduler {
        Scheduler {
            inner: Arc::new(Default::default()),
            notify: Arc::new(Default::default()),
            #[cfg(test)]
            notify_waiting: Arc::new(Default::default()),
        }
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
        let cloned = self.clone();
        std::thread::spawn(move || cloned.run_loop());
    }

    fn run_loop(&self) {
        loop {
            let ready_callbacks = {
                let mut locked = self.inner.lock();
                #[cfg(test)]
                {
                    locked.waiter_observed_schedule_count = Some(locked.deadlines.len());
                    self.notify_waiting.notify_all();
                }
                match locked.next_deadline() {
                    Some(deadline) => {
                        self.notify.wait_until(&mut locked, deadline);
                    }
                    None => {
                        self.notify.wait(&mut locked);
                    }
                }
                #[cfg(test)]
                {
                    locked.waiter_observed_schedule_count = None;
                }
                locked.take_ready_callbacks(Instant::now())
            };

            for callback in ready_callbacks {
                callback();
            }
        }
    }

    #[cfg(test)]
    fn expect_observed_schedule_count(&self, expected: usize) {
        use std::time::Duration;
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut locked = self.inner.lock();
        while locked.waiter_observed_schedule_count != Some(expected) {
            let res = self.notify_waiting.wait_until(&mut locked, deadline);
            let actual = (*locked).waiter_observed_schedule_count;
            assert!(
                !res.timed_out(),
                "expected {expected} observed schedules, actually {actual:?}"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::mpsc;
    use std::sync::mpsc::Sender;
    use std::time::Duration;
    use std::time::Instant;

    use super::Scheduler;

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
    fn smoke() {
        let (tx, rx) = mpsc::channel();
        let scheduler = Scheduler::new();
        scheduler.schedule(
            Instant::now() + Duration::from_millis(10),
            MockCallback::new(&tx, "1"),
        );
        assert_eq!(MockResult::Call("1"), rx.recv().unwrap());
        let locked = scheduler.inner.lock();
        assert_eq!(0, locked.callbacks.len());
        assert_eq!(0, locked.deadlines.len());
    }

    #[test]
    fn cancel_drops() {
        let (tx, rx) = mpsc::channel();
        let scheduler = Scheduler::new();
        scheduler.expect_observed_schedule_count(0);

        // Verify that canceling a schedule drops it.
        let id = scheduler.schedule(
            Instant::now() + Duration::from_secs(60),
            MockCallback::new(&tx, "1"),
        );
        scheduler.cancel(id);
        assert_eq!(MockResult::Drop("1"), rx.recv().unwrap())
    }

    #[test]
    fn schedules_run_in_order() {
        let (tx, rx) = mpsc::channel();
        // Create, but do not start the scheduler. This lets us install the desired schedules
        // before allowing any to run so we can verify they run in the expected order.
        let scheduler = Scheduler::new_unstarted();

        // These schedules should run in order of timestamp once the scheduler is started.
        scheduler.schedule(
            Instant::now() + Duration::from_millis(2),
            MockCallback::new(&tx, "2"),
        );
        scheduler.schedule(
            Instant::now() + Duration::from_millis(1),
            MockCallback::new(&tx, "1"),
        );
        scheduler.schedule(
            Instant::now() + Duration::from_millis(4),
            MockCallback::new(&tx, "4"),
        );
        scheduler.schedule(
            Instant::now() + Duration::from_millis(6),
            MockCallback::new(&tx, "6"),
        );
        scheduler.schedule(
            Instant::now() + Duration::from_millis(3),
            MockCallback::new(&tx, "3"),
        );
        scheduler.schedule(
            Instant::now() + Duration::from_millis(5),
            MockCallback::new(&tx, "5"),
        );

        // Ensure all schedules run in the expected order
        scheduler.start();
        assert_eq!(MockResult::Call("1"), rx.recv().unwrap());
        assert_eq!(MockResult::Call("2"), rx.recv().unwrap());
        assert_eq!(MockResult::Call("3"), rx.recv().unwrap());
        assert_eq!(MockResult::Call("4"), rx.recv().unwrap());
        assert_eq!(MockResult::Call("5"), rx.recv().unwrap());
        assert_eq!(MockResult::Call("6"), rx.recv().unwrap());
    }

    #[test]
    fn wake_for_new_schedule() {
        let (tx, rx) = mpsc::channel();
        let scheduler = Scheduler::new();

        // The scheduler thread should initially go to sleep
        // after observing 0 pending schedules.
        scheduler.expect_observed_schedule_count(0);

        // Schedule a task far in future.
        let id1 = scheduler.schedule(
            Instant::now() + Duration::from_secs(3600),
            MockCallback::new(&tx, "1"),
        );

        // The scheduler thread should be woken, see the newly added schedule,
        // and then go back to sleep waiting for the scheduled deadline to arrive.
        scheduler.expect_observed_schedule_count(1);

        // Schedule a second task, still in the future, but sooner than the first.
        let id2 = scheduler.schedule(
            Instant::now() + Duration::from_secs(1800),
            MockCallback::new(&tx, "2"),
        );

        // Since we have a new soonest schedule the scheduler thread should be woken,
        // see the new soonest schedule, and go back to sleep waiting for it.
        scheduler.expect_observed_schedule_count(2);

        // Next, we schedule a third task to expire very near in the future. This too
        // should wake the scheduler thread and the callback should be invoked quite soon.
        scheduler.schedule(
            Instant::now() + Duration::from_millis(10),
            MockCallback::new(&tx, "3"),
        );
        assert_eq!(
            MockResult::Call("3"),
            rx.recv_timeout(Duration::from_secs(10)).unwrap()
        );

        // Cancel the first two tasks.
        scheduler.cancel(id1);
        assert_eq!(MockResult::Drop("1"), rx.recv().unwrap());
        scheduler.cancel(id2);
        assert_eq!(MockResult::Drop("2"), rx.recv().unwrap());

        // Ensure the scheduler is now empty.
        let locked = scheduler.inner.lock();
        assert_eq!(0, locked.callbacks.len());
        assert_eq!(0, locked.deadlines.len());
    }
}
