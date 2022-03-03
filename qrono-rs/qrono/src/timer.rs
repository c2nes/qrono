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
}

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
                match locked.callbacks.keys().next().cloned() {
                    Some((deadline, _)) => {
                        notify.wait_until(&mut locked, deadline);
                    }
                    None => {
                        notify.wait(&mut locked);
                    }
                }

                let now = Instant::now();

                // We use collect() here to terminate the iterator chain and
                // drop the immutable borrow of `locked` so we can borrow it
                // mutably below.
                #[allow(clippy::needless_collect)]
                let ready_keys = locked
                    .callbacks
                    .keys()
                    .take_while(|(deadline, _)| *deadline <= now)
                    .cloned()
                    .collect::<Vec<_>>();

                ready_keys
                    .into_iter()
                    .map(|key| locked.callbacks.remove(&key).unwrap())
                    .collect::<Vec<_>>()
            };

            for callback in ready_callbacks {
                callback();
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use crate::timer::Scheduler;
    use log::{info, LevelFilter};
    use std::time::{Duration, Instant};

    #[test]
    fn test() {
        env_logger::Builder::new()
            .filter_level(LevelFilter::Info)
            .format_timestamp_micros()
            .parse_default_env()
            .init();

        let sched = Scheduler::new();
        sched.start();
        let handle1 = sched.schedule(Instant::now() + Duration::from_millis(250), || {
            info!("hi 1");
        });

        {
            let sched2 = sched.clone();
            sched.schedule(Instant::now() + Duration::from_millis(150), move || {
                info!("hi 2");
                sched2.cancel(handle1);
            })
        };

        sched.schedule(Instant::now() + Duration::from_millis(350), || {
            info!("hi 3");
        });

        std::thread::sleep(Duration::from_millis(500));
    }
}
