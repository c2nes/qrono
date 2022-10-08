use std::{panic::AssertUnwindSafe, thread};

use crossbeam::channel::Sender;
use rayon::ThreadPool;
use std::process::abort;

pub trait Spawn: Send + Sync {
    fn spawn(&self, op: Box<dyn FnOnce() + Send>);
}

impl Spawn for ThreadPool {
    fn spawn(&self, op: Box<dyn FnOnce() + Send>) {
        ThreadPool::spawn(self, op)
    }
}

pub struct Unpooled;

impl Spawn for Unpooled {
    fn spawn(&self, op: Box<dyn FnOnce() + Send>) {
        thread::spawn(op);
    }
}

pub struct StaticPool {
    tasks_tx: Sender<Box<dyn FnOnce() + Send>>,
}

impl StaticPool {
    pub fn new(nthreads: usize) -> StaticPool {
        let (tasks_tx, tasks_rx) = crossbeam::channel::unbounded::<Box<dyn FnOnce() + Send>>();
        for i in 0..nthreads {
            let tasks_rx = tasks_rx.clone();
            thread::Builder::new()
                .name(format!("SchedulerThread-{}", i))
                .spawn(move || {
                    for task in tasks_rx {
                        if std::panic::catch_unwind(AssertUnwindSafe(task)).is_err() {
                            eprintln!("Unexpected panic; aborting");
                            abort()
                        }
                    }
                })
                .unwrap();
        }
        StaticPool { tasks_tx }
    }
}

impl Spawn for StaticPool {
    fn spawn(&self, op: Box<dyn FnOnce() + Send>) {
        self.tasks_tx.send(op).unwrap()
    }
}
