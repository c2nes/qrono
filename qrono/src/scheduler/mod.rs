mod error;
mod spawn;
mod tasks;
pub use error::*;
pub use spawn::*;
pub use tasks::*;

use std::fmt::Debug;

use crate::promise::{Future, Promise, TransferableFuture};
use crate::scheduler::ScheduleState::{Complete, Failed};

use parking_lot::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use ScheduleState::{Canceled, Idle, Rescheduled, Running, Scheduled};

pub type TaskResult<T> = Result<<T as Task>::Value, TaskError<T>>;
pub type TaskFuture<T> = Future<(T, TaskResult<T>)>;
type TaskPromise<T> = Promise<(T, TaskResult<T>)>;

#[derive(Clone)]
pub struct Scheduler {
    pool: Arc<dyn Spawn>,
}

impl Scheduler {
    pub fn new<T: Spawn + 'static>(pool: T) -> Scheduler {
        let pool: Arc<dyn Spawn> = Arc::new(pool);
        Scheduler { pool }
    }

    pub fn register<T: Task + 'static>(&self, task: T) -> (TaskHandle<T>, TaskFuture<T>) {
        let (promise, future) = Future::new();
        (
            TaskHandle {
                inner: TaskContext {
                    pool: Arc::clone(&self.pool),
                    f: Arc::new(Mutex::new(Some((task, promise)))),
                    state: Arc::new(AtomicScheduleState::new(Idle)),
                },
            },
            future,
        )
    }

    pub fn spawn<T: FnOnce() + Send + 'static>(&self, task: T) {
        self.pool.spawn(Box::new(task));
    }
}

pub trait Task: Send + Sized + 'static {
    type Value: Send;
    type Error: Send;

    fn run(&mut self, ctx: &TaskContext<Self>) -> Result<State<Self::Value>, Self::Error>;
}

pub enum State<T> {
    Idle,
    Runnable,
    Complete(T),
}

pub struct TaskHandle<T: Task> {
    inner: TaskContext<T>,
}

impl<T: Task> TaskHandle<T> {
    pub fn schedule(&self) -> ScheduleResult {
        self.inner.schedule()
    }

    pub fn cancel(&self) {
        self.inner.cancel();
    }
}

impl<T: Task> Drop for TaskHandle<T> {
    fn drop(&mut self) {
        self.cancel()
    }
}

pub struct TaskContext<T: Task> {
    pool: Arc<dyn Spawn>,
    #[allow(clippy::type_complexity)]
    f: Arc<Mutex<Option<(T, TaskPromise<T>)>>>,
    state: Arc<AtomicScheduleState>,
}

impl<T: Task> TaskContext<T> {
    #[inline]
    pub fn schedule(&self) -> ScheduleResult {
        match self.state.load() {
            Scheduled | Rescheduled => Ok(()),
            _ => self.schedule_slow(),
        }
    }

    fn schedule_slow(&self) -> ScheduleResult {
        loop {
            match self.state.load() {
                Idle => {
                    if self.state.compare_exchange(Idle, Scheduled).is_ok() {
                        self.submit();
                        return Ok(());
                    }
                }
                Running => {
                    if self.state.compare_exchange(Running, Rescheduled).is_ok() {
                        return Ok(());
                    }
                }
                Scheduled | Rescheduled => {
                    return Ok(());
                }
                Canceled => return Err(ScheduleError::Canceled),
                Failed => return Err(ScheduleError::Failed),
                Complete => return Err(ScheduleError::Complete),
            }
        }
    }

    fn cancel(&self) {
        while let state @ (Idle | Running | Scheduled | Rescheduled) = self.state.load() {
            if self.state.compare_exchange(state, Canceled).is_ok() {
                break;
            }
        }

        if let Some(mut f) = self.f.try_lock() {
            if let Some((task, promise)) = f.take() {
                promise.complete((task, Err(TaskError::Canceled)));
            }
        }
    }

    fn execute(&self) {
        // Mark ourselves as running.
        if let Err(state) = self.state.compare_exchange(Scheduled, Running) {
            match state {
                Canceled | Failed | Complete => return,
                state => panic!("unexpected state, {:?}", state),
            }
        }

        let task_state = match self.f.lock().as_mut() {
            Some((task, _)) => task.run(self),
            None => return,
        };

        let reschedule = match task_state {
            Ok(State::Idle) => false,
            Ok(State::Runnable) => true,
            Ok(State::Complete(value)) => {
                if let Some((task, promise)) = self.f.lock().take() {
                    promise.complete((task, Ok(value)));
                    self.state.store(Complete);
                }

                return;
            }
            Err(err) => {
                if let Some((task, promise)) = self.f.lock().take() {
                    promise.complete((task, Err(TaskError::Failed(err))));
                    self.state.store(Failed);
                }

                return;
            }
        };

        loop {
            match self.state.load() {
                Rescheduled => {
                    if self.state.compare_exchange(Rescheduled, Scheduled).is_ok() {
                        return self.submit();
                    }
                }
                Canceled => {
                    if let Some((task, promise)) = self.f.lock().take() {
                        promise.complete((task, Err(TaskError::Canceled)));
                    }
                    return;
                }
                Complete | Failed => panic!("BUG"),
                state => {
                    if reschedule {
                        if self.state.compare_exchange(state, Scheduled).is_ok() {
                            return self.submit();
                        }
                    } else if self.state.compare_exchange(state, Idle).is_ok() {
                        return;
                    }
                }
            };
        }
    }

    fn submit(&self) {
        let context = self.clone();
        self.pool.spawn(Box::new(move || context.execute()));
    }
}

impl<T: Task> Clone for TaskContext<T> {
    fn clone(&self) -> Self {
        let pool = self.pool.clone();
        let f = self.f.clone();
        let state = self.state.clone();
        TaskContext { pool, f, state }
    }
}

pub type ScheduleResult = Result<(), ScheduleError>;

#[repr(usize)]
#[derive(Copy, Clone, Debug)]
enum ScheduleState {
    Idle = 0,
    Scheduled = 1,
    Running = 2,
    Rescheduled = 3,
    Canceled = 4,
    Failed = 5,
    Complete = 6,
}

impl From<usize> for ScheduleState {
    fn from(v: usize) -> Self {
        match v {
            0 => Idle,
            1 => Scheduled,
            2 => Running,
            3 => Rescheduled,
            4 => Canceled,
            5 => Failed,
            6 => Complete,
            _ => panic!("no such state"),
        }
    }
}

impl From<ScheduleState> for usize {
    fn from(state: ScheduleState) -> Self {
        state as Self
    }
}

struct AtomicScheduleState(AtomicUsize);

impl AtomicScheduleState {
    fn new(state: ScheduleState) -> AtomicScheduleState {
        AtomicScheduleState(AtomicUsize::new(state.into()))
    }

    fn load(&self) -> ScheduleState {
        self.0.load(Ordering::Relaxed).into()
    }

    fn store(&self, val: ScheduleState) {
        self.0.store(val.into(), Ordering::Relaxed)
    }

    fn compare_exchange(
        &self,
        current: ScheduleState,
        new: ScheduleState,
    ) -> Result<ScheduleState, ScheduleState> {
        match self.0.compare_exchange(
            current.into(),
            new.into(),
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            Ok(state) => Ok(state.into()),
            Err(state) => Err(state.into()),
        }
    }
}

pub trait TransferAsync<T> {
    fn transfer_async<F: FnOnce(T) + Send + 'static>(self, scheduler: &Scheduler, handler: F);
}

impl<T: Send + 'static> TransferAsync<T> for TransferableFuture<T> {
    fn transfer_async<F: FnOnce(T) + Send + 'static>(self, scheduler: &Scheduler, handler: F) {
        let scheduler = scheduler.clone();
        self.transfer(move |val| scheduler.spawn(move || handler(val)))
    }
}
