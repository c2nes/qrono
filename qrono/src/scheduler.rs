use rayon::ThreadPool;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};

use crate::promise::{Future, Promise, TransferableFuture};
use crate::scheduler::ScheduleState::{Complete, Failed};
use crossbeam::channel::Sender;
use parking_lot::Mutex;
use std::panic::AssertUnwindSafe;
use std::process::abort;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use ScheduleState::{Canceled, Idle, Rescheduled, Running, Scheduled};

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

#[derive(Clone)]
pub struct Scheduler {
    pool: Arc<dyn Spawn>,
}

pub struct TaskHandle<T: Task + 'static> {
    inner: TaskContext<T>,
}

pub struct TaskContext<T: Task + 'static> {
    pool: Arc<dyn Spawn>,
    #[allow(clippy::type_complexity)]
    f: Arc<Mutex<Option<(T, TaskPromise<T>)>>>,
    state: Arc<AtomicScheduleState>,
}

impl<T: Task + 'static> Clone for TaskContext<T> {
    fn clone(&self) -> Self {
        let pool = self.pool.clone();
        let f = self.f.clone();
        let state = self.state.clone();
        TaskContext { pool, f, state }
    }
}

impl<T: Task + 'static> TaskHandle<T> {
    pub fn schedule(&self) -> ScheduleResult {
        self.inner.schedule()
    }

    pub fn cancel(&self) {
        self.inner.cancel();
        if let Some(mut f) = self.inner.f.try_lock() {
            if let Some((task, promise)) = f.take() {
                promise.complete((task, Err(TaskError::Canceled)));
            }
        }
    }
}

impl<T: Task + 'static> Drop for TaskHandle<T> {
    fn drop(&mut self) {
        self.cancel()
    }
}

pub enum TaskError<T: Task> {
    Canceled,
    Failed(T::Error),
}

impl<T> Debug for TaskError<T>
where
    T: Task,
    T::Error: Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self {
            TaskError::Canceled => write!(f, "canceled"),
            TaskError::Failed(err) => write!(f, "failed: {:?}", err),
        }
    }
}

pub type TaskResult<T> = Result<<T as Task>::Value, TaskError<T>>;
pub type TaskFuture<T> = Future<(T, TaskResult<T>)>;
type TaskPromise<T> = Promise<(T, TaskResult<T>)>;

pub enum State<T> {
    Idle,
    Runnable,
    Complete(T),
}

pub trait Task: Send + Sized {
    const IDLE: Result<State<Self::Value>, Self::Error> = Ok(State::Idle);

    type Value: Send;
    type Error: Send;

    fn run(&mut self, ctx: &TaskContext<Self>) -> Result<State<Self::Value>, Self::Error>;
}

pub trait SimpleTask: Send {
    fn run(&mut self) -> bool;
}

impl<T: SimpleTask> Task for T {
    type Value = ();
    type Error = ();

    fn run(&mut self, _: &TaskContext<T>) -> Result<State<Self::Value>, Self::Error> {
        Ok(if self.run() {
            State::Runnable
        } else {
            State::Idle
        })
    }
}

pub struct FnTask(Box<dyn FnMut() -> bool + Send + 'static>);

impl FnTask {
    pub fn new<F: FnMut() -> bool + Send + 'static>(task: F) -> FnTask {
        FnTask(Box::new(task))
    }
}

impl SimpleTask for FnTask {
    fn run(&mut self) -> bool {
        self.0()
    }
}

impl<F: FnMut() -> bool + Send + 'static> From<F> for FnTask {
    fn from(f: F) -> Self {
        FnTask(Box::new(f))
    }
}

impl<T: Task + 'static> TaskContext<T> {
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
        loop {
            match self.state.load() {
                state @ (Idle | Running | Scheduled | Rescheduled) => {
                    if self.state.compare_exchange(state, Canceled).is_ok() {
                        return;
                    }
                }
                Canceled | Failed | Complete => return,
            }
        }
    }

    fn submit(&self) {
        let context = self.clone();
        self.pool.spawn(Box::new(move || {
            // Mark ourselves as running.
            if let Err(state) = context.state.compare_exchange(Scheduled, Running) {
                match state {
                    Canceled | Failed | Complete => return,
                    state => panic!("unexpected state, {:?}", state),
                }
            }

            let task_state = match context.f.lock().as_mut() {
                Some((task, _)) => task.run(&context),
                None => return,
            };

            let reschedule = match task_state {
                Ok(State::Idle) => false,
                Ok(State::Runnable) => true,
                Ok(State::Complete(value)) => {
                    if let Some((task, promise)) = context.f.lock().take() {
                        promise.complete((task, Ok(value)));
                        context.state.store(Complete);
                    }

                    return;
                }
                Err(err) => {
                    if let Some((task, promise)) = context.f.lock().take() {
                        promise.complete((task, Err(TaskError::Failed(err))));
                        context.state.store(Failed);
                    }

                    return;
                }
            };

            loop {
                match context.state.load() {
                    Rescheduled => {
                        if context
                            .state
                            .compare_exchange(Rescheduled, Scheduled)
                            .is_ok()
                        {
                            return context.submit();
                        }
                    }
                    Canceled => {
                        if let Some((task, promise)) = context.f.lock().take() {
                            promise.complete((task, Err(TaskError::Canceled)));
                        }
                        return;
                    }
                    Complete | Failed => panic!("BUG"),
                    state => {
                        if reschedule {
                            if context.state.compare_exchange(state, Scheduled).is_ok() {
                                return context.submit();
                            }
                        } else if context.state.compare_exchange(state, Idle).is_ok() {
                            return;
                        }
                    }
                };
            }
        }));
    }
}

#[derive(Debug, Copy, Clone)]
pub enum ScheduleError {
    Canceled,
    Failed,
    Complete,
}

impl Display for ScheduleError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for ScheduleError {}

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

impl Scheduler {
    pub fn new<T: Spawn + 'static>(pool: T) -> Scheduler {
        let pool: Arc<dyn Spawn> = Arc::new(pool);
        Scheduler { pool }
    }

    pub fn register_fn<F: FnMut() -> bool + Send + 'static>(
        &self,
        task: F,
    ) -> (TaskHandle<FnTask>, TaskFuture<FnTask>) {
        self.register(FnTask::new(task))
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

pub trait TransferAsync<T> {
    fn transfer_async<F: FnOnce(T) + Send + 'static>(self, scheduler: &Scheduler, handler: F);
}

impl<T: Send + 'static> TransferAsync<T> for TransferableFuture<T> {
    fn transfer_async<F: FnOnce(T) + Send + 'static>(self, scheduler: &Scheduler, handler: F) {
        let scheduler = scheduler.clone();
        self.transfer(move |val| scheduler.spawn(move || handler(val)))
    }
}
