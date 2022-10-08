use super::Task;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};

pub enum TaskError<T: Task> {
    Canceled,
    Failed(T::Error),
}

impl<T: Task> Debug for TaskError<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match &self {
            TaskError::Canceled => write!(f, "canceled"),
            TaskError::Failed(_) => write!(f, "failed"),
        }
    }
}

impl<T: Task> Display for TaskError<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self, f)
    }
}

impl<T: Task> Error for TaskError<T> {}

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
