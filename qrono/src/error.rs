use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum QronoError {
    NoSuchQueue,
    NoItemReady,
    ItemNotDequeued,
    Internal,
}

impl Error for QronoError {}

impl Display for QronoError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            QronoError::NoSuchQueue => write!(f, "no such queue"),
            QronoError::NoItemReady => write!(f, "no item ready"),
            QronoError::ItemNotDequeued => write!(f, "item not dequeued"),
            QronoError::Internal => write!(f, "internal"),
        }
    }
}
