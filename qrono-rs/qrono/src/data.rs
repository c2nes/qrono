use std::cmp::Ordering;

use crate::bytes::Bytes;
use std::ops::{Add, AddAssign, Sub};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub type ID = u64;

#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq, Default)]
pub struct Timestamp(i64);

impl Timestamp {
    pub const ZERO: Timestamp = Timestamp(0);

    pub fn now() -> Timestamp {
        Timestamp(match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(duration) => duration.as_millis() as i64,
            Err(err) => -(err.duration().as_millis() as i64),
        })
    }

    pub fn as_system_time(&self) -> SystemTime {
        if self.0 < 0 {
            UNIX_EPOCH - Duration::from_millis(-self.0 as u64)
        } else {
            UNIX_EPOCH + Duration::from_millis(self.0 as u64)
        }
    }

    pub const fn from_millis(timestamp: i64) -> Timestamp {
        Timestamp(timestamp)
    }

    pub fn millis(&self) -> i64 {
        self.0
    }
}

impl Add<Duration> for Timestamp {
    type Output = Self;

    fn add(self, rhs: Duration) -> Self::Output {
        Timestamp::from_millis(self.millis() + rhs.as_millis() as i64)
    }
}

impl AddAssign<Duration> for Timestamp {
    fn add_assign(&mut self, rhs: Duration) {
        self.0 += rhs.as_millis() as i64;
    }
}

impl Add<i64> for Timestamp {
    type Output = Self;

    fn add(self, rhs: i64) -> Self::Output {
        Timestamp::from_millis(self.millis() + rhs)
    }
}

impl AddAssign<i64> for Timestamp {
    fn add_assign(&mut self, rhs: i64) {
        self.0 += rhs;
    }
}

impl Sub for Timestamp {
    type Output = Duration;

    fn sub(self, rhs: Self) -> Self::Output {
        let millis = self.0 - rhs.0;
        if millis < 0 {
            panic!("negative duration")
        }
        Duration::from_millis(millis as u64)
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Default)]
pub struct Stats {
    pub enqueue_time: Timestamp,
    pub requeue_time: Timestamp,
    pub dequeue_count: u32,
}

pub type SegmentID = ID;

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Item {
    pub id: ID,
    pub deadline: Timestamp,
    pub stats: Stats,
    pub value: Bytes,
    pub segment_id: SegmentID,
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum Entry {
    Pending(Item),
    Tombstone {
        id: ID,
        deadline: Timestamp,
        segment_id: SegmentID,
    },
}

impl Entry {
    pub fn key(&self) -> Key {
        match self {
            Entry::Pending(Item { id, deadline, .. }) => Key::Pending {
                id: *id,
                deadline: *deadline,
            },
            Entry::Tombstone { id, deadline, .. } => Key::Tombstone {
                id: *id,
                deadline: *deadline,
            },
        }
    }

    pub fn segment_id(&self) -> SegmentID {
        match self {
            Entry::Pending(Item { segment_id, .. }) => *segment_id,
            Entry::Tombstone { segment_id, .. } => *segment_id,
        }
    }

    pub fn item(&self) -> Option<&Item> {
        match self {
            Entry::Pending(item) => Some(item),
            _ => None,
        }
    }
}

impl Ord for Entry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key().cmp(&other.key())
    }
}

impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Key {
    Tombstone { id: ID, deadline: Timestamp },
    Pending { id: ID, deadline: Timestamp },
}

impl Key {
    pub const ZERO: Key = Key::Tombstone {
        id: 0,
        deadline: Timestamp::ZERO,
    };

    pub fn id(&self) -> ID {
        match self {
            Key::Tombstone { id, .. } => *id,
            Key::Pending { id, .. } => *id,
        }
    }

    pub fn deadline(&self) -> Timestamp {
        match self {
            Key::Tombstone { deadline, .. } => *deadline,
            Key::Pending { deadline, .. } => *deadline,
        }
    }

    pub fn mirror(self) -> Key {
        match self {
            Key::Tombstone { id, deadline } => Key::Pending { id, deadline },
            Key::Pending { id, deadline } => Key::Tombstone { id, deadline },
        }
    }

    pub fn inc(self) -> Key {
        match self {
            Key::Tombstone { id, deadline } => Key::Pending { id, deadline },
            Key::Pending { id, deadline } => Key::Tombstone {
                id: id + 1,
                deadline,
            },
        }
    }

    fn ord_key(&self) -> (Timestamp, ID, usize) {
        match self {
            Key::Tombstone { id, deadline } => (*deadline, *id, 0),
            Key::Pending { id, deadline } => (*deadline, *id, 1),
        }
    }
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> Ordering {
        self.ord_key().cmp(&other.ord_key())
    }
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
