use std::cmp::Ordering;

use crate::bytes::Bytes;
use serde::Serialize;
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

#[derive(Debug, Eq, PartialEq, Clone, Default, Serialize)]
pub struct Stats {
    pub enqueue_time: Timestamp,
    pub requeue_time: Timestamp,
    pub dequeue_count: u32,
}

pub type SegmentID = ID;

#[derive(Debug, Eq, PartialEq, Clone, Serialize)]
pub struct Item {
    pub id: ID,
    pub deadline: Timestamp,
    pub stats: Stats,
    pub value: Bytes,
    #[serde(skip)]
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

    pub fn is_pending(&self) -> bool {
        matches!(self, Self::Pending { .. })
    }

    pub fn is_tombstone(&self) -> bool {
        matches!(self, Self::Tombstone { .. })
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

mod serde_impl {
    use super::Timestamp;
    use chrono::{DateTime, SecondsFormat, TimeZone, Utc};
    use serde::de::{Error, Unexpected, Visitor};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::fmt::Formatter;

    struct TimestampVisitor;

    impl<'de> Visitor<'de> for TimestampVisitor {
        type Value = Timestamp;

        fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
            formatter.write_str("RFC3339 timestamp or unix millisecond timestamp")
        }

        fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
        where
            E: Error,
        {
            Ok(Timestamp::from_millis(v))
        }

        fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
        where
            E: Error,
        {
            self.visit_i64(
                v.try_into()
                    .map_err(|_| Error::custom("value out of range"))?,
            )
        }

        fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
        where
            E: Error,
        {
            self.visit_i64(v as i64)
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: Error,
        {
            let date_time = DateTime::parse_from_rfc3339(v)
                .map_err(|_| Error::invalid_value(Unexpected::Str(v), &self))?;

            Ok(Timestamp::from_millis(date_time.timestamp_millis()))
        }
    }

    impl<'de> Deserialize<'de> for Timestamp {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserializer.deserialize_any(TimestampVisitor)
        }
    }

    impl Serialize for Timestamp {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            serializer.serialize_str(
                &Utc.timestamp_millis(self.millis())
                    .to_rfc3339_opts(SecondsFormat::Millis, true),
            )
        }
    }
}

#[cfg(test)]
pub(crate) mod generator {
    use crate::bytes::Bytes;
    use crate::data::{Entry, Item, Stats, Timestamp, ID};
    use rand::distributions::uniform::{
        SampleBorrow, SampleUniform, UniformDuration, UniformSampler,
    };
    use rand::distributions::{Distribution, Standard};
    use rand::seq::SliceRandom;
    use rand::{Rng, SeedableRng};
    use rand_chacha::ChaCha20Rng;
    use std::time::Duration;

    const BASE_TIME: Timestamp = Timestamp::from_millis(1431735440000);
    const ONE_WEEK: Duration = Duration::from_secs(7 * 24 * 60 * 60);

    impl Distribution<Timestamp> for Standard {
        fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> Timestamp {
            BASE_TIME + rng.gen_range(Duration::ZERO..ONE_WEEK)
        }
    }

    pub struct UniformTimestamp {
        base: Timestamp,
        range: UniformDuration,
    }

    impl UniformSampler for UniformTimestamp {
        type X = Timestamp;

        fn new<B1, B2>(low: B1, high: B2) -> Self
        where
            B1: SampleBorrow<Self::X> + Sized,
            B2: SampleBorrow<Self::X> + Sized,
        {
            let spread = *high.borrow() - *low.borrow();
            Self {
                base: *low.borrow(),
                range: UniformDuration::new(Duration::ZERO, spread),
            }
        }

        fn new_inclusive<B1, B2>(low: B1, high: B2) -> Self
        where
            B1: SampleBorrow<Self::X> + Sized,
            B2: SampleBorrow<Self::X> + Sized,
        {
            let spread = *high.borrow() - *low.borrow();
            Self {
                base: *low.borrow(),
                range: UniformDuration::new_inclusive(Duration::ZERO, spread),
            }
        }

        fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> Self::X {
            self.base + self.range.sample(rng)
        }
    }

    impl SampleUniform for Timestamp {
        type Sampler = UniformTimestamp;
    }

    pub struct EntryGenerator {
        rng: ChaCha20Rng,
        last_id: ID,
    }

    impl EntryGenerator {
        pub fn id(&mut self) -> ID {
            let id = self.last_id + 1;
            self.last_id = id;
            id
        }

        pub fn item(&mut self) -> Item {
            let deadline = self.rng.gen();
            let dequeue_count = self.rng.gen_range(0..3);

            let requeue_time = if dequeue_count > 0 {
                self.rng.gen_range(BASE_TIME..deadline)
            } else {
                Timestamp::ZERO
            };

            let enqueue_time = if dequeue_count > 0 {
                self.rng.gen_range(BASE_TIME..requeue_time)
            } else {
                self.rng.gen_range(BASE_TIME..deadline)
            };

            let value_len_base = [0, 32, 256, 1024].choose(&mut self.rng).unwrap();
            let value_len = self.rng.gen_range(
                (value_len_base - (value_len_base / 10))..=(value_len_base + (value_len_base / 10)),
            );

            let mut value = vec![0u8; value_len];
            self.rng.fill(&mut value[..]);

            Item {
                id: self.id(),
                deadline,
                stats: Stats {
                    enqueue_time,
                    requeue_time,
                    dequeue_count,
                },
                value: Bytes::from(value),
                segment_id: 0,
            }
        }

        pub fn pending(&mut self) -> Entry {
            Entry::Pending(self.item())
        }

        pub fn tombstone(&mut self) -> Entry {
            Entry::Tombstone {
                id: self.id(),
                deadline: self.rng.gen(),
                segment_id: 0,
            }
        }

        pub fn entry(&mut self) -> Entry {
            if self.rng.gen() {
                self.pending()
            } else {
                self.tombstone()
            }
        }
    }

    impl Default for EntryGenerator {
        fn default() -> Self {
            let rng = ChaCha20Rng::from_seed([0; 32]);
            EntryGenerator { rng, last_id: 0 }
        }
    }
}
