use std::fmt::Debug;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::bytes::Bytes;
use crate::data::{Item, Timestamp, ID};

#[derive(Deserialize, Debug)]
pub enum ValueReq {
    #[serde(rename = "value")]
    Bytes(Bytes),

    #[serde(rename = "string")]
    String(String),
}

impl From<&str> for ValueReq {
    fn from(s: &str) -> Self {
        Self::String(s.to_string())
    }
}

impl From<&[u8]> for ValueReq {
    fn from(s: &[u8]) -> Self {
        Self::Bytes(s.into())
    }
}

impl From<ValueReq> for Bytes {
    fn from(value: ValueReq) -> Self {
        match value {
            ValueReq::Bytes(bytes) => bytes,
            ValueReq::String(string) => string.into(),
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum DeadlineReq {
    Now,
    Relative(Duration),
    Absolute(Timestamp),
}

impl DeadlineReq {
    pub fn resolve(self, now: Timestamp) -> Timestamp {
        match self {
            DeadlineReq::Now => now,
            DeadlineReq::Relative(delay) => now + delay,
            DeadlineReq::Absolute(timestamp) => timestamp,
        }
    }
}

impl Default for DeadlineReq {
    fn default() -> Self {
        Self::Now
    }
}

#[derive(Debug, Deserialize)]
pub struct EnqueueReq {
    #[serde(flatten)]
    pub value: ValueReq,

    #[serde(default)]
    pub deadline: DeadlineReq,
}

#[derive(Debug, Serialize)]
pub struct EnqueueResp {
    pub id: ID,
    pub deadline: Timestamp,
}

#[derive(Debug, Deserialize)]
pub struct DequeueReq {
    #[serde(with = "serde_impl::Timeout", default)]
    pub timeout: Duration,

    #[serde(default = "DequeueReq::default_count")]
    pub count: u64,
}

impl DequeueReq {
    pub const fn default_count() -> u64 {
        1
    }
}

pub type DequeueResp = Vec<Item>;

#[derive(Debug, Deserialize)]
pub struct RequeueReq {
    pub id: IdPattern,
    pub deadline: DeadlineReq,
}

#[derive(Debug, Serialize)]
pub struct RequeueResp {
    pub deadline: Timestamp,
}

#[derive(Debug, Deserialize)]
pub struct ReleaseReq {
    #[serde(flatten)]
    pub id: IdPattern,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum IdPattern {
    #[serde(with = "serde_impl::IdPatternAny")]
    Any,
    Id(ID),
}

pub type ReleaseResp = ();

#[derive(Debug)]
pub struct PeekReq;

pub type PeekResp = Item;

#[derive(Debug)]
pub struct InfoReq;

#[derive(Debug, Clone, Serialize)]
pub struct InfoResp {
    pub pending: u64,
    pub dequeued: u64,
}

#[derive(Debug)]
pub struct DeleteReq;

pub type DeleteResp = ();

#[derive(Debug)]
pub struct CompactReq;

pub type CompactResp = ();

mod serde_impl {
    use std::fmt::Formatter;
    use std::time::Duration;

    use chrono::DateTime;
    use serde::de::{Error, Unexpected, Visitor};
    use serde::{de, Deserialize, Deserializer};

    use crate::data::Timestamp;
    use crate::ops::DeadlineReq;

    fn duration_from_str<E: de::Error>(delay: &str) -> Result<Duration, E> {
        for (suffix, scale) in [("ms", 1e-3), ("s", 1.0), ("m", 60.0), ("h", 3600.0)] {
            if let Some(base) = delay.strip_suffix(suffix) {
                let base = base
                    .parse::<u64>()
                    .map_err(|_| E::invalid_value(Unexpected::Str(delay), &"duration"))?;

                return Ok(Duration::from_secs_f64((base as f64) * scale));
            }
        }

        Err(E::invalid_value(Unexpected::Str(delay), &"duration"))
    }

    pub(super) struct Timeout;

    impl Timeout {
        pub(super) fn deserialize<'de, D: Deserializer<'de>>(
            deserializer: D,
        ) -> Result<Duration, D::Error> {
            struct DurationVisitor;

            impl<'de> Visitor<'de> for DurationVisitor {
                type Value = Duration;

                fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                    formatter.write_str("timeout")
                }

                fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
                where
                    E: Error,
                {
                    Ok(Duration::from_millis(v as u64))
                }

                fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
                where
                    E: Error,
                {
                    Ok(Duration::from_millis(v))
                }

                fn visit_str<E>(self, delay: &str) -> Result<Self::Value, E>
                where
                    E: Error,
                {
                    duration_from_str(delay)
                }
            }

            deserializer.deserialize_str(DurationVisitor)
        }
    }

    struct DeadlineReqVisitor;

    impl<'de> Visitor<'de> for DeadlineReqVisitor {
        type Value = DeadlineReq;

        fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
            formatter.write_str("deadline or delay")
        }

        fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
        where
            E: Error,
        {
            Ok(DeadlineReq::Absolute(Timestamp::from_millis(v)))
        }

        fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
        where
            E: Error,
        {
            self.visit_i64(v as i64)
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
            if let Some(delay) = v.strip_prefix('+') {
                Ok(DeadlineReq::Relative(duration_from_str(delay)?))
            } else {
                let date_time = DateTime::parse_from_rfc3339(v)
                    .map_err(|_| de::Error::invalid_value(Unexpected::Str(v), &self))?;
                let timestamp = Timestamp::from_millis(date_time.timestamp_millis());
                Ok(DeadlineReq::Absolute(timestamp))
            }
        }
    }

    impl<'de> Deserialize<'de> for DeadlineReq {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            deserializer.deserialize_str(DeadlineReqVisitor)
        }
    }

    pub(super) struct IdPatternAny;

    impl IdPatternAny {
        pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<(), D::Error>
        where
            D: Deserializer<'de>,
        {
            struct AnyVisitor;

            impl<'de> Visitor<'de> for AnyVisitor {
                type Value = ();

                fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                    formatter.write_str("\"any\" to be set to true")
                }

                fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
                where
                    E: Error,
                {
                    if v {
                        Ok(())
                    } else {
                        Err(E::invalid_value(Unexpected::Bool(v), &self))
                    }
                }

                fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
                where
                    E: Error,
                {
                    if v == "true" || v.is_empty() {
                        Ok(())
                    } else {
                        Err(E::invalid_value(Unexpected::Str(v), &self))
                    }
                }
            }

            deserializer.deserialize_any(AnyVisitor)
        }
    }
}
