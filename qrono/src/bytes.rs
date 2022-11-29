use serde::de::{Error, Unexpected, Visitor};
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use std::alloc::Layout;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::ops::Deref;
use std::sync::Arc;

use crate::alloc;

#[derive(Clone, Eq, PartialEq, Hash)]
pub struct Bytes {
    data: Arc<[u8]>,
}

impl Bytes {
    pub fn empty() -> Bytes {
        vec![].into()
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn size_on_heap(&self) -> usize {
        const ARC_INNER_OVERHEAD: usize = 16;
        let slice: &[u8] = &self.data;
        let size = std::mem::size_of_val(slice) + ARC_INNER_OVERHEAD;
        let align = std::mem::align_of_val(slice);
        alloc::size_of(Layout::from_size_align(size, align).unwrap())
    }
}

impl Deref for Bytes {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl AsRef<[u8]> for Bytes {
    fn as_ref(&self) -> &[u8] {
        self.data.as_ref()
    }
}

impl From<Vec<u8>> for Bytes {
    fn from(data: Vec<u8>) -> Self {
        let data = Arc::from(data.into_boxed_slice());
        Self { data }
    }
}

impl From<String> for Bytes {
    fn from(data: String) -> Self {
        data.into_bytes().into()
    }
}

impl From<&[u8]> for Bytes {
    fn from(data: &[u8]) -> Self {
        data.to_vec().into()
    }
}

impl From<&str> for Bytes {
    fn from(data: &str) -> Self {
        data.to_string().into()
    }
}

impl Default for Bytes {
    fn default() -> Self {
        Self::empty()
    }
}

impl Debug for Bytes {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let data: &[u8] = self.data.as_ref();
        data.fmt(f)
    }
}

struct BytesVisitor;

impl<'de> Visitor<'de> for BytesVisitor {
    type Value = Bytes;

    fn expecting(&self, formatter: &mut Formatter) -> fmt::Result {
        formatter.write_str("base64 encoded byte array")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: Error,
    {
        base64::decode(v)
            .map(Bytes::from)
            .map_err(|_| de::Error::invalid_value(Unexpected::Str(v), &self))
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(Bytes::from(v))
    }

    fn visit_byte_buf<E>(self, v: Vec<u8>) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Ok(Bytes::from(v))
    }
}

impl<'de> Deserialize<'de> for Bytes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(BytesVisitor)
    }
}

impl Serialize for Bytes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&base64::encode(self))
    }
}

#[cfg(test)]
mod tests {
    use crate::alloc;

    use super::Bytes;

    #[test]
    fn test_size_on_heap() {
        for i in 0..10000 {
            let mem_tracker = alloc::track();
            let b = Bytes::from(vec![0u8; i]);
            let allocated = mem_tracker.allocation_change();
            assert_ne!(allocated, 0);
            assert_eq!(allocated, b.size_on_heap() as isize);
        }
    }
}
