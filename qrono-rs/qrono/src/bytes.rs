use std::fmt::{Debug, Formatter};
use std::ops::Deref;
use std::sync::Arc;

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
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let data: &[u8] = self.data.as_ref();
        data.fmt(f)
    }
}
