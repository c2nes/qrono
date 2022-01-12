use crate::data::{Entry, Key};
use crate::segment::{Metadata, Segment, SegmentReader};
use std::collections::BTreeMap;
use std::io;

use std::sync::Arc;

#[derive(Clone)]
pub struct FrozenSegment {
    entries: Arc<Vec<Entry>>,
    tombstone_count: u64,
}

impl FrozenSegment {
    fn from_reader_with_capacity<R: SegmentReader>(
        mut src: R,
        capacity: usize,
    ) -> io::Result<Self> {
        let mut entries = Vec::with_capacity(capacity);
        let mut tombstone_count = 0;
        while let Some(entry) = src.next()? {
            if let Entry::Tombstone { .. } = &entry {
                tombstone_count += 1;
            }
            entries.push(entry);
        }
        let entries = Arc::new(entries);
        Ok(FrozenSegment {
            entries,
            tombstone_count,
        })
    }

    pub fn from_reader<R: SegmentReader>(src: R) -> io::Result<Self> {
        Self::from_reader_with_capacity(src, 0)
    }

    pub fn from_segment<S: Segment>(src: &S) -> io::Result<Self> {
        let meta = src.metadata();
        Self::from_reader_with_capacity(
            src.open_reader(Key::ZERO)?,
            (meta.tombstone_count + meta.pending_count) as usize,
        )
    }

    pub(crate) fn from_sorted_map(entries: &BTreeMap<Key, Entry>) -> Self {
        let entries = Arc::new(entries.values().cloned().collect::<Vec<_>>());
        let mut tombstone_count = 0;
        for entry in entries.iter() {
            if let Entry::Tombstone { .. } = entry {
                tombstone_count += 1;
            }
        }
        Self {
            entries,
            tombstone_count,
        }
    }
}

impl Segment for FrozenSegment {
    type R = FrozenSegmentReader;

    fn open_reader(&self, pos: Key) -> io::Result<Self::R> {
        let pos = if pos == Key::ZERO {
            0
        } else {
            match self.entries.binary_search_by_key(&pos, |e| e.key()) {
                Ok(n) => n + 1,
                Err(n) => n,
            }
        };

        Ok(FrozenSegmentReader {
            entries: Arc::clone(&self.entries),
            pos,
        })
    }

    fn metadata(&self) -> Metadata {
        let tombstone_count = self.tombstone_count;
        let pending_count = self.entries.len() as u64 - tombstone_count;
        Metadata {
            pending_count,
            tombstone_count,
        }
    }
}

pub struct FrozenSegmentReader {
    entries: Arc<Vec<Entry>>,
    pos: usize,
}

impl FrozenSegmentReader {
    fn peek_inner(&self) -> Option<Entry> {
        self.entries.get(self.pos).cloned()
    }
}

impl SegmentReader for FrozenSegmentReader {
    fn next(&mut self) -> io::Result<Option<Entry>> {
        let res = self.peek_inner();
        if res.is_some() {
            self.pos += 1;
        }
        Ok(res)
    }

    fn peek(&mut self) -> io::Result<Option<Entry>> {
        Ok(self.peek_inner())
    }

    fn peek_key(&self) -> Option<Key> {
        self.peek_inner().map(|e| e.key())
    }
}
