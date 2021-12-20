use super::{Segment, SegmentReader};
use crate::data::{Entry, Key};
use std::collections::BTreeMap;
use std::io;

use crate::wal::WriteAheadLog;

use crate::segment::{FrozenSegment, Metadata};
use std::ops::Bound;

use std::sync::{Arc, Mutex};
use Bound::{Excluded, Unbounded};

pub struct MemorySegment {
    inner: Arc<Mutex<Inner>>,
    wal: Option<WriteAheadLog>,
}

struct Inner {
    entries: BTreeMap<Key, Entry>,
    size: usize,
    tombstone_count: u64,
    // The latest tombstone (by ID and Deadline) in the segment.
    // Used by readers to skip tombstones when `next()` is called.
    last_tombstone: Key,
}

impl Inner {
    fn next_after(&self, key: Key) -> Option<(&Key, &Entry)> {
        self.entries.range((Excluded(key), Unbounded)).next()
    }

    fn add(&mut self, entry: Entry) {
        if let Entry::Tombstone { id, deadline, .. } = entry {
            if let Some(pair) = self.entries.remove(&Key::Pending { id, deadline }) {
                // Tombstone cancels out existing pending item.
                self.size -= cost_estimate(&pair);
                return;
            }
            let key = entry.key();
            if key > self.last_tombstone {
                self.last_tombstone = key;
            }
            self.tombstone_count += 1;
        }

        self.size += cost_estimate(&entry);
        self.entries.insert(entry.key(), entry);
    }

    fn open_reader(inner: &Arc<Mutex<Self>>, pos: Key) -> io::Result<MemorySegmentReader> {
        Ok(MemorySegmentReader {
            inner: Arc::clone(inner),
            last: pos,
            pending_only: false,
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

impl MemorySegment {
    pub fn new(wal: Option<WriteAheadLog>) -> MemorySegment {
        MemorySegment {
            inner: Arc::new(Mutex::new(Inner {
                entries: BTreeMap::new(),
                size: 0,
                tombstone_count: 0,
                last_tombstone: Key::ZERO,
            })),
            wal,
        }
    }

    pub fn from_reader<R: SegmentReader>(mut src: R) -> io::Result<Self> {
        let mut segment = MemorySegment::new(None);
        while let Some(entry) = src.next()? {
            segment.add(entry)?;
        }
        Ok(segment)
    }

    pub fn add(&mut self, entry: Entry) -> io::Result<()> {
        self.add_all(vec![entry])
    }

    pub fn add_all(&mut self, entries: Vec<Entry>) -> io::Result<()> {
        if let Some(wal) = &mut self.wal {
            wal.append(&entries)?;
        }
        let mut inner = self.inner.lock().unwrap();
        for entry in entries {
            inner.add(entry);
        }
        Ok(())
    }

    pub fn size(&self) -> usize {
        self.inner.lock().unwrap().size
    }

    pub fn open_pending_reader(&self, pos: Key) -> MemorySegmentReader {
        MemorySegmentReader {
            inner: Arc::clone(&self.inner),
            last: pos,
            pending_only: true,
        }
    }

    pub fn freeze(self) -> (FrozenMemorySegment, Option<WriteAheadLog>) {
        (FrozenMemorySegment { inner: self.inner }, self.wal)
    }
}

impl Segment for MemorySegment {
    type R = MemorySegmentReader;

    fn open_reader(&self, pos: Key) -> io::Result<Self::R> {
        Inner::open_reader(&self.inner, pos)
    }

    fn metadata(&self) -> Metadata {
        self.inner.lock().unwrap().metadata()
    }
}

#[derive(Clone)]
pub struct FrozenMemorySegment {
    inner: Arc<Mutex<Inner>>,
}

impl FrozenMemorySegment {
    pub fn to_frozen_segment(&self) -> FrozenSegment {
        FrozenSegment::from_sorted_map(&self.inner.lock().unwrap().entries)
    }
}

impl Segment for FrozenMemorySegment {
    type R = MemorySegmentReader;

    fn open_reader(&self, pos: Key) -> io::Result<Self::R> {
        Inner::open_reader(&self.inner, pos)
    }

    fn metadata(&self) -> Metadata {
        self.inner.lock().unwrap().metadata()
    }
}

pub struct MemorySegmentReader {
    inner: Arc<Mutex<Inner>>,
    last: Key,
    pending_only: bool,
}

impl MemorySegmentReader {
    fn pos(&self, inner: &Inner) -> Key {
        if self.pending_only {
            self.last.max(inner.last_tombstone)
        } else {
            self.last
        }
    }
}

impl SegmentReader for MemorySegmentReader {
    fn next(&mut self) -> io::Result<Option<Entry>> {
        let inner = self.inner.lock().unwrap();
        let pos = self.pos(&inner);
        match inner.next_after(pos) {
            None => Ok(None),
            Some((key, entry)) => {
                self.last = *key;
                Ok(Some(entry.clone()))
            }
        }
    }

    fn peek(&mut self) -> io::Result<Option<Entry>> {
        let inner = self.inner.lock().unwrap();
        let pos = self.pos(&inner);
        match inner.next_after(pos) {
            None => Ok(None),
            Some((_, entry)) => Ok(Some(entry.clone())),
        }
    }

    fn peek_key(&self) -> Option<Key> {
        let inner = self.inner.lock().unwrap();
        let pos = self.pos(&inner);
        inner.next_after(pos).map(|(key, _)| *key)
    }
}

fn cost_estimate(entry: &Entry) -> usize {
    // This estimate is empirically derived by measuring RSS
    // with various value sizes and number of entries.
    const ENTRY_OVERHEAD: usize = 256;

    let value_size = match entry {
        Entry::Pending(item) => item.value.len(),
        Entry::Tombstone { .. } => 0,
    };

    value_size + ENTRY_OVERHEAD
}

#[cfg(test)]
mod tests {
    use super::super::{Segment, SegmentReader};
    use super::MemorySegment;
    use crate::data::{Entry, Key, Timestamp};
    use crate::segment::mem::cost_estimate;

    #[test]
    fn test() {
        let mut segment = MemorySegment::new(None);
        segment
            .add(Entry::Tombstone {
                id: 0,
                deadline: Timestamp::from_millis(99),
                segment_id: 0,
            })
            .unwrap();
        let mut reader = segment.open_reader(Key::ZERO).unwrap();
        println!("{:?}", reader.next().unwrap());
        segment
            .add(Entry::Tombstone {
                id: 0,
                deadline: Timestamp::from_millis(100),
                segment_id: 0,
            })
            .unwrap();
        println!("{:?}", reader.next().unwrap());
        println!("{:?}", reader.next().unwrap());
        segment
            .add(Entry::Tombstone {
                id: 0,
                deadline: Timestamp::from_millis(101),
                segment_id: 0,
            })
            .unwrap();
        println!("{:?}", reader.next().unwrap());
        println!("{:?}", reader.next().unwrap());
    }

    #[test]
    fn sizes() {
        dbg!(cost_estimate(&Entry::Tombstone {
            id: 0,
            deadline: Timestamp::now(),
            segment_id: 0,
        }));
    }
}
