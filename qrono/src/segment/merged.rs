use crate::data::{Entry, Key};
use crate::segment::{Metadata, Segment, SegmentReader};
use std::cmp::Ordering;

use rustc_hash::FxHashMap;
use std::collections::{BinaryHeap, HashSet};
use std::hash::{BuildHasher, Hash};
use std::io;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct Token(usize);

struct SegmentWrapper<S, K>(S, K)
where
    S: Segment + Send,
    <S as Segment>::R: Send;

impl<S, K> Segment for SegmentWrapper<S, K>
where
    S: Segment + Send,
    <S as Segment>::R: Send,
    K: Copy + 'static,
{
    type R = ReaderWrapper<K>;

    fn open_reader(&self, pos: Key) -> io::Result<Self::R> {
        match self.0.open_reader(pos) {
            Ok(reader) => Ok(ReaderWrapper::new(reader, self.1)),
            Err(err) => Err(err),
        }
    }

    fn metadata(&self) -> Metadata {
        self.0.metadata()
    }
}

pub struct ReaderWrapper<K> {
    reader: Box<dyn SegmentReader + Send + 'static>,
    peeked_entry: Option<Entry>,
    key: K,
}

impl<K> ReaderWrapper<K> {
    fn new<R: SegmentReader + Send + 'static>(reader: R, key: K) -> ReaderWrapper<K> {
        ReaderWrapper {
            reader: Box::new(reader),
            peeked_entry: None,
            key,
        }
    }
}

impl<K> SegmentReader for ReaderWrapper<K> {
    fn next(&mut self) -> io::Result<Option<Entry>> {
        match self.peeked_entry.take() {
            Some(entry) => Ok(Some(entry)),
            None => self.reader.next(),
        }
    }

    fn peek(&mut self) -> io::Result<Option<Entry>> {
        match &self.peeked_entry {
            Some(entry) => Ok(Some(entry.clone())),
            None => self.reader.peek(),
        }
    }

    fn peek_key(&self) -> Option<Key> {
        match &self.peeked_entry {
            Some(entry) => Some(entry.key()),
            None => self.reader.peek_key(),
        }
    }
}

impl<K> PartialEq<Self> for ReaderWrapper<K> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl<K> Eq for ReaderWrapper<K> {}

impl<K> PartialOrd<Self> for ReaderWrapper<K> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<K> Ord for ReaderWrapper<K> {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max heap, so reverse the ordering so we get a min-heap instead
        self.peek_key()
            .unwrap()
            .cmp(&other.peek_key().unwrap())
            .reverse()
    }
}

#[derive(Default)]
pub struct MergedSegmentReader<K> {
    readers: BinaryHeap<ReaderWrapper<K>>,
    segments: FxHashMap<K, Box<dyn Segment<R = ReaderWrapper<K>> + Send>>,
    head: Option<ReaderWrapper<K>>,
}

impl<K> MergedSegmentReader<K>
where
    K: Eq + Hash + Send + Copy + 'static,
{
    pub fn new() -> MergedSegmentReader<K> {
        MergedSegmentReader {
            readers: Default::default(),
            segments: Default::default(),
            head: None,
        }
    }

    pub fn add<S>(&mut self, key: K, segment: S, pos: Key) -> io::Result<()>
    where
        S: Segment + Send + 'static,
        <S as Segment>::R: Send,
    {
        let mut dirty = false;

        // Remove existing segment with same key if one exists.
        if self.segments.remove(&key).is_some() {
            self.readers = self
                .readers
                .drain()
                .chain(self.head.take())
                .filter(|r| r.key != key)
                .collect();
            dirty = true;
        }

        let segment = SegmentWrapper(segment, key);
        let reader = segment.open_reader(pos)?;
        self.segments.insert(key, Box::new(segment));

        if reader.peek_key().is_some() {
            self.readers.push(reader);
            dirty = true;
        }

        if dirty {
            self.check_head();
            self.advance_to_next_unpaired_entry()?;
        }

        Ok(())
    }

    /// Removes the segments associated with the given keys.
    ///
    /// Returns an opaque representation of the removed segments to allow their dropping
    /// to be deferred as desired.
    pub fn remove<S: BuildHasher>(&mut self, keys: &HashSet<K, S>) -> io::Result<impl Drop> {
        let mut removed = Vec::with_capacity(keys.len());

        for key in keys {
            if let Some(segment) = self.segments.remove(key) {
                removed.push(segment);
            }
        }

        self.readers = self
            .readers
            .drain()
            .chain(self.head.take())
            .filter(|r| !keys.contains(&r.key))
            .collect();

        self.check_head();
        self.advance_to_next_unpaired_entry()?;
        Ok(removed)
    }

    fn check_head(&mut self) {
        match &self.head {
            Some(head) if head.peek_key().is_some() => {
                if let Some(max) = self.readers.peek() {
                    if max > head {
                        self.readers.push(self.head.take().unwrap());
                        self.head = self.readers.pop();
                    }
                }
            }
            _ => self.head = self.readers.pop(),
        }
    }

    fn raw_peek(&mut self) -> io::Result<Option<Entry>> {
        if let Some(head) = &mut self.head {
            head.peek()
        } else {
            Ok(None)
        }
    }

    fn raw_peek_key(&self) -> Option<Key> {
        if let Some(head) = &self.head {
            head.peek_key()
        } else {
            None
        }
    }

    fn raw_next(&mut self) -> io::Result<Option<Entry>> {
        if let Some(head) = &mut self.head {
            let res = head.next();
            self.check_head();
            res
        } else {
            Ok(None)
        }
    }

    // TODO: Check this again!
    fn advance_to_next_unpaired_entry(&mut self) -> io::Result<()> {
        loop {
            if let Some(Key::Tombstone { .. }) = self.raw_peek_key() {
                let head = self.head.as_mut().unwrap();

                // Take the tombstone entry
                let tombstone = head
                    .next()?
                    .expect("peek_key indicated a value was available");

                // Compare `head` with the new top of the heap, using whichever is appropriate
                // to peek the key after the tombstone entry.
                let subsequent = match self.readers.peek() {
                    Some(top) if head.peek_key().is_none() || top > head => top.peek_key(),
                    _ => head.peek_key(),
                };

                if let Some(key) = subsequent {
                    // There is a matching Pending entry following the tombstone. We will
                    // drop both entries, and then restore the appropriate reader as `head'.
                    if tombstone.key() == key.mirror() {
                        self.check_head();
                        drop(tombstone);
                        drop(self.raw_next()?);
                        continue;
                    }
                }

                head.peeked_entry = Some(tombstone);
            }

            return Ok(());
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, &(dyn Segment<R = ReaderWrapper<K>> + Send))> {
        self.segments.iter().map(|(k, v)| (k, v.as_ref()))
    }
}

pub struct Iter<'t, K> {
    entries: slab::Iter<'t, Box<dyn Segment<R = ReaderWrapper<K>> + Send>>,
}

impl<'t, K> Iterator for Iter<'t, K> {
    type Item = &'t (dyn Segment<R = ReaderWrapper<K>> + Send);

    fn next(&mut self) -> Option<Self::Item> {
        self.entries.next().map(|v| v.1.as_ref())
    }
}

impl<K> SegmentReader for MergedSegmentReader<K>
where
    K: Eq + Hash + Send + Copy + 'static,
{
    fn next(&mut self) -> io::Result<Option<Entry>> {
        let res = self.raw_next()?;
        self.advance_to_next_unpaired_entry()?;
        Ok(res)
    }

    fn peek(&mut self) -> io::Result<Option<Entry>> {
        self.raw_peek()
    }

    fn peek_key(&self) -> Option<Key> {
        self.raw_peek_key()
    }
}

#[cfg(test)]
mod tests {
    use crate::bytes::Bytes;
    use crate::data::{Entry, Item, Key, Timestamp, ID};
    use crate::segment::merged::MergedSegmentReader;
    use crate::segment::mock::MockSegmentReader;
    use crate::segment::{MemorySegment, Segment, SegmentReader};
    use std::collections::HashSet;

    use std::time::Instant;

    fn pending(id: ID, millis: i64) -> Entry {
        Entry::Pending(Item {
            id,
            deadline: Timestamp::from_millis(millis),
            stats: Default::default(),
            value: Default::default(),
            segment_id: 0,
        })
    }

    fn tombstone(id: ID, millis: i64) -> Entry {
        Entry::Tombstone {
            id,
            deadline: Timestamp::from_millis(millis),
            segment_id: 0,
        }
    }

    fn segment(entries: Vec<Entry>) -> MemorySegment {
        let mut segment = MemorySegment::new(None);
        for entry in entries {
            segment.add(entry).unwrap();
        }
        segment
    }

    #[test]
    fn test_skip_tombstoned() {
        let mut src1 = MemorySegment::new(None);
        src1.add(pending(1, 1)).unwrap();
        src1.add(pending(2, 1)).unwrap();

        let mut src2 = MemorySegment::new(None);
        src2.add(tombstone(1, 1)).unwrap();

        let mut merged = MergedSegmentReader::new();
        merged.add(1, src1, Key::ZERO).unwrap();
        merged.add(2, src2, Key::ZERO).unwrap();
        assert_eq!(Some(pending(2, 1)), merged.next().unwrap())
    }

    #[test]
    fn test_add_tombstone_segment() {
        let src1 = segment(vec![pending(1, 1), pending(2, 1), pending(3, 1)]);
        let mut merged = MergedSegmentReader::new();
        merged.add(1, src1, Key::ZERO).unwrap();

        assert_eq!(Some(pending(1, 1)), merged.next().unwrap());
        assert_eq!(Some(pending(2, 1)), merged.next().unwrap());

        let src2 = segment(vec![tombstone(1, 1), tombstone(2, 1), pending(1, 5)]);
        merged.add(2, src2, pending(2, 1).key()).unwrap();

        let src2 = segment(vec![tombstone(1, 1), tombstone(2, 1), pending(1, 5)]);
        merged.remove(&HashSet::from([2])).unwrap();
        merged.add(2, src2, pending(2, 1).key()).unwrap();

        assert_eq!(Some(pending(3, 1)), merged.next().unwrap());
        assert_eq!(Some(pending(1, 5)), merged.next().unwrap());
    }

    #[test]
    fn test_merge_to_empty() {
        let mut src1 = MemorySegment::new(None);
        for i in 1..10 {
            src1.add(pending(i, 1)).unwrap();
        }

        let src2 = MemorySegment::new(None);
        for i in 1..10 {
            src1.add(tombstone(i, 1)).unwrap();
        }

        let mut merged = MergedSegmentReader::new();
        merged.add(1, src1, Key::ZERO).unwrap();
        merged.add(2, src2, Key::ZERO).unwrap();
        assert_eq!(None, merged.next().unwrap())
    }

    #[test]
    fn test() {
        fn generate<F>(i: usize, n: usize, id: F) -> Option<Entry>
        where
            F: Fn(usize) -> usize,
        {
            if i < n {
                Some(Entry::Pending(Item {
                    id: id(i) as ID,
                    deadline: Timestamp::from_millis(id(i) as i64 + 100000),
                    stats: Default::default(),
                    value: Bytes::from("AAAAAAAA"),
                    segment_id: 0,
                }))
            } else {
                None
            }
        }

        {
            let n = 10_000;
            let m = 5;
            let mut merged = MergedSegmentReader::new();
            for j in 0..m {
                let src = MockSegmentReader::new(|i| generate(i, n, |i| i * m + j));
                merged
                    .add(j, MemorySegment::from_reader(src).unwrap(), Key::ZERO)
                    .unwrap();
            }

            let start = Instant::now();
            for i in 0..(n * m) {
                match merged.next() {
                    Ok(Some(Entry::Pending(Item { id, .. }))) => assert_eq!(id, i as ID),
                    err => panic!("Failed at {}, {:?}", i, err),
                }
            }
            match merged.next() {
                Ok(None) => {}
                err => panic!("Unexpected: {:?}", err),
            }
            let elapsed = dbg!(start.elapsed());
            let rate = ((n * m) as f64) / elapsed.as_secs_f64();
            dbg!(rate);
        }

        {
            let n = 10_000;
            let m = 5;
            let mut merged = MergedSegmentReader::new();
            for j in 0..m {
                let src = MockSegmentReader::new(|i| generate(i, n, |i| i + n * j));
                merged
                    .add(j, MemorySegment::from_reader(src).unwrap(), Key::ZERO)
                    .unwrap();
            }

            let start = Instant::now();
            for i in 0..(n * m) {
                match merged.next() {
                    Ok(Some(Entry::Pending(Item { id, .. }))) => assert_eq!(id, i as ID),
                    err => panic!("Failed at {}, {:?}", i, err),
                }
            }
            match merged.next() {
                Ok(None) => {}
                err => panic!("Unexpected: {:?}", err),
            }
            let elapsed = dbg!(start.elapsed());
            let rate = ((n * m) as f64) / elapsed.as_secs_f64();
            dbg!(rate);
        }

        {
            let n = 100_000;
            let src = MockSegmentReader::new(|i| generate(i, n, |i| i));
            let segment = MemorySegment::from_reader(src).unwrap();
            let mut merged = segment.open_reader(Key::ZERO).unwrap();
            let start = Instant::now();
            for i in 0..n {
                match merged.next() {
                    Ok(Some(Entry::Pending(Item { id, .. }))) => assert_eq!(id, i as ID),
                    err => panic!("Failed at {}, {:?}", i, err),
                }
            }
            match merged.next() {
                Ok(None) => {}
                err => panic!("Unexpected: {:?}", err),
            }
            let elapsed = dbg!(start.elapsed());
            let rate = (n as f64) / elapsed.as_secs_f64();
            dbg!(rate);
        }
    }
}
