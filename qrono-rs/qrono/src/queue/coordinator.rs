use crate::data::{Entry, Key};
use crate::queue::filenames::{SegmentKey, SequenceID};
use crate::queue::segment_set::SegmentSet;
use crate::queue::{segment_set, Shared};

use log::debug;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet};

use crate::segment::{FrozenMemorySegment, ImmutableSegment, SegmentReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use std::vec::IntoIter;
use std::{fs, io};

pub(super) struct SegmentCoordinator {
    directory: PathBuf,
    next_seq_id: SequenceID,
    segments: SegmentSet,
    shared: Arc<Mutex<Shared>>,
}

impl SegmentCoordinator {
    pub(super) fn open<P: AsRef<Path>>(
        directory: P,
        shared: Arc<Mutex<Shared>>,
    ) -> io::Result<SegmentCoordinator> {
        let directory = directory.as_ref().to_path_buf();

        let mut tx = Transaction::new();
        for entry in directory.read_dir()? {
            let path = entry?.path();
            if SegmentKey::from_path(&path).is_some() {
                tx.promote(ImmutableSegment::open(path)?);
            }
        }

        let segments = SegmentSet::new();
        let mut coordinator = SegmentCoordinator {
            directory,
            next_seq_id: 1,
            segments,
            shared,
        };

        coordinator.commit(tx)?;
        coordinator.remove_exhausted()?;

        Ok(coordinator)
    }

    fn next_seq_id(&mut self) -> SequenceID {
        let id = self.next_seq_id;
        self.next_seq_id += 1;
        id
    }

    pub(super) fn commit(&mut self, mut tx: Transaction) -> io::Result<()> {
        let mut segments_added = Vec::new();
        let mut ranges_removed = Vec::new();

        // Process pending segments first to ensure ranges are created before
        // we add any tombstone segments.
        let promoted = tx.promoted_pending.into_iter().chain(tx.promoted_tombstone);
        for mut segment in promoted {
            let key = match SegmentKey::from_path(segment.path()).expect("invalid path") {
                SegmentKey::Tombstone { start, end, seq_id } if seq_id == 0 => {
                    SegmentKey::tombstone(start, end, self.next_seq_id())
                }
                key => key,
            };

            segment.rename(key.to_path(&self.directory))?;
            self.next_seq_id = self.next_seq_id.max(key.seq_id() + 1);

            match self.segments.add(key, segment.clone()) {
                Ok(ranges) => {
                    ranges_removed.extend(ranges);
                    segments_added.push((key, segment));
                }
                Err(err) => {
                    // Segment has been superseded
                    debug!("Deleting superseded segment {:?}", err.segment.path());
                    fs::remove_file(err.segment.path())?;
                }
            }
        }

        for range in ranges_removed.iter() {
            tx.removed.extend(range.keys());
        }

        let mut shared = self.shared.lock();
        let start = Instant::now();
        let pos = shared.last;
        let num_removed = tx.removed.len();
        let num_added = segments_added.len();
        let removed = shared.immutable.remove(&tx.removed)?;
        for (key, segment) in segments_added {
            shared.immutable.add(key, segment, pos)?;
        }
        drop(shared);

        debug!(
            "BLOCKING: Replaced immutable segments (+{}/-{}) in {:?}",
            num_added,
            num_removed,
            start.elapsed(),
        );

        // "removed" may be expensive to drop due to the presence of large in-memory segments
        // so we make sure to defer the drop until after releasing the shared lock.
        drop(removed);

        for range in ranges_removed {
            for path in range.paths() {
                debug!("Removing {:?}", path);
                fs::remove_file(path)?;
            }
        }

        Ok(())
    }

    pub(super) fn split(
        &self,
        input: &FrozenMemorySegment,
    ) -> Vec<(SegmentKey, impl SegmentReader)> {
        let entries = input.entries();
        let mut keys = HashMap::new();
        let mut key_counts: HashMap<SegmentKey, usize> = HashMap::new();

        for entry in entries.iter() {
            let pending = matches!(entry, Entry::Pending(_));
            let id = entry.segment_id();
            let key = keys.entry((pending, id)).or_insert_with(|| {
                let range = self
                    .segments
                    .get_key(id)
                    .map(|key| key.range())
                    .unwrap_or_else(|| id..=id);

                let start = *range.start();
                let end = *range.end();

                if pending {
                    SegmentKey::pending(start, end, 0)
                } else {
                    SegmentKey::tombstone(start, end, 0)
                }
            });

            *key_counts.entry(*key).or_default() += 1;
        }

        let mut key_to_entries = key_counts
            .into_iter()
            .map(|(key, count)| (key, Vec::with_capacity(count)))
            .collect::<HashMap<_, _>>();

        for entry in entries {
            let pending = matches!(entry, Entry::Pending(_));
            let id = entry.segment_id();
            let key = keys.get(&(pending, id)).unwrap();
            key_to_entries.get_mut(key).unwrap().push(entry);
        }

        key_to_entries
            .into_iter()
            .map(|(key, entries)| (key, FastReader::new(entries)))
            .collect()
    }

    pub(super) fn ranges(&self) -> impl Iterator<Item = &segment_set::Range> + '_ {
        self.segments.iter()
    }

    pub(super) fn remove_exhausted(&mut self) -> io::Result<()> {
        for range in self.segments.remove_exhausted() {
            debug!("Deleting exhausted range {:?}", range);

            let keys = range.keys().into_iter().collect::<HashSet<_>>();
            let mut shared = self.shared.lock();
            let start = Instant::now();
            let removed = shared.immutable.remove(&keys)?;
            drop(shared);
            // "removed" may be expensive to drop due to the presence of large in-memory segments
            // so we make sure to defer the drop until after releasing the shared lock.
            drop(removed);

            debug!(
                "BLOCKING: Dropped expired immutable segments in {:?}",
                start.elapsed()
            );

            for path in range.paths() {
                fs::remove_file(path)?;
            }
        }

        Ok(())
    }
}

pub(super) struct Transaction {
    promoted_pending: Vec<ImmutableSegment>,
    promoted_tombstone: Vec<ImmutableSegment>,
    removed: HashSet<SegmentKey>,
}

impl Transaction {
    pub(super) fn new() -> Self {
        Self {
            promoted_pending: vec![],
            promoted_tombstone: vec![],
            removed: HashSet::new(),
        }
    }

    pub(super) fn promote(&mut self, segment: ImmutableSegment) {
        if segment.kind().is_pending() {
            self.promoted_pending.push(segment);
        } else {
            self.promoted_tombstone.push(segment);
        }
    }

    pub(super) fn remove(&mut self, key: SegmentKey) {
        self.removed.insert(key);
    }
}

pub(super) fn tx() -> Transaction {
    Transaction::new()
}

struct FastReader {
    entries: IntoIter<Entry>,
}

impl FastReader {
    fn new(entries: Vec<Entry>) -> Self {
        Self {
            entries: entries.into_iter(),
        }
    }
}

impl SegmentReader for FastReader {
    fn next(&mut self) -> std::io::Result<Option<Entry>> {
        Ok(self.entries.next())
    }

    fn peek(&mut self) -> std::io::Result<Option<Entry>> {
        Ok(self.entries.as_slice().first().cloned())
    }

    fn peek_key(&self) -> Option<Key> {
        self.entries.as_slice().first().map(Entry::key)
    }
}
