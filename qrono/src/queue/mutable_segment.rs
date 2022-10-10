use crate::bytes::Bytes;
use crate::data::{Entry, Item, Key, SegmentID, Stats, Timestamp, ID};
use crate::queue::filenames::QueueFile;
use crate::segment::{
    FrozenMemorySegment, MemorySegment, MemorySegmentReader, Metadata, Segment, SegmentReader,
};
use crate::wal::WriteAheadLog;
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::{io, mem};

pub(super) struct MutableSegment {
    directory: PathBuf,
    wal_sync_period: Option<Duration>,
    active: Active,
    tx: Vec<Entry>,
}

impl MutableSegment {
    pub(super) fn open<P: AsRef<Path>>(
        directory: P,
        next_segment_id: SegmentID,
        wal_sync_period: Option<Duration>,
    ) -> Self {
        let directory = directory.as_ref().to_path_buf();
        let active = Active::new(&directory, next_segment_id, Key::ZERO, wal_sync_period);
        let tx = Vec::new();
        Self {
            directory,
            wal_sync_period,
            active,
            tx,
        }
    }

    pub(super) fn new_transaction(&mut self) -> Transaction {
        self.tx.clear();
        Transaction {
            segment_id: self.active.id,
            entries: mem::take(&mut self.tx),
        }
    }

    pub(super) fn commit(&mut self, mut tx: Transaction) -> io::Result<()> {
        if tx.segment_id != self.active.id {
            let actual = tx.segment_id;
            let expected = self.active.id;
            panic!(
                "Transaction has stale SegmentID ({} != {})",
                actual, expected
            );
        }

        self.active.segment.add_all(tx.entries.drain(..))?;
        self.tx = tx.entries;
        Ok(())
    }

    pub(super) fn size(&self) -> usize {
        self.active.segment.size()
    }

    pub(super) fn rotate(&mut self, pos: Key) -> RotatedSegment {
        let next_active = Active::new(
            &self.directory,
            self.active.id + 1,
            pos,
            self.wal_sync_period,
        );
        let rotated = mem::replace(&mut self.active, next_active);
        let id = rotated.id;
        let (segment, wal) = rotated.segment.freeze();
        let wal = wal.expect("MemorySegment was created with a WAL");
        RotatedSegment { id, segment, wal }
    }

    pub(super) fn metadata(&self) -> Metadata {
        self.active.segment.metadata()
    }
}

impl SegmentReader for MutableSegment {
    fn next(&mut self) -> io::Result<Option<Entry>> {
        self.active.reader.next()
    }

    fn peek(&mut self) -> io::Result<Option<Entry>> {
        self.active.reader.peek()
    }

    fn peek_key(&self) -> Option<Key> {
        self.active.reader.peek_key()
    }
}

pub(super) struct Transaction {
    segment_id: SegmentID,
    entries: Vec<Entry>,
}

impl Transaction {
    pub(super) fn add_pending(&mut self, id: ID, deadline: Timestamp, stats: Stats, value: Bytes) {
        let segment_id = self.segment_id;
        self.entries.push(Entry::Pending(Item {
            id,
            deadline,
            stats,
            value,
            segment_id,
        }))
    }

    pub(super) fn add_tombstone(&mut self, id: ID, deadline: Timestamp, segment_id: SegmentID) {
        self.entries.push(Entry::Tombstone {
            id,
            deadline,
            segment_id,
        })
    }
}

struct Active {
    id: SegmentID,
    segment: MemorySegment,
    reader: MemorySegmentReader,
}

impl Active {
    fn new(directory: &Path, id: SegmentID, pos: Key, wal_sync_period: Option<Duration>) -> Self {
        let path = QueueFile::WriteAheadLog(id).to_path(directory);
        let wal = WriteAheadLog::new(path, wal_sync_period);
        let segment = MemorySegment::new(Some(wal));
        let reader = segment.open_pending_reader(pos);

        Self {
            id,
            segment,
            reader,
        }
    }
}

pub(super) struct RotatedSegment {
    pub(super) id: SegmentID,
    pub(super) segment: FrozenMemorySegment,
    pub(super) wal: WriteAheadLog,
}
