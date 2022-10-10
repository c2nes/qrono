use crate::data::SegmentID;
use crate::queue::compactor::Compactor;
use crate::queue::coordinator::SegmentCoordinator;
use crate::queue::filenames::{QueueFile, SegmentKey};
use crate::queue::mutable_segment::RotatedSegment;
use crate::queue::{coordinator, Shared};
use crate::result::IgnoreErr;
use crate::scheduler::{State, Task, TaskContext, TaskFuture, TaskHandle};
use crate::segment::{FrozenMemorySegment, ImmutableSegment};
use crate::wal::{ReadError, WriteAheadLog};

use log::{debug, info};
use parking_lot::Mutex;
use std::collections::HashSet;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use std::{fs, io};

pub(super) const SEGMENT_FLUSH_THRESHOLD: usize = 128 * 1024 * 1024;

pub(super) struct MemorySegmentWriter {
    directory: PathBuf,
    shared: Arc<Mutex<Shared>>,
    coordinator: Arc<Mutex<SegmentCoordinator>>,

    compactor: TaskHandle<Compactor>,
    compactor_future: TaskFuture<Compactor>,
}

impl MemorySegmentWriter {
    pub(super) fn open<P: AsRef<Path>>(
        directory: P,
        shared: Arc<Mutex<Shared>>,
        coordinator: Arc<Mutex<SegmentCoordinator>>,
        compactor: TaskHandle<Compactor>,
        compactor_future: TaskFuture<Compactor>,
    ) -> io::Result<MemorySegmentWriter> {
        let directory = directory.as_ref().to_path_buf();
        Ok(MemorySegmentWriter {
            directory,
            shared,
            coordinator,
            compactor,
            compactor_future,
        })
    }

    pub(crate) fn flush(&mut self) -> io::Result<()> {
        if let Some(mut segment) = self.rotate() {
            let start = Instant::now();
            segment.wal.sync()?;
            let (temp_dir, mut pending, tombstones) = self.write(segment.id, segment.segment)?;
            segment.wal.delete().unwrap();

            // Promote new segments and remove temporary mem segment
            let mut tx = coordinator::tx();
            tx.remove(SegmentKey::pending(segment.id, segment.id, 0));
            for seg in pending.take().into_iter().chain(tombstones) {
                tx.promote(seg);
            }
            self.coordinator.lock().commit(tx)?;

            fs::remove_dir(temp_dir).unwrap();
            self.compactor.schedule().ignore_err();
            debug!("Flush completed in {:?}", start.elapsed());
        }
        Ok(())
    }

    fn rotate(&mut self) -> Option<RotatedSegment> {
        let start = Instant::now();
        let mut locked = self.shared.lock();
        if locked.force_flush {
            locked.force_flush = false
        } else if locked.mutable.size() < SEGMENT_FLUSH_THRESHOLD {
            return None;
        }

        let pos = locked.last;
        let rotated = locked.mutable.rotate(pos);
        let id = rotated.id;
        let segment = rotated.segment.clone();
        locked
            .immutable
            .add(SegmentKey::pending(id, id, 0), segment, pos)
            .unwrap();
        drop(locked);

        debug!("BLOCKING: Rotated current segment in {:?}", start.elapsed());
        Some(rotated)
    }

    fn write(
        &self,
        id: SegmentID,
        segment: FrozenMemorySegment,
    ) -> io::Result<(PathBuf, Option<ImmutableSegment>, Vec<ImmutableSegment>)> {
        let inputs = self.coordinator.lock().split(&segment);
        let segment_dir = QueueFile::TemporarySegmentDirectory(id).to_path(&self.directory);
        fs::create_dir(&segment_dir)?;

        let mut pending_segment = None;
        let mut tombstone_segments = vec![];

        for (key, reader) in inputs {
            let path = key.to_path(&segment_dir);

            debug!("Writing segment {}", key);
            if key.is_pending() {
                let seg = ImmutableSegment::write_pending(path, reader, id)?;
                assert!(
                    pending_segment.replace(seg).is_none(),
                    "there should be at most one pending segment"
                );
            } else {
                let seg = ImmutableSegment::write_tombstone(path, reader, id)?;
                tombstone_segments.push(seg);
            }
        }
        Ok((segment_dir, pending_segment, tombstone_segments))
    }

    pub(super) fn recover(&self, files: &[QueueFile]) -> io::Result<()> {
        let mut recovered_ids = HashSet::new();
        for file in files {
            match file {
                QueueFile::WriteAheadLog(id) => {
                    let start = Instant::now();
                    let id = *id;

                    // Remove any existing segment directory
                    let segment_dir =
                        QueueFile::TemporarySegmentDirectory(id).to_path(&self.directory);
                    if segment_dir.exists() {
                        fs::remove_dir_all(&segment_dir)?;
                    }

                    let path = file.to_path(&self.directory);
                    let segment = match WriteAheadLog::read(&path) {
                        Ok(segment) => segment,
                        Err(ReadError::IO(err)) => return Err(err),
                        Err(err) => return Err(io::Error::new(ErrorKind::Other, err)),
                    };
                    self.write(id, segment.freeze().0)?;

                    fs::remove_file(&path)?;

                    info!("Recovering WAL {:?} in {:?}", path, start.elapsed());

                    recovered_ids.insert(id);
                }
                QueueFile::TemporarySegmentDirectory(id) => {
                    recovered_ids.insert(*id);
                }
                _ => {}
            }
        }

        for id in recovered_ids {
            let dir = QueueFile::TemporarySegmentDirectory(id).to_path(&self.directory);

            let mut tx = coordinator::tx();
            for segment_entry in dir.read_dir()? {
                let path = segment_entry?.path();
                if SegmentKey::from_path(&path).is_some() {
                    tx.promote(ImmutableSegment::open(path)?);
                }
            }

            self.coordinator.lock().commit(tx)?;
            fs::remove_dir_all(dir)?;
        }

        Ok(())
    }

    pub(super) fn shutdown(self) {
        // Cancel compactor and wait for it to stop
        self.compactor.cancel();
        let _ = self.compactor_future.take();
    }
}

impl Task for MemorySegmentWriter {
    type Value = ();
    type Error = io::Error;

    fn run(&mut self, _ctx: &TaskContext<Self>) -> Result<State<()>, io::Error> {
        self.flush()?;
        Ok(State::Idle)
    }
}
