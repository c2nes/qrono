use crate::data::Key;
use crate::queue::coordinator::{SegmentCoordinator, Transaction};
use crate::queue::filenames::SegmentKey;
use crate::queue::segment_set::Range;
use crate::scheduler::{State, Task, TaskContext};
use crate::segment::{ImmutableSegment, MergedSegmentReader};

use log::debug;
use parking_lot::Mutex;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
use std::{io, result};

pub(super) struct Compactor {
    directory: PathBuf,
    coordinator: Arc<Mutex<SegmentCoordinator>>,
}

impl Compactor {
    pub(super) fn new<P: AsRef<Path>>(
        directory: P,
        coordinator: Arc<Mutex<SegmentCoordinator>>,
    ) -> Self {
        let directory = directory.as_ref().to_path_buf();

        Self {
            directory,
            coordinator,
        }
    }

    fn merge_range_keys(ranges: &[Range]) -> SegmentKey {
        let (start, end, seq_id) = ranges
            .iter()
            .flat_map(|r| r.keys())
            .map(|k| k.unpack())
            .reduce(|l, r| (l.0.min(r.0), l.1.max(r.1), l.2.max(r.2)))
            .unwrap();

        SegmentKey::pending(start, end, seq_id)
    }
}

impl Task for Compactor {
    type Value = ();
    type Error = io::Error;

    fn run(&mut self, _: &TaskContext<Compactor>) -> result::Result<State<()>, io::Error> {
        debug!("Running compactor");

        self.coordinator.lock().remove_exhausted()?;

        // Compaction serves two goals:
        //
        //   1. Limit the total number of files, so we need fewer open file descriptors.
        //   2. Limit the amount of disk space wasted.
        //
        // These goals could be met by simply compacting the entire queue into a single pending
        // segment, but this is also IO intensive since every file must be read, and every pending
        // item rewritten on disk. To balance optimal compaction with IO requirements, we introduce
        // a third goal,
        //
        //   3. Limit the amount of data written during a compaction.
        //
        // For the purpose of calculating an optimum compaction set, we minimize the L2-norm of the
        // following variables,
        //
        //   1. The percent of files remaining after compaction.
        //   2. The percent of pending items remaining.
        //   3. The percent of pending items rewritten.
        //

        /// A compaction is triggered if the number of segment files exceeds this threshold.
        const FILE_COUNT_THRESHOLD: usize = 16;

        /// A compaction is triggered if the occupancy of the queue falls below this threshold.
        ///
        /// The occupancy of the queue is the percent of pending items which have not been
        /// tombstoned.
        const OCCUPANCY_THRESHOLD: f64 = 0.6;

        let ranges = self
            .coordinator
            .lock()
            .ranges()
            .cloned()
            .collect::<Vec<_>>();

        let total_files: usize = ranges.iter().map(|r| r.tombstone_segments.len() + 1).sum();
        let total_pending: u64 = ranges.iter().map(|r| r.metadata.pending_count).sum();
        let total_tombstone: u64 = ranges.iter().map(|r| r.metadata.tombstone_count).sum();
        let occupancy = ((total_pending - total_tombstone) as f64) / (total_pending as f64);

        debug!(
            "Compaction run; occupancy={:.2}, file_count={}",
            occupancy, total_files
        );

        if occupancy >= OCCUPANCY_THRESHOLD && total_files <= FILE_COUNT_THRESHOLD {
            return Ok(State::Idle);
        }

        let start = Instant::now();
        let mut best = None;

        for i in 0..ranges.len() {
            for j in (i + 1)..=ranges.len() {
                let num_files: usize = ranges[i..j]
                    .iter()
                    .map(|r| r.tombstone_segments.len() + 1)
                    .sum();
                let num_pending: u64 = ranges[i..j].iter().map(|r| r.metadata.pending_count).sum();
                let num_tombstone: u64 = ranges[i..j]
                    .iter()
                    .map(|r| r.metadata.tombstone_count)
                    .sum();

                let net_files = total_files - num_files + 1;
                let net_pending = total_pending - num_tombstone;
                let net_tombstone = total_tombstone - num_tombstone;
                let pending_written = num_pending - num_tombstone;
                let net_occupancy = ((net_pending - net_tombstone) as f64) / (net_pending as f64);

                if net_files > FILE_COUNT_THRESHOLD || net_occupancy < OCCUPANCY_THRESHOLD {
                    continue;
                }

                let norm = ((net_files as f64 / total_files as f64).powf(2.0)
                    + (net_pending as f64 / total_pending as f64).powf(2.0)
                    + (pending_written as f64 / total_pending as f64).powf(2.0))
                .sqrt();

                debug!(
                    "Compaction score: {} -> {:.4}",
                    Self::merge_range_keys(&ranges[i..j]),
                    norm
                );

                if match best {
                    None => true,
                    Some((_, _, score)) if norm < score => true,
                    _ => false,
                } {
                    best = Some((i, j, norm));
                }
            }
        }

        if let Some((i, j, _)) = best {
            let inputs = &ranges[i..j];
            let keys = inputs.iter().flat_map(|r| r.keys()).collect::<Vec<_>>();
            let key = Self::merge_range_keys(inputs);

            debug!("Compacting {} files into {}", keys.len(), key);
            for key in keys {
                debug!("  {}", key);
            }

            let mut merged = MergedSegmentReader::new();
            for (i, segment) in inputs.iter().flat_map(|r| r.segments()).enumerate() {
                merged.add(i, segment, Key::ZERO)?;
            }

            let output = key.to_path(&self.directory);
            let segment = ImmutableSegment::write_pending(output, merged, key.start())?;
            debug!("Compacted segment written in {:?}", start.elapsed());
            let mut tx = Transaction::new();
            tx.promote(segment);
            self.coordinator.lock().commit(tx)?;

            debug!("Compaction completed in {:?}", start.elapsed());
            Ok(State::Runnable)
        } else {
            Ok(State::Idle)
        }
    }
}
