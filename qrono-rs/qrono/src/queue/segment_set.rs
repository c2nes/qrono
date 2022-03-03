use crate::data::SegmentID;
use crate::queue::filenames::SegmentKey;
use crate::segment::{ImmutableSegment, Metadata, Segment};

use std::collections::{BTreeMap, HashSet};
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::iter::once;
use std::path::Path;

/// Error returned when a segment can not be added to a SegmentSet because
/// another segment already in the set supersedes it.
pub(super) struct SegmentSupersededError {
    pub(super) key: SegmentKey,
    pub(super) segment: ImmutableSegment,
}

impl Display for SegmentSupersededError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} segment {} has been superseded",
            self.segment.kind(),
            self.key
        )
    }
}

impl Debug for SegmentSupersededError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        <Self as Display>::fmt(self, f)
    }
}

impl Error for SegmentSupersededError {}

pub(super) struct SegmentSet {
    // Indexed by SegmentKey.start
    ranges: BTreeMap<SegmentID, Range>,
    exhausted: HashSet<SegmentID>,
}

impl SegmentSet {
    pub(super) fn new() -> Self {
        Self {
            ranges: Default::default(),
            exhausted: Default::default(),
        }
    }

    /// Adds the given segment to the set, returning any segment ranges which are afterwards
    /// no longer needed. Ranges returned have either been superseded (indicating that a more
    /// recently compacted segment contains the equivalent entries) or exhausted (indicating that
    /// all pending entries in the range have been tombstoned).
    ///
    /// If an attempt is made to add a segment which is itself superseded by an existing segment
    /// within the set, then a `SegmentSupersededError` will be returned.
    pub(super) fn add(
        &mut self,
        key: SegmentKey,
        segment: ImmutableSegment,
    ) -> Result<Vec<Range>, SegmentSupersededError> {
        if segment.kind().is_tombstone() {
            return match self.get_mut(key.start()) {
                Some(range) => {
                    if range.includes(&key) {
                        range.add_tombstone((key, segment));
                        if range.is_exhausted() {
                            let start = range.key.start();
                            self.exhausted.insert(start);
                        }
                        Ok(vec![])
                    } else {
                        Err(SegmentSupersededError { key, segment })
                    }
                }
                None => panic!("No matching Pending segment found for Tombstone segment"),
            };
        }

        assert!(
            segment.metadata().pending_count > 0,
            "Pending segments must be non-empty"
        );

        if let Some(range) = self.get_mut(key.start()) {
            if range.key.start() < key.start() {
                // If the range that was found starts before ours then our range has been
                // superseded by a larger range, *or* something has gone truly wrong and we
                // partially overlap with another range. This should never happen.
                assert!(
                    key.end() <= range.key.end(),
                    "Segment partially overlaps existing segment"
                );
                return Err(SegmentSupersededError { key, segment });
            }

            if key.end() == range.key.end() && key.seq_id() <= range.key.seq_id() {
                // Ranges are the same, but the existing segment has a later sequence ID
                return Err(SegmentSupersededError { key, segment });
            }
        };

        let superseded_keys = self
            .ranges
            .range(key.range())
            .map(|(k, _)| *k)
            .collect::<Vec<_>>();

        let mut superseded_ranges = superseded_keys
            .iter()
            .map(|k| self.ranges.remove(k).unwrap())
            .collect::<Vec<_>>();

        let mut range = Range::new(key, segment);
        for superseded in superseded_ranges.iter_mut() {
            self.exhausted.remove(&superseded.key.start());

            let mut i = 0;
            while i < superseded.tombstone_segments.len() {
                let (key, _) = &superseded.tombstone_segments[i];
                if range.includes(key) {
                    let pair = superseded.tombstone_segments.swap_remove(i);
                    range.add_tombstone(pair);
                } else {
                    i += 1;
                }
            }
        }

        if range.is_exhausted() {
            self.exhausted.insert(range.key.start());
        }

        self.ranges.insert(range.key.start(), range);

        Ok(superseded_ranges)
    }

    fn get(&self, id: SegmentID) -> Option<&Range> {
        match self.ranges.range(..=id).last() {
            Some((_, range)) if range.key.range().contains(&id) => Some(range),
            _ => None,
        }
    }

    fn get_mut(&mut self, id: SegmentID) -> Option<&mut Range> {
        match self.ranges.range_mut(..=id).last() {
            Some((_, range)) if range.key.range().contains(&id) => Some(range),
            _ => None,
        }
    }

    /// Returns the pending segment key within this set that contains the given ID.
    pub(super) fn get_key(&self, id: SegmentID) -> Option<SegmentKey> {
        self.get(id).map(|r| r.key)
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = &Range> + '_ {
        self.ranges.values()
    }

    pub(super) fn remove_exhausted(&mut self) -> Vec<Range> {
        self.exhausted
            .drain()
            .map(|k| self.ranges.remove(&k).unwrap())
            .collect()
    }
}

#[derive(Clone)]
pub(super) struct Range {
    /// Key for the pending segment which defines this range.
    pub(super) key: SegmentKey,

    /// The pending segment for this range.
    pub(super) pending_segment: ImmutableSegment,

    /// Tombstone segments containing tombstones for this range's pending segment.
    pub(super) tombstone_segments: Vec<(SegmentKey, ImmutableSegment)>,

    /// Combined metadata for the pending and tombstone segments.
    pub(super) metadata: Metadata,
}

impl Range {
    fn new(key: SegmentKey, pending_segment: ImmutableSegment) -> Self {
        let metadata = pending_segment.metadata();
        let tombstone_segments = vec![];
        Self {
            key,
            pending_segment,
            tombstone_segments,
            metadata,
        }
    }

    fn includes(&self, key: &SegmentKey) -> bool {
        let (start, end, seq_id) = self.key.unpack();
        key.is_tombstone() && start <= key.start() && key.end() <= end && seq_id < key.seq_id()
    }

    fn add_tombstone(&mut self, pair: (SegmentKey, ImmutableSegment)) {
        self.metadata += pair.1.metadata();
        self.tombstone_segments.push(pair);
    }

    pub(super) fn is_exhausted(&self) -> bool {
        self.metadata.pending_count == self.metadata.tombstone_count
    }

    pub(super) fn keys(&self) -> Vec<SegmentKey> {
        self.tombstone_segments
            .iter()
            .map(|(key, _)| *key)
            .chain(once(self.key))
            .collect()
    }

    pub(super) fn paths(&self) -> Vec<&Path> {
        self.tombstone_segments
            .iter()
            .map(|(_, segment)| segment.path())
            .chain(once(self.pending_segment.path()))
            .collect()
    }

    pub(super) fn segments(&self) -> Vec<ImmutableSegment> {
        self.tombstone_segments
            .iter()
            .map(|(_, segment)| segment.clone())
            .chain(once(self.pending_segment.clone()))
            .collect()
    }
}

impl Debug for Range {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let segment_count = 1 + self.tombstone_segments.len();
        write!(
            f,
            "Range({}, {} {}, {} pending, {} tombstone)",
            self.key,
            segment_count,
            if segment_count == 1 {
                "segment"
            } else {
                "segments"
            },
            self.metadata.pending_count,
            self.metadata.tombstone_count,
        )
    }
}
