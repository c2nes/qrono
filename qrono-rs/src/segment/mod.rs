use crate::data::{Entry, Key};

use std::io;

#[derive(Default, Copy, Clone, Debug)]
pub struct Metadata {
    pub pending_count: u64,
    pub tombstone_count: u64,
}

pub trait Segment {
    type R: SegmentReader + 'static;

    /// Opens a reader positioned at the first key *after* `pos`.
    fn open_reader(&self, pos: Key) -> io::Result<Self::R>;

    fn metadata(&self) -> Metadata;
}

pub trait SegmentReader {
    fn next(&mut self) -> io::Result<Option<Entry>>;
    fn peek(&mut self) -> io::Result<Option<Entry>>;
    fn peek_key(&self) -> Option<Key>;
}

mod empty;
mod filtered;
mod frozen;
mod immutable;
mod mem;
mod merged;

pub use empty::EmptySegment;
pub use filtered::FilteredSegmentReader;
pub use frozen::{FrozenSegment, FrozenSegmentReader};
pub use immutable::{ImmutableSegment, ImmutableSegmentReader};
pub use mem::{FrozenMemorySegment, MemorySegment, MemorySegmentReader};
pub use merged::{MergedSegmentReader, Token};

pub mod mock;
