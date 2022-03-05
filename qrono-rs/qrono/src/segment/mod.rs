use crate::data::{Entry, Key};

use std::io;
use std::ops::{Add, AddAssign};

#[derive(Default, Copy, Clone, Debug)]
pub struct Metadata {
    pub pending_count: u64,
    pub tombstone_count: u64,
}

impl Add for Metadata {
    type Output = Self;

    fn add(self, rhs: Self) -> Self::Output {
        Metadata {
            pending_count: self.pending_count + rhs.pending_count,
            tombstone_count: self.tombstone_count + rhs.tombstone_count,
        }
    }
}

impl AddAssign for Metadata {
    fn add_assign(&mut self, rhs: Self) {
        self.pending_count += rhs.pending_count;
        self.tombstone_count += rhs.tombstone_count;
    }
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
mod immutable;
mod mem;
mod merged;

pub use empty::EmptySegment;
pub use immutable::Kind as ImmutableSegmentKind;
pub use immutable::{ImmutableSegment, ImmutableSegmentReader};
pub use mem::{FrozenMemorySegment, MemorySegment, MemorySegmentReader};
pub use merged::{MergedSegmentReader, Token};

pub mod mock;
