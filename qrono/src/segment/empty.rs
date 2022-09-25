use crate::data::{Entry, Key};
use crate::segment::{Metadata, Segment, SegmentReader};

pub struct EmptySegment;

impl Segment for EmptySegment {
    type R = EmptySegment;

    fn open_reader(&self, _pos: Key) -> std::io::Result<Self::R> {
        Ok(EmptySegment)
    }

    fn metadata(&self) -> Metadata {
        Default::default()
    }
}

impl SegmentReader for EmptySegment {
    fn next(&mut self) -> std::io::Result<Option<Entry>> {
        Ok(None)
    }

    fn peek(&mut self) -> std::io::Result<Option<Entry>> {
        Ok(None)
    }

    fn peek_key(&self) -> Option<Key> {
        None
    }
}
