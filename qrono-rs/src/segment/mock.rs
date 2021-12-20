use crate::data::{Entry, Key};
use crate::segment::{Metadata, Segment, SegmentReader};

use std::sync::Arc;

pub struct MockSegment<F>
where
    F: Fn(usize) -> Option<Entry>,
{
    f: Arc<F>,
}

impl<F> MockSegment<F>
where
    F: Fn(usize) -> Option<Entry>,
{
    pub fn new(f: F) -> MockSegment<F> {
        MockSegment { f: Arc::new(f) }
    }
}

impl<F> Segment for MockSegment<F>
where
    F: Fn(usize) -> Option<Entry> + 'static,
{
    type R = MockSegmentReader<F>;

    fn open_reader(&self, _pos: Key) -> std::io::Result<Self::R> {
        let reader = MockSegmentReader::new_arc(Arc::clone(&self.f));
        Ok(reader)
    }

    fn metadata(&self) -> Metadata {
        Metadata::default()
    }
}

pub struct MockSegmentReader<F>
where
    F: Fn(usize) -> Option<Entry>,
{
    i: usize,
    f: Arc<F>,
    entry: Option<Entry>,
}

impl<F> MockSegmentReader<F>
where
    F: Fn(usize) -> Option<Entry>,
{
    fn new_arc(f: Arc<F>) -> MockSegmentReader<F> {
        let entry = f(0);
        MockSegmentReader { i: 1, f, entry }
    }

    pub fn new(f: F) -> MockSegmentReader<F> {
        Self::new_arc(Arc::new(f))
    }
}

impl<F> SegmentReader for MockSegmentReader<F>
where
    F: Fn(usize) -> Option<Entry>,
{
    fn next(&mut self) -> std::io::Result<Option<Entry>> {
        Ok(match self.entry.take() {
            Some(entry) => {
                self.entry = (self.f)(self.i);
                self.i += 1;
                Some(entry)
            }
            None => None,
        })
    }

    fn peek(&mut self) -> std::io::Result<Option<Entry>> {
        Ok(self.entry.clone())
    }

    fn peek_key(&self) -> Option<Key> {
        self.entry.as_ref().map(|entry| entry.key())
    }
}
