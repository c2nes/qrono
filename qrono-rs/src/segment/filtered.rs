use crate::data::{Entry, Key};
use crate::segment::SegmentReader;

pub struct FilteredSegmentReader<R: SegmentReader, P: Fn(&Entry) -> bool> {
    reader: R,
    predicate: P,
}

impl<R: SegmentReader, P: Fn(&Entry) -> bool> FilteredSegmentReader<R, P> {
    pub fn new(reader: R, predicate: P) -> std::io::Result<FilteredSegmentReader<R, P>> {
        let mut filtered_reader = FilteredSegmentReader { reader, predicate };
        filtered_reader.advance()?;
        Ok(filtered_reader)
    }

    fn advance(&mut self) -> std::io::Result<()> {
        while let Some(entry) = self.reader.peek()? {
            if (self.predicate)(&entry) {
                break;
            }
            self.reader.next()?;
        }
        Ok(())
    }
}

impl<R: SegmentReader, P: Fn(&Entry) -> bool> SegmentReader for FilteredSegmentReader<R, P> {
    fn next(&mut self) -> std::io::Result<Option<Entry>> {
        let res = self.reader.next()?;
        self.advance()?;
        Ok(res)
    }

    fn peek(&mut self) -> std::io::Result<Option<Entry>> {
        self.reader.peek()
    }

    fn peek_key(&self) -> Option<Key> {
        self.reader.peek_key()
    }
}
