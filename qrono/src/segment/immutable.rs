use crate::data::{Entry, Item, Key, SegmentID};
use crate::encoding;

use crate::segment::{Metadata, Segment, SegmentReader};

use std::fs::{File, OpenOptions};
use std::io::{BufReader, Cursor, Read, Seek, Write};

use crate::bytes::Bytes;
use crate::encoding::{Decoder, Encoder};
use crate::io::ReadInto;
use bytes::{Buf, BufMut};
use encoding::{STATS_SIZE, VALUE_LEN_SIZE};
use std::fmt::{Display, Formatter};
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::{fs, io};

/*
Segment    := Block* Footer
Block      := Entry | IndexBlock

Entry      := Tombstone | Pending
Tombstone  := Key
Pending    := Key Stats ValueLen Value:[u8; ValueLen]

IndexBlock := Count:u64 <0b01>:u64 [IndexEntry; Count]
IndexEntry := Key Pos:u64

Footer     := (empty)
*/

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum Kind {
    Pending,
    Tombstone,
}

impl Kind {
    pub fn is_pending(self) -> bool {
        matches!(self, Self::Pending)
    }

    pub fn is_tombstone(self) -> bool {
        matches!(self, Self::Tombstone)
    }
}

impl Display for Kind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Kind::Pending => "pending",
            Kind::Tombstone => "tombstone",
        })
    }
}

#[derive(Clone)]
pub struct ImmutableSegment {
    segment_id: SegmentID,
    path: PathBuf,
    metadata: Metadata,
    kind: Kind,
}

#[derive(Debug)]
enum BlockHeader {
    Entry(Key),
    IndexBlock(u64),
    Footer,
}

impl BlockHeader {
    const SIZE: usize = 16;

    // The encoding module uses 0b00 and 0b11 for Pending and Tombstone entries respectively
    const INDEX_BLOCK_TYPE: u8 = 0b0000_0001;
    const FOOTER_BLOCK_TYPE: u8 = 0b0000_0101;

    fn decode(buf: [u8; Self::SIZE]) -> BlockHeader {
        match buf[15] {
            Self::INDEX_BLOCK_TYPE => BlockHeader::IndexBlock(buf.as_ref().get_u64()),
            Self::FOOTER_BLOCK_TYPE => BlockHeader::Footer,
            _ => BlockHeader::Entry(buf.as_ref().get_key()),
        }
    }

    fn read<R: Read>(src: &mut R) -> io::Result<BlockHeader> {
        let mut buf = [0u8; 16];
        src.read_exact(&mut buf)?;
        Ok(BlockHeader::decode(buf))
    }

    fn encode(&self, buf: &mut Vec<u8>) -> usize {
        match self {
            BlockHeader::Entry(key) => buf.put_key(*key),
            BlockHeader::IndexBlock(len) => {
                buf.put_u64(*len);
                buf.put_u64(Self::INDEX_BLOCK_TYPE as u64);
            }
            BlockHeader::Footer => {
                buf.put_u64(0);
                buf.put_u64(Self::FOOTER_BLOCK_TYPE as u64);
            }
        };
        Self::SIZE
    }
}

#[derive(Copy, Clone, Debug)]
struct IndexEntry {
    key: Key,
    pos: u64,
}

impl IndexEntry {
    const SIZE: usize = 24;

    fn decode(buf: [u8; Self::SIZE]) -> IndexEntry {
        let mut buf = &buf[..];
        let key = buf.get_key();
        let pos = buf.get_u64();
        IndexEntry { key, pos }
    }

    fn encode(&self, buf: &mut Vec<u8>) -> usize {
        buf.put_key(self.key);
        buf.put_u64(self.pos);
        Self::SIZE
    }
}

struct IndexBlock(Vec<IndexEntry>);

impl IndexBlock {
    const SIZE: usize = 1 << 11;
    const ENTRIES_PER_BLOCK: usize = (Self::SIZE - BlockHeader::SIZE) / IndexEntry::SIZE;

    fn new() -> IndexBlock {
        IndexBlock(Vec::with_capacity(Self::ENTRIES_PER_BLOCK))
    }

    fn search<R: Read>(src: &mut BufReader<R>, count: u64, key: Key) -> io::Result<Option<u64>> {
        let mut pos = None;
        let mut buf = [0u8; IndexEntry::SIZE];
        for _ in 0..count {
            src.read_exact(&mut buf)?;
            let entry = IndexEntry::decode(buf);
            if key < entry.key {
                break;
            }
            pos = Some(entry.pos);
        }
        Ok(pos)
    }

    fn first_key(&self) -> Key {
        self.0[0].key
    }

    fn add(&mut self, index_entry: IndexEntry) {
        assert!(!self.is_full());
        self.0.push(index_entry);
    }

    fn len(&self) -> usize {
        self.0.len()
    }

    fn is_full(&self) -> bool {
        self.len() == Self::ENTRIES_PER_BLOCK
    }

    fn header(&self) -> BlockHeader {
        BlockHeader::IndexBlock(self.len() as u64)
    }

    fn encode(&self, buf: &mut Vec<u8>) -> usize {
        let mut len = self.header().encode(buf);
        for entry in &self.0 {
            len += entry.encode(buf);
        }
        if len < Self::SIZE {
            let padding = vec![0u8; Self::SIZE - len];
            buf.put_slice(&padding);
        }
        Self::SIZE
    }
}

struct Footer {
    segment_id: SegmentID,
    metadata: Metadata,
    kind: Kind,
}

impl Footer {
    const SIZE: usize = BlockHeader::SIZE + 8 + 8 + 8 + 1;

    fn decode(buf: [u8; Self::SIZE]) -> Footer {
        let mut buf = Cursor::new(&buf);
        // Skip block header
        buf.set_position(BlockHeader::SIZE as u64);

        let segment_id = buf.get_u64() as SegmentID;
        let pending_count = buf.get_u64();
        let tombstone_count = buf.get_u64();
        let flags = buf.get_u8();
        let kind = match flags & 0b1 {
            0b0 => Kind::Tombstone,
            _ => Kind::Pending,
        };
        Footer {
            segment_id,
            metadata: Metadata {
                pending_count,
                tombstone_count,
            },
            kind,
        }
    }

    fn encode(&self, buf: &mut Vec<u8>) -> usize {
        BlockHeader::Footer.encode(buf);
        buf.put_u64(self.segment_id);
        buf.put_u64(self.metadata.pending_count);
        buf.put_u64(self.metadata.tombstone_count);
        buf.put_u8(match self.kind {
            Kind::Pending => 0b1,
            Kind::Tombstone => 0b0,
        });
        Self::SIZE
    }
}

impl ImmutableSegment {
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<ImmutableSegment> {
        let mut src = File::open(&path)?;
        let Footer {
            metadata,
            segment_id,
            kind,
        } = ImmutableSegmentReader::read_footer(&mut src)?;
        let path = path.as_ref().into();
        Ok(ImmutableSegment {
            path,
            metadata,
            segment_id,
            kind,
        })
    }

    pub fn write_pending<P: AsRef<Path>, R: SegmentReader>(
        path: P,
        src: R,
        segment_id: SegmentID,
    ) -> io::Result<ImmutableSegment> {
        Self::write(path, src, segment_id, Kind::Pending)
    }

    pub fn write_tombstone<P: AsRef<Path>, R: SegmentReader>(
        path: P,
        src: R,
        segment_id: SegmentID,
    ) -> io::Result<ImmutableSegment> {
        Self::write(path, src, segment_id, Kind::Tombstone)
    }

    fn write<P: AsRef<Path>, R: SegmentReader>(
        path: P,
        mut src: R,
        segment_id: SegmentID,
        kind: Kind,
    ) -> io::Result<ImmutableSegment> {
        let temp_path = crate::path::with_temp_suffix(&path);
        let mut metadata = Metadata::default();

        {
            let mut out = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temp_path)?;

            let mut buf = Vec::with_capacity(1024 * 1024);
            let mut pos = 0;

            let mut index = Vec::with_capacity(8);
            index.push(IndexBlock::new());

            while let Some(entry) = src.next()? {
                if index[0].is_full() {
                    let mut carry = None;
                    for blk in index.iter_mut() {
                        if let Some(carry) = carry.take() {
                            blk.add(carry);
                        }

                        if blk.is_full() {
                            let key = blk.first_key();
                            carry = Some(IndexEntry { key, pos });
                            pos += blk.encode(&mut buf) as u64;
                            *blk = IndexBlock::new();
                        } else {
                            break;
                        }
                    }
                    if let Some(carry) = carry.take() {
                        let mut blk = IndexBlock::new();
                        blk.add(carry);
                        index.push(blk);
                    }
                }

                let key = entry.key();
                index[0].add(IndexEntry { key, pos });

                let len_before = buf.len();
                buf.put_key(entry.key());

                if let Entry::Pending(item) = &entry {
                    assert!(
                        kind.is_pending(),
                        "pending item added to tombstone segment: {:?}",
                        entry,
                    );
                    buf.put_stats(&item.stats);
                    buf.put_value(&item.value);
                } else {
                    assert!(
                        kind.is_tombstone(),
                        "tombstone added to pending segment: {:?}",
                        entry,
                    );
                }
                pos += (buf.len() - len_before) as u64;

                if buf.len() > 512 * 1024 {
                    out.write_all(&buf)?;
                    buf.clear();
                }

                match entry {
                    Entry::Tombstone { .. } => metadata.tombstone_count += 1,
                    Entry::Pending(_) => metadata.pending_count += 1,
                }
            }

            // Flush remaining index blocks
            let mut carry = None;
            for blk in index.iter_mut() {
                if let Some(carry) = carry.take() {
                    blk.add(carry);
                }

                let key = blk.first_key();
                carry = Some(IndexEntry { key, pos });
                pos += blk.encode(&mut buf) as u64;
            }

            // Write footer
            Footer {
                segment_id,
                metadata,
                kind,
            }
            .encode(&mut buf);

            out.write_all(&buf)?;
            out.sync_all()?;
        }

        fs::rename(&temp_path, &path)?;

        let path = path.as_ref().to_path_buf();
        Ok(ImmutableSegment {
            path,
            metadata,
            segment_id,
            kind,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn kind(&self) -> Kind {
        self.kind
    }

    pub fn rename<P: AsRef<Path>>(&mut self, path: P) -> io::Result<()> {
        let path = path.as_ref().to_path_buf();
        if self.path != path {
            fs::rename(&self.path, &path)?;
            self.path = path;
        }
        Ok(())
    }
}

impl Segment for ImmutableSegment {
    type R = ImmutableSegmentReader;

    fn open_reader(&self, pos: Key) -> io::Result<Self::R> {
        ImmutableSegmentReader::open(&self.path, pos, self.segment_id)
    }

    fn metadata(&self) -> Metadata {
        self.metadata
    }
}

pub struct ImmutableSegmentReader {
    segment_id: SegmentID,
    src: BufReader<File>,
    peeked_key: Option<Key>,
    peeked_entry: Option<Entry>,
}

impl ImmutableSegmentReader {
    fn open<P: AsRef<Path>>(
        path: P,
        pos: Key,
        segment_id: SegmentID,
    ) -> io::Result<ImmutableSegmentReader> {
        let src = File::open(&path)?;
        let src = BufReader::new(src);
        let mut reader = ImmutableSegmentReader {
            segment_id,
            src,
            peeked_key: None,
            peeked_entry: None,
        };
        reader.seek(pos)?;
        Ok(reader)
    }

    fn read_footer<R: Read + Seek>(src: &mut R) -> io::Result<Footer> {
        src.seek(SeekFrom::End(-(Footer::SIZE as i64)))?;
        let mut buf = [0u8; Footer::SIZE];
        src.read_exact(&mut buf)?;
        Ok(Footer::decode(buf))
    }

    fn seek(&mut self, key: Key) -> io::Result<()> {
        let root_block_offset = IndexBlock::SIZE + Footer::SIZE;
        self.src.seek(SeekFrom::End(-(root_block_offset as i64)))?;

        loop {
            match BlockHeader::read(&mut self.src)? {
                BlockHeader::Entry(key) => {
                    if key.is_pending() {
                        // Read stats and value length, then skip over value.
                        let mut scratch = [0u8; STATS_SIZE + VALUE_LEN_SIZE];
                        self.src.read_exact(&mut scratch[..])?;

                        let mut buf = &scratch[..];
                        let _ = buf.get_stats();
                        let len = buf.get_value_len();
                        self.src.seek(SeekFrom::Current(len as i64))?;
                    }
                    return self.read_next_key();
                }
                BlockHeader::IndexBlock(len) => {
                    match IndexBlock::search(&mut self.src, len, key)? {
                        Some(pos) => {
                            self.src.seek(SeekFrom::Start(pos))?;
                        }
                        None => {
                            self.src.rewind()?;
                            return self.read_next_key();
                        }
                    }
                }
                BlockHeader::Footer => panic!("unexpected footer"),
            }
        }
    }

    fn read_next_key(&mut self) -> io::Result<()> {
        loop {
            match BlockHeader::read(&mut self.src) {
                Ok(header) => match header {
                    BlockHeader::Entry(key) => {
                        self.peeked_key = Some(key);
                        return Ok(());
                    }
                    BlockHeader::IndexBlock(_) => {
                        // Skip over the index block
                        let offset = IndexBlock::SIZE - BlockHeader::SIZE;
                        self.src.seek_relative(offset as i64)?;
                    }
                    BlockHeader::Footer => {
                        self.peeked_entry = None;
                        return Ok(());
                    }
                },
                Err(err) => {
                    return match err.kind() {
                        io::ErrorKind::UnexpectedEof => {
                            self.peeked_key = None;
                            Ok(())
                        }
                        _ => Err(err),
                    }
                }
            }
        }
    }
}

impl SegmentReader for ImmutableSegmentReader {
    fn next(&mut self) -> io::Result<Option<Entry>> {
        if let Some(entry) = self.peeked_entry.take() {
            return Ok(Some(entry));
        }

        Ok(match self.peeked_key.take() {
            Some(key) => {
                let segment_id = self.segment_id;
                let entry = match key {
                    Key::Pending { id, deadline } => {
                        let mut scratch = [0u8; STATS_SIZE + VALUE_LEN_SIZE];
                        self.src.read_exact(&mut scratch[..])?;

                        let mut buf = &scratch[..];
                        let stats = buf.get_stats();
                        let len = buf.get_value_len();

                        let mut value = Vec::with_capacity(len);
                        self.src.read_exact_into(&mut value)?;
                        let value = Bytes::from(value);

                        Entry::Pending(Item {
                            id,
                            deadline,
                            stats,
                            value,
                            segment_id,
                        })
                    }
                    Key::Tombstone { id, deadline } => Entry::Tombstone {
                        id,
                        deadline,
                        segment_id,
                    },
                };

                self.read_next_key()?;
                Some(entry)
            }
            None => None,
        })
    }

    fn peek(&mut self) -> io::Result<Option<Entry>> {
        match &self.peeked_entry {
            Some(entry) => Ok(Some(entry.clone())),
            None => {
                let entry = self.next()?;
                self.peeked_entry = entry.clone();
                Ok(entry)
            }
        }
    }

    fn peek_key(&self) -> Option<Key> {
        match &self.peeked_entry {
            Some(entry) => Some(entry.key()),
            None => self.peeked_key,
        }
    }
}

#[cfg(all(test, not(miri)))]
mod tests {
    use crate::data::{Entry, Key, Timestamp, ID};
    use crate::segment::mock::MockSegmentReader;
    use crate::segment::{ImmutableSegment, Segment, SegmentReader};
    use std::io;
    use tempfile::{tempdir, TempDir};

    fn tombstone_segment() -> io::Result<(TempDir, ImmutableSegment)> {
        let dir = tempdir()?;
        let src = MockSegmentReader::new(|i| {
            if i < 100 {
                Some(Entry::Tombstone {
                    id: (i + 100) as ID,
                    deadline: Timestamp::ZERO,
                    segment_id: 0,
                })
            } else {
                None
            }
        });

        let path = dir.path().join("segment");
        let segment = ImmutableSegment::write_tombstone(path, src, 0)?;
        Ok((dir, segment))
    }

    #[test]
    fn open_before_start() -> io::Result<()> {
        let (_dir, segment) = tombstone_segment()?;
        let reader = segment.open_reader(Key::Tombstone {
            id: 98,
            deadline: Timestamp::ZERO,
        })?;

        assert_eq!(
            Some(Key::Tombstone {
                id: 100,
                deadline: Timestamp::ZERO
            }),
            reader.peek_key()
        );

        Ok(())
    }

    #[test]
    fn open_at_start() -> io::Result<()> {
        let (_dir, segment) = tombstone_segment()?;
        let reader = segment.open_reader(Key::Tombstone {
            id: 99,
            deadline: Timestamp::ZERO,
        })?;

        assert_eq!(
            Some(Key::Tombstone {
                id: 100,
                deadline: Timestamp::ZERO
            }),
            reader.peek_key()
        );

        Ok(())
    }

    #[test]
    fn open_after_start() -> io::Result<()> {
        let (_dir, segment) = tombstone_segment()?;
        let reader = segment.open_reader(Key::Tombstone {
            id: 100,
            deadline: Timestamp::ZERO,
        })?;

        assert_eq!(
            Some(Key::Tombstone {
                id: 101,
                deadline: Timestamp::ZERO
            }),
            reader.peek_key()
        );

        Ok(())
    }

    #[test]
    fn open_before_end() -> io::Result<()> {
        let (_dir, segment) = tombstone_segment()?;
        let mut reader = segment.open_reader(Key::Tombstone {
            id: 198,
            deadline: Timestamp::ZERO,
        })?;

        assert_eq!(
            Some(Key::Tombstone {
                id: 199,
                deadline: Timestamp::ZERO
            }),
            reader.peek_key()
        );

        assert!(reader.next()?.is_some());
        assert!(reader.next()?.is_none());

        Ok(())
    }

    #[test]
    fn open_at_end() -> io::Result<()> {
        let (_dir, segment) = tombstone_segment()?;
        let mut reader = segment.open_reader(Key::Tombstone {
            id: 199,
            deadline: Timestamp::ZERO,
        })?;

        assert_eq!(None, reader.peek_key());
        assert_eq!(None, reader.peek()?);
        assert_eq!(None, reader.next()?);
        Ok(())
    }

    #[test]
    fn open_after_end() -> io::Result<()> {
        let (_dir, segment) = tombstone_segment()?;
        let mut reader = segment.open_reader(Key::Tombstone {
            id: 200,
            deadline: Timestamp::ZERO,
        })?;

        assert_eq!(None, reader.peek_key());
        assert_eq!(None, reader.peek()?);
        assert_eq!(None, reader.next()?);
        Ok(())
    }
}
