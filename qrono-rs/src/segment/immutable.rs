use crate::data::{Entry, Key, SegmentID};
use crate::encoding;

use crate::segment::{Metadata, Segment, SegmentReader};
use bytes::{Buf, BufMut, BytesMut};

use std::convert::TryInto;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Cursor, Read, Seek, Write};

use std::path::{Path, PathBuf};
use std::{fs, io};
use tokio::io::SeekFrom;

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

#[derive(Copy, Clone)]
enum Kind {
    Pending,
    Tombstone,
}

impl Kind {
    fn is_pending(self) -> bool {
        matches!(self, Self::Pending)
    }

    fn is_tombstone(self) -> bool {
        matches!(self, Self::Tombstone)
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
            Self::INDEX_BLOCK_TYPE => {
                let len = u64::from_be_bytes(buf[..8].try_into().unwrap());
                BlockHeader::IndexBlock(len)
            }
            Self::FOOTER_BLOCK_TYPE => BlockHeader::Footer,
            _ => {
                let key = encoding::get_key(&mut &buf[..]);
                BlockHeader::Entry(key)
            }
        }
    }

    fn read<R: Read>(src: &mut R) -> io::Result<BlockHeader> {
        let mut buf = [0u8; 16];
        src.read_exact(&mut buf)?;
        Ok(BlockHeader::decode(buf))
    }

    fn encode(&self, buf: &mut BytesMut) -> usize {
        match self {
            BlockHeader::Entry(key) => encoding::put_key(buf, *key),
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
        let key = encoding::decode_key(buf[..16].try_into().unwrap());
        let pos = u64::from_be_bytes(buf[16..].try_into().unwrap());
        IndexEntry { key, pos }
    }

    fn encode(&self, buf: &mut BytesMut) -> usize {
        encoding::put_key(buf, self.key);
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

    fn encode(&self, buf: &mut BytesMut) -> usize {
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

    fn encode(&self, buf: &mut BytesMut) -> usize {
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

            let mut buf = BytesMut::with_capacity(1024 * 1024);
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
                encoding::put_key(&mut buf, entry.key());
                if let Entry::Pending(item) = &entry {
                    assert!(
                        kind.is_pending(),
                        "pending item added to tombstone segment: {:?}",
                        entry,
                    );
                    encoding::put_stats(&mut buf, &item.stats);
                    encoding::put_value(&mut buf, &item.value);
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
                    encoding::read_entry_rest(&mut self.src, key, self.segment_id)?;
                    return self.read_next_key();
                }
                BlockHeader::IndexBlock(len) => {
                    match IndexBlock::search(&mut self.src, len, key)? {
                        Some(pos) => {
                            self.src.seek(SeekFrom::Start(pos))?;
                        }
                        None => {
                            self.src.seek(SeekFrom::Start(0))?;
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
            self.read_next_key()?;
            return Ok(Some(entry));
        }

        Ok(match self.peeked_key.take() {
            Some(key) => {
                let entry = encoding::read_entry_rest(&mut self.src, key, self.segment_id)?;
                self.read_next_key()?;
                Some(entry)
            }
            None => None,
        })
    }

    fn peek(&mut self) -> io::Result<Option<Entry>> {
        let entry = self.next()?;
        self.peeked_entry = entry.clone();
        Ok(entry)
    }

    fn peek_key(&self) -> Option<Key> {
        match &self.peeked_entry {
            Some(entry) => Some(entry.key()),
            None => self.peeked_key,
        }
    }
}
