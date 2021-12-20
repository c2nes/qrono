use crate::data::{Item, Key, SegmentID, Timestamp, ID};
use crate::encoding;
use bytes::{Buf, Bytes, BytesMut};
use memmap::{MmapMut, MmapOptions};
use slab::Slab;

use std::fs::OpenOptions;
use std::io::{Cursor, ErrorKind};
use std::path::{Path, PathBuf};

use rustc_hash::{FxHashMap, FxHashSet};
use std::sync::{Arc, Mutex};
use std::{fs, io};

pub type FileId = u32;

#[derive(Clone)]
pub struct WorkingSet {
    inner: Arc<Mutex<Inner>>,
    directory: PathBuf,
}
// (item.deadline, file_id as u32, pos, len)

// ID -> FileID

// 1. Track empty files. Add their FileIDs to a Vec.
// 2. Have a background thread.

#[derive(Default)]
struct Inner {
    items: FxHashMap<ID, FileId>,
    files: Slab<WorkingSetFile>,
    file_ids: FxHashSet<FileId>,
    current: FileId,
}

pub struct ItemRef {
    id: ID,
    deadline: Timestamp,
    segment_id: SegmentID,
    len: u32,
    inner: Arc<Mutex<Inner>>,
}

impl WorkingSet {
    pub fn new<P: AsRef<Path>>(directory: P) -> io::Result<WorkingSet> {
        fs::create_dir_all(&directory)?;

        Ok(WorkingSet {
            inner: Default::default(),
            directory: directory.as_ref().to_path_buf(),
        })
    }

    pub fn add(&self, item: &Item) -> io::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        if inner.files.is_empty() || !inner.files[inner.current as usize].has_capacity(item) {
            let entry = inner.files.vacant_entry();
            let file_id = entry.key() as FileId;
            let path = {
                let mut path = self.directory.clone();
                path.push(format!("working.{}", file_id));
                path
            };

            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(&path)?;
            file.set_len(u32::MAX as u64)?;
            let mmap = unsafe { MmapOptions::new().map_mut(&file)? };
            entry.insert(WorkingSetFile {
                index: Default::default(),
                mmap,
                pos: 0,
                used: 0,
            });
            inner.file_ids.insert(file_id);
            inner.current = file_id;
        }
        let file_id = inner.current;
        inner.files[file_id as usize].add(item)?;
        inner.items.insert(item.id, file_id);
        Ok(())
    }

    // Added -> [Moved ...] -> Removed -> [Added ...]

    pub fn get(&self, id: ID) -> io::Result<Option<ItemRef>> {
        let inner = self.inner.lock().unwrap();
        let file_id = match inner.items.get(&id) {
            Some(file_id) => *file_id,
            None => return Ok(None),
        };

        let file = &inner.files[file_id as usize];
        let IndexEntry {
            deadline,
            segment_id,
            len,
            ..
        } = file.index[&id];
        let inner = Arc::clone(&self.inner);
        Ok(Some(ItemRef {
            id,
            deadline,
            segment_id,
            len,
            inner,
        }))
    }
}

impl ItemRef {
    pub fn key(&self) -> Key {
        Key::Pending {
            id: self.id,
            deadline: self.deadline,
        }
    }

    pub fn segment_id(&self) -> SegmentID {
        self.segment_id
    }

    pub fn load(&self) -> io::Result<Item> {
        let inner = self.inner.lock().unwrap();
        let file_id = match inner.items.get(&self.id) {
            Some(file_id) => *file_id,
            None => return Err(io::Error::new(ErrorKind::Other, "Item released")),
        };

        inner.files[file_id as usize].get(self.id)
    }

    pub fn release(self) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(file_id) = inner.items.remove(&self.id) {
            inner.files[file_id as usize].remove(&self);
        }
    }
}

#[derive(Copy, Clone)]
struct IndexEntry {
    deadline: Timestamp,
    segment_id: SegmentID,
    pos: u32,
    len: u32,
}

struct WorkingSetFile {
    index: FxHashMap<ID, IndexEntry>,
    mmap: MmapMut,
    pos: usize,
    used: usize,
}

impl WorkingSetFile {
    fn encoded_len(item: &Item) -> usize {
        encoding::STATS_SIZE + 4 + item.value.len()
    }

    fn occupancy(&self) -> f32 {
        if self.pos > 0 {
            self.used as f32 / self.pos as f32
        } else {
            1.0
        }
    }

    fn has_capacity(&self, item: &Item) -> bool {
        let available = self.mmap.len() - self.pos;
        let required = Self::encoded_len(item);
        required <= available
    }

    fn add(&mut self, item: &Item) -> io::Result<()> {
        let len = Self::encoded_len(item);
        let mut buf = BytesMut::with_capacity(len);
        // [ID][Stats][Value]
        // [
        encoding::put_stats(&mut buf, &item.stats);
        encoding::put_value(&mut buf, &item.value);
        self.mmap[self.pos..self.pos + len].copy_from_slice(&buf);
        let pos = self.pos;
        self.pos += len;
        self.used += len;
        self.index.insert(
            item.id,
            IndexEntry {
                deadline: item.deadline,
                segment_id: item.segment_id,
                pos: pos as u32,
                len: len as u32,
            },
        );
        Ok(())
    }

    fn get(&self, id: ID) -> io::Result<Item> {
        let entry = self.index[&id];
        let offset = entry.pos as usize;
        let mut buf = Cursor::new(&self.mmap[offset..]);
        let stats = encoding::get_stats(&mut buf);
        let len = buf.get_u32() as usize;
        let offset = offset + encoding::STATS_SIZE + 4;
        let value = &self.mmap[offset..offset + len];
        let value = Bytes::copy_from_slice(value);
        Ok(Item {
            id,
            deadline: entry.deadline,
            stats,
            value,
            segment_id: entry.segment_id,
        })
    }

    fn remove(&mut self, item: &ItemRef) {
        self.used -= item.len as usize;
        self.index.remove(&item.id);
    }
}
