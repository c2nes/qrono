use crate::data::{Item, Key, SegmentID, Timestamp, ID};
use crate::encoding;
use bytes::{Buf, Bytes, BytesMut};
use memmap::{MmapMut, MmapOptions};
use slab::Slab;

use std::fs::OpenOptions;
use std::io::{Cursor, ErrorKind};
use std::path::{Path, PathBuf};

use crate::scheduler::{Scheduler, State, Task, TaskHandle};
use log::info;
use rustc_hash::{FxHashMap, FxHashSet};
use std::sync::{Arc, Mutex};
use std::{fs, io, mem};

pub type FileId = u32;

#[derive(Clone)]
pub struct WorkingSet {
    stripes: Arc<Vec<Stripe>>,
}
// (item.deadline, file_id as u32, pos, len)

// ID -> FileID

// 1. Track empty files. Add their FileIDs to a Vec.
// 2. Have a background thread.

struct Stripe {
    shared: Arc<Mutex<Shared>>,
    compactor: TaskHandle<Compactor>,
}

#[derive(Default)]
struct Shared {
    items: FxHashMap<ID, FileId>,
    files: Slab<WorkingSetFile>,
    file_ids: FxHashSet<FileId>,
    empty_files: Vec<FileId>,
    current: FileId,
    directory: PathBuf,

    used: usize,
    total: usize,
}

impl Shared {
    const OCCUPANCY_TARGET: f32 = 0.1;

    fn occupancy(&self) -> f32 {
        self.used as f32 / self.total as f32
    }

    pub fn add(&mut self, item: &Item) -> io::Result<()> {
        let file = match self.files.get_mut(self.current as usize) {
            Some(file) if file.has_capacity(item) => file,
            old_file => {
                if let Some(old_file) = old_file {
                    self.used += old_file.used;
                    self.total += old_file.pos;
                }

                let entry = self.files.vacant_entry();
                let file_id = entry.key() as FileId;
                let path = {
                    let mut path = self.directory.clone();
                    path.push(format!("working.{}", file_id));
                    path
                };

                self.file_ids.insert(file_id);
                self.current = file_id;

                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&path)?;
                file.set_len(100 * 1024 * 1024)?;
                let mmap = unsafe { MmapOptions::new().map_mut(&file)? };

                entry.insert(WorkingSetFile {
                    path: path.clone(),
                    index: Default::default(),
                    mmap,
                    pos: 0,
                    used: 0,
                })
            }
        };

        let file_id = self.current;
        file.add(item)?;
        self.items.insert(item.id, file_id);
        Ok(())
    }

    pub fn get(&mut self, id: ID) -> Option<&IndexEntry> {
        self.items
            .get(&id)
            .map(|file_id| &self.files[*file_id as usize].index[&id])
    }

    pub fn release(&mut self, id: ID) -> bool {
        if let Some(file_id) = self.items.remove(&id) {
            let file = &mut self.files[file_id as usize];
            let len = file.remove(id);
            if file_id != self.current {
                self.used -= len;
            }

            if file.used == 0 {
                if file_id == self.current {
                    file.pos = 0;
                } else {
                    self.total -= file.pos;
                    self.empty_files.push(file_id);
                }
                return true;
            } else if self.occupancy() < Self::OCCUPANCY_TARGET {
                return true;
            }
        }

        false
    }
}

struct Compactor {
    shared: Arc<Mutex<Shared>>,
    directory: PathBuf,
}

impl Task for Compactor {
    type Value = ();
    type Error = io::Error;

    fn run(&mut self) -> Result<State<()>, io::Error> {
        let empty_files = {
            let mut shared = self.shared.lock().unwrap();
            mem::take(&mut shared.empty_files)
                .into_iter()
                .map(|file_id| {
                    shared.file_ids.remove(&file_id);
                    let file_id = file_id as usize;
                    let file = &shared.files[file_id];
                    (file_id, file.path.clone())
                })
                .collect::<Vec<_>>()
        };

        for (_, path) in &empty_files {
            fs::remove_file(path)?;
        }

        let mut shared = self.shared.lock().unwrap();
        for (file_id, _) in &empty_files {
            shared.files.remove(*file_id);
        }

        let occupancy = shared.occupancy();
        if occupancy < Shared::OCCUPANCY_TARGET {
            info!(
                "Working set compaction triggered; occupancy={:.3}, used={}, total={}",
                occupancy, shared.used, shared.total,
            );
            let items = shared
                .file_ids
                .iter()
                .map(|file_id| &shared.files[*file_id as usize])
                .max_by_key(|file| file.pos - file.used)
                .map(|file| {
                    file.index
                        .keys()
                        .take(10000)
                        .map(|id| file.get(*id))
                        .collect::<Vec<_>>()
                })
                .unwrap_or(vec![]);

            for item in items {
                let item = item?;
                shared.release(item.id);
                shared.add(&item)?;
            }
        }

        if shared.occupancy() < Shared::OCCUPANCY_TARGET || !shared.empty_files.is_empty() {
            Ok(State::Runnable)
        } else {
            Ok(State::Idle)
        }
    }
}

pub struct ItemRef<'a> {
    id: ID,
    deadline: Timestamp,
    segment_id: SegmentID,
    len: u32,
    stripe: &'a Stripe,
}

impl WorkingSet {
    pub fn new<P: AsRef<Path>>(directory: P, scheduler: Scheduler) -> io::Result<WorkingSet> {
        fs::create_dir_all(&directory)?;

        let directory = directory.as_ref().to_path_buf();
        let shared: Arc<Mutex<Shared>> = Arc::new(Mutex::new(Shared {
            directory: directory.clone(),
            ..Default::default()
        }));
        let (compactor, _) = scheduler.register(Compactor {
            shared: Arc::clone(&shared),
            directory: directory.clone(),
        });
        let stripes = Arc::new(vec![Stripe { shared, compactor }]);
        Ok(WorkingSet { stripes })
    }

    fn stripe(&self, id: ID) -> &Stripe {
        let n = self.stripes.len();
        let idx = id as usize % n;
        &self.stripes[idx]
    }

    pub fn add(&self, item: &Item) -> io::Result<()> {
        self.stripe(item.id).shared.lock().unwrap().add(item)
    }

    // Added -> [Moved ...] -> Removed -> [Added ...]

    pub fn get(&self, id: ID) -> io::Result<Option<ItemRef>> {
        let stripe = self.stripe(id);
        let mut shared = stripe.shared.lock().unwrap();
        if let Some(IndexEntry {
            deadline,
            segment_id,
            len,
            ..
        }) = shared.get(id)
        {
            Ok(Some(ItemRef {
                id,
                deadline: *deadline,
                segment_id: *segment_id,
                len: *len,
                stripe,
            }))
        } else {
            Ok(None)
        }
    }
}

impl<'a> ItemRef<'a> {
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
        let shared = self.stripe.shared.lock().unwrap();
        let file_id = match shared.items.get(&self.id) {
            Some(file_id) => *file_id,
            None => return Err(io::Error::new(ErrorKind::Other, "Item released")),
        };

        shared.files[file_id as usize].get(self.id)
    }

    pub fn release(self) {
        let mut shared = self.stripe.shared.lock().unwrap();
        if shared.release(self.id) {
            self.stripe.compactor.schedule();
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
    path: PathBuf,
    index: FxHashMap<ID, IndexEntry>,
    mmap: MmapMut,
    pos: usize,
    used: usize,
}

impl WorkingSetFile {
    fn encoded_len(item: &Item) -> usize {
        encoding::STATS_SIZE + 4 + item.value.len()
    }

    fn has_capacity(&self, item: &Item) -> bool {
        let available = self.mmap.len() - self.pos;
        let required = Self::encoded_len(item);
        required <= available
    }

    fn add(&mut self, item: &Item) -> io::Result<usize> {
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
        Ok(len)
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

    fn remove(&mut self, id: ID) -> usize {
        if let Some(entry) = self.index.remove(&id) {
            let len = entry.len as usize;
            self.used -= len;
            len
        } else {
            0
        }
    }
}
