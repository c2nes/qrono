use crate::data::{Item, Stats, ID};
use crate::encoding;
use memmap2::{MmapMut, MmapOptions};
use slab::Slab;

use std::fs::OpenOptions;
use std::io::ErrorKind;
use std::path::PathBuf;

use crate::scheduler::{Scheduler, State, Task, TaskContext, TaskHandle};
use log::info;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::bytes::Bytes;
use crate::encoding::{Decoder, Encoder};
use crate::result::IgnoreErr;
use parking_lot::Mutex;
use std::sync::Arc;
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
    stripe_idx: usize,
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

    pub fn add<I: ItemData>(&mut self, item: &I) -> io::Result<()> {
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
                    path.push(format!("working.{}.{}", self.stripe_idx, file_id));
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
                    path,
                    index: Default::default(),
                    mmap,
                    pos: 0,
                    used: 0,
                })
            }
        };

        let file_id = self.current;
        file.add(item)?;
        self.items.insert(item.id(), file_id);
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
}

impl Task for Compactor {
    type Value = ();
    type Error = io::Error;

    fn run(&mut self, _: &TaskContext<Compactor>) -> Result<State<()>, io::Error> {
        let empty_files = {
            let mut shared = self.shared.lock();
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

        let mut shared = self.shared.lock();
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
                .unwrap_or_else(Vec::new);

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

trait ItemData {
    fn id(&self) -> ID;
    fn stats(&self) -> &Stats;
    fn value(&self) -> &Bytes;
}

/// A WorkingItem contains the subset of Item data stored by the WorkingSet itself.
pub struct WorkingItem {
    pub id: ID,
    pub stats: Stats,
    pub value: Bytes,
}

impl ItemData for WorkingItem {
    fn id(&self) -> ID {
        self.id
    }

    fn stats(&self) -> &Stats {
        &self.stats
    }

    fn value(&self) -> &Bytes {
        &self.value
    }
}

impl ItemData for Item {
    fn id(&self) -> ID {
        self.id
    }

    fn stats(&self) -> &Stats {
        &self.stats
    }

    fn value(&self) -> &Bytes {
        &self.value
    }
}

impl<'a> ItemData for &'a Item {
    fn id(&self) -> ID {
        self.id
    }

    fn stats(&self) -> &Stats {
        &self.stats
    }

    fn value(&self) -> &Bytes {
        &self.value
    }
}

pub struct ItemRef<'a> {
    id: ID,
    stripe: &'a Stripe,
}

enum Op<'a> {
    Add(&'a Item),
    Release(ID),
}

pub struct Transaction<'a> {
    stripes: Vec<(&'a Stripe, Vec<Op<'a>>)>,
}

impl<'a> Transaction<'a> {
    fn stripe(&mut self, id: ID) -> &mut (&'a Stripe, Vec<Op<'a>>) {
        let n = self.stripes.len();
        let idx = id as usize % n;
        &mut self.stripes[idx]
    }

    pub fn add(&mut self, item: &'a Item) {
        self.stripe(item.id).1.push(Op::Add(item));
    }

    pub fn release(&mut self, id: ID) {
        self.stripe(id).1.push(Op::Release(id));
    }

    pub fn commit(self) -> io::Result<()> {
        let mut batch = self.stripes;
        let mut next = Vec::with_capacity(batch.len());
        let mut block = false;

        while !batch.is_empty() {
            let mut progressed = false;
            for (stripe, ops) in batch.drain(..) {
                if ops.is_empty() {
                    continue;
                }

                if let Some(mut locked) = if block {
                    block = false;
                    Some(stripe.shared.lock())
                } else {
                    stripe.shared.try_lock()
                } {
                    progressed = true;
                    let mut schedule_compaction = false;
                    for op in ops {
                        match op {
                            Op::Add(item) => locked.add(item)?,
                            Op::Release(id) => {
                                if locked.release(id) {
                                    schedule_compaction = true;
                                }
                            }
                        }
                    }
                    if schedule_compaction {
                        stripe.compactor.schedule().ignore_err();
                    }
                } else {
                    next.push((stripe, ops));
                }
            }

            mem::swap(&mut batch, &mut next);
            if !progressed {
                block = true;
            }
        }

        Ok(())
    }
}

impl WorkingSet {
    pub fn new(stripes: Vec<(PathBuf, Scheduler)>) -> io::Result<WorkingSet> {
        for (directory, _) in &stripes {
            fs::create_dir_all(&directory)?;
        }

        let stripes = stripes
            .into_iter()
            .enumerate()
            .map(|(stripe_idx, (directory, scheduler))| {
                let shared = Arc::new(Mutex::new(Shared {
                    stripe_idx,
                    directory,
                    ..Default::default()
                }));

                let (compactor, _) = scheduler.register(Compactor {
                    shared: Arc::clone(&shared),
                });
                Stripe { shared, compactor }
            })
            .collect::<Vec<_>>()
            .into();

        Ok(WorkingSet { stripes })
    }

    pub fn tx(&self) -> Transaction {
        let mut stripes = vec![];
        for stripe in self.stripes.iter() {
            stripes.push((stripe, vec![]));
        }
        Transaction { stripes }
    }

    fn stripe(&self, id: ID) -> &Stripe {
        let n = self.stripes.len();
        let idx = id as usize % n;
        &self.stripes[idx]
    }

    pub fn add(&self, item: &Item) -> io::Result<()> {
        self.stripe(item.id).shared.lock().add(item)
    }

    // Added -> [Moved ...] -> Removed -> [Added ...]

    pub fn get(&self, id: ID) -> io::Result<Option<ItemRef>> {
        let stripe = self.stripe(id);
        let mut shared = stripe.shared.lock();
        if shared.get(id).is_some() {
            Ok(Some(ItemRef { id, stripe }))
        } else {
            Ok(None)
        }
    }
}

impl<'a> ItemRef<'a> {
    pub fn load(&self) -> io::Result<WorkingItem> {
        let shared = self.stripe.shared.lock();
        let file_id = match shared.items.get(&self.id) {
            Some(file_id) => *file_id,
            None => return Err(io::Error::new(ErrorKind::Other, "Item released")),
        };

        shared.files[file_id as usize].get(self.id)
    }

    pub fn release(self) {
        let mut shared = self.stripe.shared.lock();
        if shared.release(self.id) {
            self.stripe.compactor.schedule().ignore_err();
        }
    }
}

#[derive(Copy, Clone)]
struct IndexEntry {
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
    fn encoded_len<I: ItemData>(item: &I) -> usize {
        encoding::STATS_SIZE + 4 + item.value().len()
    }

    fn has_capacity<I: ItemData>(&self, item: &I) -> bool {
        let available = self.mmap.len() - self.pos;
        let required = Self::encoded_len(item);
        required <= available
    }

    fn add<I: ItemData>(&mut self, item: &I) -> io::Result<usize> {
        let len = Self::encoded_len(item);
        let mut buf = Vec::with_capacity(len);
        buf.put_stats(item.stats());
        buf.put_value(item.value());
        self.mmap[self.pos..self.pos + len].copy_from_slice(&buf);
        let pos = self.pos;
        self.pos += len;
        self.used += len;
        self.index.insert(
            item.id(),
            IndexEntry {
                pos: pos as u32,
                len: len as u32,
            },
        );
        Ok(len)
    }

    fn get(&self, id: ID) -> io::Result<WorkingItem> {
        let entry = self.index[&id];
        let offset = entry.pos as usize;
        let mut buf = &self.mmap[offset..];
        let stats = buf.get_stats();
        let value = buf.get_value();
        Ok(WorkingItem { id, stats, value })
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
