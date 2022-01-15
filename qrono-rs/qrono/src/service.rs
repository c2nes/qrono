use crate::ops::{
    CompactReq, CompactResp, DeleteReq, DeleteResp, DequeueReq, DequeueResp, EnqueueReq,
    EnqueueResp, IdPattern, InfoReq, InfoResp, PeekReq, PeekResp, ReleaseReq, ReleaseResp,
    RequeueReq, RequeueResp,
};
use crate::{promise, timer};

use std::fs::File;
use std::hash::BuildHasherDefault;

use crate::data::{Entry, Item, Key, SegmentID, Stats, Timestamp, ID};
use crate::scheduler::{
    Scheduler, SimpleTask, State, Task, TaskContext, TaskFuture, TaskHandle, TransferAsync,
};
use crate::segment::{
    FilteredSegmentReader, FrozenMemorySegment, ImmutableSegment, MemorySegment,
    MemorySegmentReader, MergedSegmentReader, Metadata, Segment, SegmentReader,
};
use crate::wal::{ReadError, WriteAheadLog};

use crate::id_generator::IdGenerator;
use crossbeam::channel::{Receiver, Sender};
use dashmap::DashMap;

use crate::working_set::WorkingSet;
use indexmap::IndexSet;
use log::{debug, error, info, trace};
use rustc_hash::{FxHashSet, FxHasher};

use std::path::{Path, PathBuf};

use crate::service::blocked_dequeues::BlockedDequeues;
use backtrace::Backtrace;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::fmt::{Display, Formatter};
use std::ops::RangeInclusive;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::{fs, io, result};
use QueueFile::{DeletionMarker, PendingSegment, TemporarySegmentDirectory, TombstoneSegment};

const SEGMENT_FLUSH_THRESHOLD: usize = 128 * 1024 * 1024;

enum Op {
    Enqueue(EnqueueReq, Promise<EnqueueResp>),
    Dequeue(DequeueReq, Promise<DequeueResp>),
    Requeue(RequeueReq, Promise<RequeueResp>),
    Release(ReleaseReq, Promise<ReleaseResp>),
    Info(InfoReq, Promise<InfoResp>),
    Peek(PeekReq, Promise<PeekResp>),
    Delete(DeleteReq, Promise<DeleteResp>),
    Compact(CompactReq, Promise<CompactResp>),
}

enum Response {
    Enqueue(Promise<EnqueueResp>, Result<EnqueueResp>),
    Dequeue(Promise<DequeueResp>, Result<DequeueResp>),
    Requeue(Promise<RequeueResp>, Result<RequeueResp>),
    Release(Promise<ReleaseResp>, Result<ReleaseResp>),
    Info(Promise<InfoResp>, Result<InfoResp>),
    Peek(Promise<PeekResp>, Result<PeekResp>),
    Delete(Promise<DeleteResp>, Result<DeleteResp>),
    Compact(Promise<CompactResp>, Result<CompactResp>),
}

impl Response {
    fn complete(self) {
        match self {
            Response::Enqueue(resp, val) => resp.complete(val),
            Response::Dequeue(resp, val) => resp.complete(val),
            Response::Requeue(resp, val) => resp.complete(val),
            Response::Release(resp, val) => resp.complete(val),
            Response::Info(resp, val) => resp.complete(val),
            Response::Peek(resp, val) => resp.complete(val),
            Response::Delete(resp, val) => resp.complete(val),
            Response::Compact(resp, val) => resp.complete(val),
        }
    }
}

struct Queue {
    name: String,
    op_sender: Sender<Op>,
    op_processor: TaskHandle<OpProcessor>,
    op_processor_future: TaskFuture<OpProcessor>,
    directory: PathBuf,
}

struct Shared {
    current: MemorySegment,
    current_reader: MemorySegmentReader,
    current_id: SegmentID,
    immutable: MergedSegmentReader<SegmentKey>,
    last: Key,
    force_flush: bool,
}

enum DequeueError {
    Empty,
    Pending(Timestamp),
    IOError(io::Error),
}

impl From<io::Error> for DequeueError {
    fn from(err: io::Error) -> Self {
        DequeueError::IOError(err)
    }
}

impl Shared {
    fn head(&mut self) -> Option<(&mut dyn SegmentReader, Key, &'static str)> {
        let key_current = self.current_reader.peek_key();
        let key_immutable = self.immutable.peek_key();

        match (key_current, key_immutable) {
            (Some(key_current), Some(key_immutable)) => {
                if key_current < key_immutable {
                    Some((&mut self.current_reader, key_current, "current"))
                } else {
                    Some((&mut self.immutable, key_immutable, "immutable"))
                }
            }
            (Some(key), None) => Some((&mut self.current_reader, key, "current")),
            (None, Some(key)) => Some((&mut self.immutable, key, "immutable")),
            (None, None) => None,
        }
    }

    fn dequeue(&mut self, now: Timestamp) -> result::Result<Item, DequeueError> {
        if let Some((reader, key, reader_name)) = self.head() {
            if key.deadline() <= now {
                match reader.next()?.unwrap() {
                    Entry::Pending(item) => Ok(item),
                    Entry::Tombstone { id, deadline, .. } => {
                        panic!(
                            "BUG: dequeued tombstone; id={}, deadline={:?}",
                            id, deadline
                        );
                    }
                }
            } else {
                Err(DequeueError::Pending(key.deadline()))
            }
        } else {
            Err(DequeueError::Empty)
        }
    }

    fn peek(&mut self, now: Timestamp) -> Result<Option<Entry>> {
        if let Some((reader, key, _)) = self.head() {
            if key.deadline() <= now {
                return Ok(reader.peek()?);
            }
        }
        Ok(None)
    }
}

struct OpProcessor {
    op_receiver: Receiver<Op>,
    shared: Arc<Mutex<Shared>>,
    coordinator: Arc<Mutex<SegmentCoordinator>>,
    working_set: WorkingSet,
    working_set_ids: IndexSet<ID, BuildHasherDefault<FxHasher>>,
    id_generator: IdGenerator,
    segment_flusher: TaskHandle<MemorySegmentFlusher>,
    segment_flusher_future: TaskFuture<MemorySegmentFlusher>,

    blocked_dequeues: BlockedDequeues,
    timer: timer::Scheduler,
    timer_id: Option<timer::ID>,
}

impl Task for OpProcessor {
    type Value = ();
    type Error = ();

    fn run(&mut self, ctx: &TaskContext<Self>) -> result::Result<State<()>, ()> {
        let batch: Vec<Op> = self.op_receiver.try_iter().take(100).collect();
        let more_ready = batch.len() == 100;

        /*
        1. Execute read-only operations (peek, info).
        2. Prepare release, requeue, and enqueue operations.
        3. Write entry batch.
        4. Complete release, requeue and enqueue operations.
        5. Execute dequeue operations.
         */

        let mut enqueue_count = 0;
        let mut dequeue_count = 0;
        let mut requeue_count = 0;
        let mut release_count = 0;
        let mut deleted = false;
        for op in batch.iter() {
            match op {
                Op::Enqueue(_, _) => enqueue_count += 1,
                Op::Dequeue(_, _) => dequeue_count += 1,
                Op::Requeue(_, _) => requeue_count += 1,
                Op::Release(_, _) => release_count += 1,
                Op::Delete(_, _) => deleted = true,
                _ => {}
            }
        }

        let mut ids = self.id_generator.generate_ids(enqueue_count);
        let now = Timestamp::now();
        let mut entries = Vec::with_capacity(enqueue_count + 2 * requeue_count + release_count);
        let mut dequeues = Vec::with_capacity(dequeue_count);
        let mut working_set_releases = Vec::with_capacity(requeue_count + release_count);
        let mut responses = Vec::with_capacity(batch.len());

        let mut locked = self.shared.lock().unwrap();
        let last = locked.last;
        let adjust_deadline = |id, mut deadline| {
            if deadline < last.deadline() {
                deadline = last.deadline()
            }
            if deadline == last.deadline() && id < last.id() {
                deadline += Duration::from_millis(1);
            }
            deadline
        };

        let mut info_resp = None;

        for op in batch {
            match op {
                Op::Enqueue(req, resp) => {
                    let id = ids.next().unwrap();
                    let deadline = adjust_deadline(id, req.deadline.resolve(now));
                    let val = Ok(EnqueueResp { id, deadline });
                    responses.push(Response::Enqueue(resp, val));
                    entries.push(Entry::Pending(Item {
                        id,
                        deadline,
                        stats: Stats {
                            enqueue_time: now,
                            ..Default::default()
                        },
                        value: req.value.clone(),
                        segment_id: locked.current_id,
                    }));
                }
                Op::Dequeue(req, resp) => dequeues.push((req, resp)),
                Op::Requeue(req, resp) => {
                    let id = match req.id {
                        IdPattern::Any => self.working_set_ids.pop(),
                        IdPattern::Id(id) => self.working_set_ids.take(&id),
                    };

                    if let Some(id) = id {
                        let deadline = adjust_deadline(id, req.deadline.unwrap_or(now));
                        let item_ref = self
                            .working_set
                            .get(id)
                            .expect("TODO: Handle error")
                            .expect("BUG: id in working_set_ids but not in working_set");
                        entries.push(Entry::Tombstone {
                            id,
                            deadline: item_ref.key().deadline(),
                            segment_id: item_ref.segment_id(),
                        });
                        let mut item = item_ref.load().unwrap();
                        item.deadline = deadline;
                        item.stats.dequeue_count += 1;
                        item.stats.requeue_time = now;
                        item.segment_id = locked.current_id;
                        entries.push(Entry::Pending(item));
                        let val = Ok(RequeueResp { deadline });
                        responses.push(Response::Requeue(resp, val));
                        working_set_releases.push(item_ref);
                    } else {
                        let val = Err(Error::ItemNotDequeued);
                        responses.push(Response::Requeue(resp, val));
                    }
                }
                Op::Release(req, resp) => {
                    // TODO: Handle duplicate Requeue/Release in batch!
                    let id = match req.id {
                        IdPattern::Any => self.working_set_ids.pop(),
                        IdPattern::Id(id) => self.working_set_ids.take(&id),
                    };

                    if let Some(id) = id {
                        let item_ref = self
                            .working_set
                            .get(id)
                            .expect("TODO: Handle error")
                            .expect("BUG: id in working_set_ids but not in working_set");
                        entries.push(Entry::Tombstone {
                            id,
                            deadline: item_ref.key().deadline(),
                            segment_id: item_ref.segment_id(),
                        });
                        responses.push(Response::Release(resp, Ok(())));
                        working_set_releases.push(item_ref);
                    } else {
                        let val = Err(Error::ItemNotDequeued);
                        responses.push(Response::Release(resp, val));
                    }
                }
                Op::Peek(_, resp) => {
                    let val = match locked.peek(now).unwrap() {
                        Some(Entry::Pending(item)) => Ok(item),

                        None => Err(Error::NoItemReady),
                        Some(Entry::Tombstone { id, deadline, .. }) => {
                            error!("Peeked tombstone! id={}, deadline={:?}", id, deadline);
                            Err(Error::Internal("BUG: peeked tombstone"))
                        }
                    };

                    responses.push(Response::Peek(resp, val));
                }
                Op::Info(_, resp) => {
                    let val = info_resp.get_or_insert_with(|| {
                        let Metadata {
                            mut pending_count,
                            mut tombstone_count,
                        } = locked.current.metadata();
                        debug!(
                            "Op::Info: (current) pending={}, tombstone={}",
                            pending_count, tombstone_count
                        );
                        let segments = self.coordinator.lock().unwrap().live_segments().unwrap();
                        for segment in segments {
                            let meta = segment.metadata();
                            debug!(
                                "Op::Info: ({:?}) pending={}, tombstone={}",
                                segment.path(),
                                meta.pending_count,
                                meta.tombstone_count
                            );
                            pending_count += meta.pending_count;
                            tombstone_count += meta.tombstone_count;
                        }

                        let dequeued = self.working_set_ids.len() as u64;
                        let pending = pending_count - tombstone_count - dequeued;

                        InfoResp { pending, dequeued }
                    });

                    responses.push(Response::Info(resp, Ok(val.clone())));
                }
                Op::Delete(_, resp) => {
                    responses.push(Response::Delete(resp, Ok(())));
                }
                Op::Compact(_, resp) => {
                    locked.force_flush = true;
                    self.segment_flusher.schedule();
                    responses.push(Response::Compact(resp, Ok(())));
                }
            };
        }

        let current = &mut locked.current;
        current.add_all(entries).unwrap();
        // TODO: This threshold should be configurable. We should also
        //   have a size limit on the WAL itself to ensure it does not
        //   grow too large regardless of in-memory size.
        if current.size() > SEGMENT_FLUSH_THRESHOLD {
            self.segment_flusher.schedule();
        }

        let mut empty = false;
        let mut pending = None;
        let mut try_dequeue = || {
            if empty {
                return Err(DequeueError::Empty);
            }

            if let Some(deadline) = pending {
                return Err(DequeueError::Pending(deadline));
            }

            let res = locked.dequeue(now);
            match &res {
                Ok(item) => {
                    self.working_set.add(&item).unwrap();
                    self.working_set_ids.insert(item.id);

                    locked.last = Key::Pending {
                        id: item.id,
                        deadline: item.deadline,
                    };
                }
                Err(DequeueError::Empty) => {
                    empty = true;
                }
                Err(DequeueError::Pending(deadline)) => {
                    pending = Some(*deadline);
                }
                _ => {}
            }
            res
        };

        let mut pending_deadline = None;

        // Deliver to blocked dequeues first
        while let Some((_, _, resp)) = self.blocked_dequeues.front() {
            // Drop cancelled dequeues
            if resp.is_cancelled() {
                self.blocked_dequeues.pop_front();
                continue;
            }

            match try_dequeue() {
                Ok(item) => {
                    let (_, count, resp) = self.blocked_dequeues.pop_front().unwrap();
                    let mut items = vec![item];
                    // TODO: Make this configurable
                    const MAX_BATCH_SIZE: u64 = 100;
                    let count = count.min(MAX_BATCH_SIZE) as usize;
                    while items.len() < count {
                        match try_dequeue() {
                            Ok(item) => items.push(item),
                            _ => break,
                        }
                    }
                    responses.push(Response::Dequeue(resp, Ok(items)));
                }
                Err(DequeueError::Empty) => {
                    break;
                }
                Err(DequeueError::Pending(deadline)) => {
                    pending_deadline = Some(deadline);
                    break;
                }
                Err(DequeueError::IOError(err)) => {
                    error!("IO error on dequeue: {:?}", err);
                    let (_, _, resp) = self.blocked_dequeues.pop_front().unwrap();
                    responses.push(Response::Dequeue(resp, Err(err.into())));
                }
            }
        }

        // Handle new dequeues next
        for (req, resp) in dequeues {
            let val = match try_dequeue() {
                Ok(item) => {
                    let mut items = vec![item];
                    // TODO: Make this configurable
                    const MAX_BATCH_SIZE: u64 = 100;
                    let count = req.count.min(MAX_BATCH_SIZE) as usize;
                    while items.len() < count {
                        match try_dequeue() {
                            Ok(item) => items.push(item),
                            _ => break,
                        }
                    }
                    Ok(items)
                }
                Err(DequeueError::IOError(err)) => {
                    error!("IO error on dequeue: {:?}", err);
                    Err(err.into())
                }
                Err(err) => {
                    if let DequeueError::Pending(deadline) = err {
                        pending_deadline = Some(deadline);
                    }

                    if req.timeout.is_zero() {
                        Err(Error::NoItemReady)
                    } else {
                        self.blocked_dequeues
                            .push_back(now + req.timeout, req.count, resp);
                        continue;
                    }
                }
            };

            responses.push(Response::Dequeue(resp, val));
        }

        drop(locked);

        // Complete blocked dequeues that have timed out
        for (_, _, resp) in self.blocked_dequeues.expire(now) {
            responses.push(Response::Dequeue(resp, Err(Error::NoItemReady)));
        }

        for item_ref in working_set_releases {
            item_ref.release();
        }

        for resp in responses {
            resp.complete();
        }

        let next_wakeup = pending_deadline
            .iter()
            .chain(self.blocked_dequeues.next_timeout().iter())
            .min()
            .cloned();

        if let Some(deadline) = next_wakeup {
            trace!("Scheduling wake up in {:?}", deadline - now);
            if let Some(id) = self.timer_id.take() {
                self.timer.cancel(id);
            }

            let deadline = Instant::now() + (deadline - now);
            self.timer_id = {
                let ctx = ctx.clone();
                Some(self.timer.schedule(deadline, move || ctx.schedule()))
            };
        }

        if deleted {
            while let Some((_, _, resp)) = self.blocked_dequeues.pop_front() {
                resp.complete(Err(Error::NoItemReady));
            }

            if let Some(id) = self.timer_id.take() {
                self.timer.cancel(id);
            }

            Ok(State::Complete(()))
        } else if more_ready {
            Ok(State::Runnable)
        } else {
            Ok(State::Idle)
        }
    }
}

struct MemorySegmentWriter {
    coordinator: Arc<Mutex<SegmentCoordinator>>,
    directory: PathBuf,
}

impl MemorySegmentWriter {
    fn open<P: AsRef<Path>>(
        coordinator: Arc<Mutex<SegmentCoordinator>>,
        directory: P,
    ) -> Result<MemorySegmentWriter> {
        let directory = directory.as_ref().to_path_buf();
        let writer = MemorySegmentWriter {
            coordinator,
            directory,
        };
        writer.recover()?;
        Ok(writer)
    }

    fn write(
        &self,
        id: SegmentID,
        segment: FrozenMemorySegment,
    ) -> Result<(PathBuf, Option<ImmutableSegment>, Vec<ImmutableSegment>)> {
        let frozen = segment.to_frozen_segment();
        let mut reader = frozen.open_reader(Key::ZERO)?;
        let mut segment_ids = HashSet::new();
        while let Some(entry) = reader.next()? {
            let pending = match entry {
                Entry::Pending(_) => true,
                Entry::Tombstone { .. } => false,
            };
            segment_ids.insert((pending, entry.segment_id()));
        }

        let segment_keys = {
            let coordinator = self.coordinator.lock().unwrap();
            segment_ids
                .into_iter()
                .map(|(pending, id)| {
                    (
                        pending,
                        SegmentKey {
                            seq_id: 0,
                            ..coordinator.canonicalize(id)
                        },
                    )
                })
                .collect::<HashSet<_>>()
        };

        let segment_dir = TemporarySegmentDirectory(id).to_path(&self.directory);
        fs::create_dir(&segment_dir)?;

        let mut pending_segment = None;
        let mut tombstone_segments = vec![];

        for (pending, key) in segment_keys {
            debug!(
                "Writing segment {:?}",
                if pending {
                    PendingSegment(key)
                } else {
                    TombstoneSegment(key)
                }
            );

            let range = key.range();
            let reader = FilteredSegmentReader::new(frozen.open_reader(Key::ZERO)?, |e| match e {
                Entry::Pending(_) => pending && range.contains(&e.segment_id()),
                Entry::Tombstone { .. } => !pending && range.contains(&e.segment_id()),
            })?;

            if pending {
                let path = PendingSegment(key).to_path(&segment_dir);
                let seg = ImmutableSegment::write_pending(path, reader, id)?;
                assert!(
                    pending_segment.replace(seg).is_none(),
                    "there should be at most one pending segment"
                );
            } else {
                let path = TombstoneSegment(key).to_path(&segment_dir);
                let seg = ImmutableSegment::write_tombstone(path, reader, id)?;
                tombstone_segments.push(seg);
            };
        }
        Ok((segment_dir, pending_segment, tombstone_segments))
    }

    fn recover(&self) -> Result<()> {
        for entry in self.directory.read_dir()? {
            let path = entry?.path();
            if let Some(QueueFile::WriteAheadLog(id)) = QueueFile::from_path(&path) {
                let start = Instant::now();

                // Remove any existing segment directory
                let segment_dir = TemporarySegmentDirectory(id).to_path(&self.directory);
                if segment_dir.exists() {
                    fs::remove_dir_all(&segment_dir)?;
                }

                let segment = WriteAheadLog::read(&path)?;
                self.write(id, segment.freeze().0)?;

                fs::remove_file(&path)?;

                info!("Recovering WAL {:?} in {:?}", path, start.elapsed());
            }
        }

        for entry in self.directory.read_dir()? {
            let entry = entry?;
            let file = match QueueFile::from_path(entry.path()) {
                Some(file) => file,
                None => continue,
            };

            if let TemporarySegmentDirectory(_) = file {
                for segment_entry in entry.path().read_dir()? {
                    let path = segment_entry?.path();
                    match QueueFile::from_path(&path) {
                        Some(PendingSegment(_)) | Some(TombstoneSegment(_)) => {
                            let mut segment = ImmutableSegment::open(path)?;
                            self.coordinator.lock().unwrap().promote(&mut segment)?;
                        }
                        _ => {}
                    }
                }
                fs::remove_dir_all(entry.path())?;
            }
        }

        Ok(())
    }
}

struct MemorySegmentFlusher {
    shared: Arc<Mutex<Shared>>,
    coordinator: Arc<Mutex<SegmentCoordinator>>,
    writer: MemorySegmentWriter,
    directory: PathBuf,
    compactor: TaskHandle<Compactor>,
    compactor_future: TaskFuture<Compactor>,
}

struct RotatedSegment {
    id: SegmentID,
    segment: FrozenMemorySegment,
    wal: Option<WriteAheadLog>,
}

impl MemorySegmentFlusher {
    fn rotate(&mut self) -> Option<RotatedSegment> {
        let start = Instant::now();
        let mut locked = self.shared.lock().unwrap();
        if locked.force_flush {
            locked.force_flush = false
        } else if locked.current.size() < SEGMENT_FLUSH_THRESHOLD {
            return None;
        }

        let pos = locked.last;
        let id = locked.current_id;
        let new_id = id + 1;
        locked.current_id = new_id;

        let new_wal_path = QueueFile::WriteAheadLog(new_id).to_path(&self.directory);
        let new_wal = WriteAheadLog::new(new_wal_path).unwrap();

        let segment = std::mem::replace(&mut locked.current, MemorySegment::new(Some(new_wal)));
        locked.current_reader = locked.current.open_pending_reader(pos);

        let (segment, wal) = segment.freeze();
        locked
            .immutable
            .add(SegmentKey::singleton(id), segment.clone(), pos)
            .unwrap();
        drop(locked);

        debug!("Rotated current segment in {:?}", start.elapsed());
        Some(RotatedSegment { id, segment, wal })
    }
}

impl SimpleTask for MemorySegmentFlusher {
    fn run(&mut self) -> bool {
        if let Some(segment) = self.rotate() {
            let start = Instant::now();
            match self.writer.write(segment.id, segment.segment) {
                Ok((temp_dir, pending, tombstones)) => {
                    if let Some(wal) = segment.wal {
                        wal.delete().unwrap();
                    }

                    // TODO: Promote segments after deleting WAL.

                    let mut locked = self.shared.lock().unwrap();
                    let key = SegmentKey::singleton(segment.id);
                    locked.immutable.remove(&HashSet::from([key])).unwrap();

                    let pos = locked.last;
                    if let Some(mut seg) = pending {
                        // We must hold the coordinator lock until we've replaced the immutable
                        // segment, otherwise we risk a race condition where the segment is
                        // compacted between being promoted and replaced. This could then result
                        // in us replacing a newer segment with this (now outdated) one.
                        let mut coordinator = self.coordinator.lock().unwrap();
                        coordinator.promote(&mut seg).unwrap();
                        locked.immutable.add(key, seg, pos).unwrap();
                    }

                    for mut tombstone in tombstones {
                        self.coordinator
                            .lock()
                            .unwrap()
                            .promote(&mut tombstone)
                            .unwrap();
                    }

                    fs::remove_dir(temp_dir).unwrap();
                    self.compactor.schedule();
                    debug!("Flush completed in {:?}", start.elapsed(),);
                }
                Err(err) => error!("Error writing segment: {}", err),
            }
        }

        false
    }
}

// <Start SegmentID>-<End SegmentID>.<Counter>.pending
// <SegmentID>.<Counter>.tombstone
// <SegmentID>.log
// deleted

type SequenceID = u64;

#[derive(Debug)]
enum QueueFile {
    WriteAheadLog(SegmentID),
    PendingSegment(SegmentKey),
    TombstoneSegment(SegmentKey),
    TemporarySegmentDirectory(SegmentID),
    DeletionMarker,
}

impl QueueFile {
    fn from_path<P: AsRef<Path>>(path: P) -> Option<Self> {
        let name = path.as_ref().file_name()?.to_str()?;
        if name == "deleted" {
            return Some(DeletionMarker);
        }

        let (name, ext) = name.rsplit_once(".")?;
        match ext {
            "pending" => Some(PendingSegment(SegmentKey::from_str(name)?)),
            "tombstone" => Some(TombstoneSegment(SegmentKey::from_str(name)?)),
            "log" => {
                let segment_id = SegmentID::from_str_radix(name, 16).ok()?;
                Some(QueueFile::WriteAheadLog(segment_id))
            }
            "segment" => {
                let segment_id = SegmentID::from_str_radix(name, 16).ok()?;
                Some(TemporarySegmentDirectory(segment_id))
            }
            _ => None,
        }
    }

    fn to_path<P: AsRef<Path>>(&self, directory: P) -> PathBuf {
        let name = match self {
            QueueFile::WriteAheadLog(id) => format!("{:016x}.log", id),
            TemporarySegmentDirectory(id) => format!("{:016x}.segment", id),
            PendingSegment(key) => format!("{}.pending", key),
            TombstoneSegment(key) => format!("{}.tombstone", key),
            DeletionMarker => "deleted".to_string(),
        };

        directory.as_ref().join(name)
    }
}

struct Compactor {
    coordinator: Arc<Mutex<SegmentCoordinator>>,
    shared: Arc<Mutex<Shared>>,
}

impl Task for Compactor {
    type Value = ();
    type Error = Error;

    fn run(&mut self, _: &TaskContext<Compactor>) -> result::Result<State<()>, Error> {
        // 1. Find and delete fully tombstoned segments.

        // 1. Find segment with the most tombstones.
        // 2.

        debug!("Running compactor");

        // Remove "retired" segments. Those that have been superseded by newly compacted segments.
        let paths = self.coordinator.lock().unwrap().retired_segments()?;
        for path in paths {
            info!("Removing retired segment {:?}", &path);
            self.coordinator.lock().unwrap().remove(path)?;
        }

        // Remove "exhausted" segments. Those that have been fully tombstoned.
        let segments = self.coordinator.lock().unwrap().exhausted_segments()?;
        for key in segments {
            let paths = self.coordinator.lock().unwrap().get_by_id(key.start)?;
            for path in paths {
                info!("Removing exhausted segment {:?}", &path);
                self.coordinator.lock().unwrap().remove(path)?;
            }
        }

        // Compaction serves two goals:
        //
        //   1. Limit the total number of files, so we need fewer open file descriptors.
        //   2. Limit the amount of disk space wasted.
        //
        // These goals could be met by simply compacting the entire queue into a single pending
        // segment, but this is also IO intensive since every file must be read, and every pending
        // item rewritten on disk. To balance optimal compaction with IO requirements, we introduce
        // a third goal,
        //
        //   3. Limit the amount of data written during a compaction.
        //
        // For the purpose of calculating an optimum compaction set, we minimize the L2-norm of the
        // following variables,
        //
        //   1. The percent of files remaining after compaction.
        //   2. The percent of pending items remaining.
        //   3. The percent of pending items rewritten.
        //
        const FILE_COUNT_THRESHOLD: usize = 16;
        const OCCUPANCY_THRESHOLD: f64 = 0.6;

        let stats = self.coordinator.lock().unwrap().statistics()?;
        let total_files: usize = stats.iter().map(|s| s.files.len()).sum();
        let total_pending: u64 = stats.iter().map(|s| s.pending_count).sum();
        let total_tombstone: u64 = stats.iter().map(|s| s.tombstone_count).sum();
        let occupancy = ((total_pending - total_tombstone) as f64) / (total_pending as f64);

        debug!(
            "Compaction run; occupancy={:.2}, file_count={}",
            occupancy, total_files
        );

        if occupancy >= OCCUPANCY_THRESHOLD && total_files <= FILE_COUNT_THRESHOLD {
            return Ok(State::Idle);
        }

        let start = Instant::now();
        let mut best = None;

        for i in 0..stats.len() {
            for j in (i + 1)..=stats.len() {
                let num_files: usize = stats[i..j].iter().map(|s| s.files.len()).sum();
                let num_pending: u64 = stats[i..j].iter().map(|s| s.pending_count).sum();
                let num_tombstone: u64 = stats[i..j].iter().map(|s| s.tombstone_count).sum();

                let net_files = total_files - num_files + 1;
                let net_pending = total_pending - num_tombstone;
                let net_tombstone = total_tombstone - num_tombstone;
                let pending_written = num_pending - num_tombstone;
                let net_occupancy = ((net_pending - net_tombstone) as f64) / (net_pending as f64);

                if net_files > FILE_COUNT_THRESHOLD || net_occupancy < OCCUPANCY_THRESHOLD {
                    continue;
                }

                let norm = ((net_files as f64 / total_files as f64).powf(2.0)
                    + (net_pending as f64 / total_pending as f64).powf(2.0)
                    + (pending_written as f64 / total_pending as f64).powf(2.0))
                .sqrt();

                debug!(
                    "Compaction score: {} -> {:.4}",
                    SegmentKey::merge(stats[i..j].iter().flat_map(|s| s.keys.iter())),
                    norm
                );

                if match best {
                    None => true,
                    Some((_, _, score)) if norm < score => true,
                    _ => false,
                } {
                    best = Some((i, j, norm));
                }
            }
        }

        if let Some((i, j, _)) = best {
            let inputs = stats[i..j]
                .iter()
                .flat_map(|s| s.files.iter())
                .collect::<Vec<_>>();

            let key = SegmentKey::merge(stats[i..j].iter().flat_map(|s| s.keys.iter()));

            debug!("Compacting {} files into {}", inputs.len(), key,);
            for input in &inputs {
                debug!("  {:?}", input);
            }

            let mut merged = MergedSegmentReader::new();
            for (i, input) in inputs.iter().enumerate() {
                merged.add(i, ImmutableSegment::open(input)?, Key::ZERO)?;
            }

            let directory = self.coordinator.lock().unwrap().directory.clone();
            let output = QueueFile::PendingSegment(key).to_path(&directory);
            let mut segment = ImmutableSegment::write_pending(output, merged, key.start)?;
            self.coordinator.lock().unwrap().promote(&mut segment)?;

            debug!("Compaction completed in {:?}", start.elapsed());

            let to_remove = stats[i..j]
                .iter()
                .flat_map(|s| s.keys.iter().cloned())
                .collect::<FxHashSet<_>>();

            let start = Instant::now();
            let mut shared = self.shared.lock().unwrap();
            let pos = shared.last;
            shared.immutable.remove(&to_remove)?;
            shared.immutable.add(key, segment, pos)?;
            drop(shared);
            debug!("Compaction replacement completed in {:?}", start.elapsed());

            Ok(State::Runnable)
        } else {
            Ok(State::Idle)
        }
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
struct SegmentKey {
    start: SegmentID,
    end: SegmentID,
    seq_id: SequenceID,
}

impl SegmentKey {
    fn new(start: SegmentID, end: SegmentID, seq_id: SequenceID) -> SegmentKey {
        SegmentKey { start, end, seq_id }
    }

    fn singleton(id: SegmentID) -> SegmentKey {
        Self::new(id, id, 0)
    }

    fn merge<'a, Iter: Iterator<Item = &'a SegmentKey>>(keys: Iter) -> Self {
        let mut result: Option<Self> = None;
        for key in keys {
            if let Some(mut result) = result.as_mut() {
                result.start = result.start.min(key.start);
                result.end = result.end.max(key.end);
                result.seq_id = result.seq_id.max(key.seq_id);
            } else {
                result = Some(*key);
            }
        }
        result.unwrap()
    }

    fn range(self) -> RangeInclusive<SegmentID> {
        self.start..=self.end
    }

    fn from_str(s: &str) -> Option<SegmentKey> {
        let (range, seq_id) = s.split_once(".")?;
        let (start, end) = range.split_once("-")?;

        let start = SegmentID::from_str_radix(start, 16).ok()?;
        let end = SegmentID::from_str_radix(end, 16).ok()?;
        let seq_id = SequenceID::from_str_radix(seq_id, 16).ok()?;

        Some(SegmentKey { start, end, seq_id })
    }
}

impl Display for SegmentKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:016x}-{:016x}.{:016x}",
            self.start, self.end, self.seq_id
        )
    }
}

#[derive(Default)]
struct SegmentMap {
    map: BTreeMap<SegmentID, SegmentKey>,
}

impl SegmentMap {
    fn new() -> Self {
        Self {
            map: Default::default(),
        }
    }

    fn insert(&mut self, key: SegmentKey) {
        // Handle the case where this range is encapsulated by a larger range.
        if let Some((_, other)) = self.map.range(..=key.start).last() {
            // Make sure the range does not end before ours starts.
            if other.start < key.start && other.end >= key.start {
                // This range should encapsulate our range, and thus our range should
                // be dropped in favor of this one. Make sure this is so and return.
                assert!(other.end > key.end);
                assert!(other.seq_id > key.seq_id);
                assert_eq!(self.map.range(key.range()).count(), 0);
                return;
            }
        }

        // No range encapsulate us. We are either inserting a brand new range, replacing
        // one or more ranges that span our range, or returning because there is an existing
        // range matching us, but with a later sequence ID.
        let overlap = self
            .map
            .range(key.range())
            .map(|(_, key)| *key)
            .collect::<Vec<_>>();

        match overlap.len() {
            0 => {
                self.map.insert(key.start, key);
            }
            1 => {
                let other = &overlap[0];
                assert_eq!(other.range(), key.range());
                if key.seq_id > other.seq_id {
                    self.map.insert(key.start, key);
                }
            }
            _ => {
                assert_eq!(overlap[0].start, key.start);
                assert_eq!(overlap.last().unwrap().end, key.end);
                assert!(overlap.iter().map(|key| key.seq_id).max().unwrap() <= key.seq_id);
                for other in overlap {
                    self.map.remove(&other.start);
                }
                self.map.insert(key.start, key);
            }
        }
    }

    fn get(&self, id: SegmentID) -> Option<SegmentKey> {
        if let Some((_, key)) = self.map.range(..=id).last() {
            if key.range().contains(&id) {
                return Some(*key);
            }
        }
        None
    }

    fn remove(&mut self, key: SegmentKey) {
        if let Some(actual) = self.map.get(&key.start) {
            if *actual == key {
                self.map.remove(&key.start);
            }
        }
    }

    fn keys(&self) -> impl Iterator<Item = SegmentKey> + '_ {
        self.map.values().cloned()
    }
}

struct SegmentStatistics {
    keys: FxHashSet<SegmentKey>,
    files: Vec<PathBuf>,
    pending_count: u64,
    tombstone_count: u64,
}

struct SegmentCoordinator {
    directory: PathBuf,
    next_seq_id: SequenceID,
    pending_segments: SegmentMap,
}

// Writing a log:
// 1. Choose next file name, <Dir>/<SegmentID>.log
// 2. Write, close, delete...

// Write segment from log:
// 1. Choose path for "Pending|Tombstone segment <Start>-<End> for log <SegmentID>.log.

impl SegmentCoordinator {
    fn open<P: AsRef<Path>>(directory: P) -> Result<SegmentCoordinator> {
        let directory = directory.as_ref().to_path_buf();
        let mut coordinator = SegmentCoordinator {
            directory,
            next_seq_id: 1,
            pending_segments: SegmentMap::new(),
        };

        let mut pending = vec![];
        for entry in coordinator.directory.read_dir()? {
            let path = entry?.path();
            match QueueFile::from_path(&path) {
                Some(PendingSegment(key)) => pending.push(key),
                Some(TombstoneSegment(key)) => {
                    coordinator.next_seq_id = coordinator.next_seq_id.max(key.seq_id + 1);
                }
                _ => {}
            }
        }

        // Process in increasing order of range size and sequence ID. This ensures
        // compacted segments will replace their constituent parts, and that the latest
        // copy of each segment is preferred.
        pending.sort_by_key(|key| (key.end - key.start, key.seq_id));
        for key in pending {
            coordinator.pending_segments.insert(key);
            coordinator.next_seq_id = coordinator.next_seq_id.max(key.seq_id + 1);
        }

        Ok(coordinator)
    }

    fn retired_segments(&self) -> Result<Vec<PathBuf>> {
        let mut superseded = vec![];
        debug!("Search for retired segments...");
        for key in self.pending_segments.keys() {
            debug!("  {}.pending", key);
        }
        for entry in self.directory.read_dir()? {
            let path = entry?.path();
            match QueueFile::from_path(&path) {
                Some(PendingSegment(key)) => match self.pending_segments.get(key.start) {
                    Some(parent) if parent == key => {
                        debug!("Segment not retired yet; segment={:?}", &path)
                    }
                    _ => superseded.push(path),
                },
                Some(TombstoneSegment(key)) => match self.pending_segments.get(key.start) {
                    Some(parent) if key.seq_id <= parent.seq_id => superseded.push(path),
                    _ => {
                        debug!("Segment not retired yet; segment={:?}", &path)
                    }
                },
                _ => {}
            }
        }
        Ok(superseded)
    }

    fn get_by_id(&self, id: SegmentID) -> Result<Vec<PathBuf>> {
        let key = match self.pending_segments.get(id) {
            Some(x) => x,
            None => return Ok(vec![]),
        };

        let mut paths = vec![PendingSegment(key).to_path(&self.directory)];
        for entry in self.directory.read_dir()? {
            let path = entry?.path();
            if let Some(TombstoneSegment(ts_key)) = QueueFile::from_path(&path) {
                if key.range().contains(&ts_key.start) && ts_key.seq_id > key.seq_id {
                    paths.push(path)
                }
            }
        }

        Ok(paths)
    }

    fn exhausted_segments(&self) -> Result<Vec<SegmentKey>> {
        let mut exhausted = vec![];
        for key in self.pending_segments.keys() {
            let mut total_pending = 0;
            let mut total_tombstone = 0;
            for path in self.get_by_id(key.start)? {
                let meta = ImmutableSegment::open(path)?.metadata();
                total_pending += meta.pending_count;
                total_tombstone += meta.tombstone_count;
            }
            if total_pending == total_tombstone {
                exhausted.push(key);
            } else {
                debug!(
                    "Segment not exhausted yet; segment={}, pending={}, tombstone={}",
                    key, total_pending, total_tombstone,
                );
            }
        }
        Ok(exhausted)
    }

    fn next_seq_id(&mut self) -> SequenceID {
        let id = self.next_seq_id;
        self.next_seq_id += 1;
        id
    }

    // Should this also remove superseded files?
    fn promote(&mut self, segment: &mut ImmutableSegment) -> Result<()> {
        let target = match QueueFile::from_path(segment.path()) {
            Some(PendingSegment(key)) => {
                self.pending_segments.insert(key);
                PendingSegment(key)
            }
            Some(TombstoneSegment(key)) => TombstoneSegment(SegmentKey {
                seq_id: self.next_seq_id(),
                ..key
            }),
            _ => panic!("invalid path"),
        };

        let dst = target.to_path(&self.directory);
        segment.rename(dst)?;
        Ok(())
    }

    fn remove<P: AsRef<Path>>(&mut self, path: P) -> Result<()> {
        let path = path.as_ref();
        fs::remove_file(path)?;
        if let Some(PendingSegment(key)) = QueueFile::from_path(path) {
            self.pending_segments.remove(key);
        }
        Ok(())
    }

    fn canonicalize(&self, id: SegmentID) -> SegmentKey {
        self.pending_segments
            .get(id)
            .unwrap_or_else(|| SegmentKey::singleton(id))
    }

    fn live_segments(&self) -> Result<Vec<ImmutableSegment>> {
        let mut segments = vec![];
        for key in self.pending_segments.keys() {
            for path in self.get_by_id(key.start)? {
                segments.push(ImmutableSegment::open(path)?);
            }
        }
        Ok(segments)
    }

    fn statistics(&self) -> Result<Vec<SegmentStatistics>> {
        let mut segments = vec![];
        for key in self.pending_segments.keys() {
            let mut stats = SegmentStatistics {
                keys: Default::default(),
                files: vec![],
                pending_count: 0,
                tombstone_count: 0,
            };
            for path in self.get_by_id(key.start)? {
                let meta = ImmutableSegment::open(&path)?.metadata();
                stats.files.push(path.to_path_buf());
                stats.pending_count += meta.pending_count;
                stats.tombstone_count += meta.tombstone_count;
                match QueueFile::from_path(&path) {
                    Some(QueueFile::PendingSegment(key))
                    | Some(QueueFile::TombstoneSegment(key)) => {
                        stats.keys.insert(key);
                    }
                    _ => {}
                }
            }
            segments.push(stats);
        }

        Ok(segments)
    }

    // Compaction
    // 1. Choose range. Criteria based on proportion of items pending,
    //    and total reclaimable space (estimate).
    // 2. List input files.
    // 3. Write merged output file (which will be a pending segment).
    // 4. Finalize output file and assign name based on input file counters.
    // 5. Replace input files with output file in merged segment reader.
    // 6. Delete input files.
}

impl Queue {
    fn open<P: AsRef<Path>>(
        name: String,
        scheduler: Scheduler,
        timer: timer::Scheduler,
        id_generator: IdGenerator,
        working_set: WorkingSet,
        directory: P,
    ) -> Result<Queue> {
        let directory = directory.as_ref().to_path_buf();
        fs::create_dir_all(&directory)?;

        let coordinator = Arc::new(Mutex::new(SegmentCoordinator::open(&directory)?));
        let writer = MemorySegmentWriter::open(Arc::clone(&coordinator), &directory)?;

        let mut immutable = MergedSegmentReader::new();
        let mut current_id = 0;

        for entry in directory.read_dir()? {
            let entry = entry?;
            if let Some(file) = QueueFile::from_path(entry.file_name()) {
                match file {
                    QueueFile::WriteAheadLog(id) => {
                        let src = match WriteAheadLog::read(entry.path()) {
                            Ok(src) => src,
                            Err(err) => panic!("TODO: {:?}", err),
                        };
                        let reader = src.open_reader(Key::ZERO)?;
                        if reader.peek_key().is_some() {
                            debug!("Writing segment from {:?}...", entry.path());
                            unimplemented!()
                            // TODO:
                            // let path = QueueFile::Segment(id, id, 0).to_path(&directory);
                            // let seg = ImmutableSegment::write(path, reader, id)?;
                            // immutable.add(seg, Key::ZERO)?;
                        }
                        current_id = current_id.max(id + 1);
                        fs::remove_file(entry.path())?;
                    }
                    PendingSegment(key) | TombstoneSegment(key) => {
                        let seg = ImmutableSegment::open(entry.path())?;
                        immutable.add(key, seg, Key::ZERO)?;
                        current_id = current_id.max(key.end + 1);
                    }
                    DeletionMarker => panic!("queue deleted"),
                    TemporarySegmentDirectory(_) => unimplemented!(),
                }
            }
        }

        let wal_path = QueueFile::WriteAheadLog(current_id).to_path(&directory);
        let wal = WriteAheadLog::new(wal_path).unwrap();
        let current = MemorySegment::new(Some(wal));
        let current_reader = current.open_reader(Key::ZERO).unwrap();
        let shared = Arc::new(Mutex::new(Shared {
            current,
            current_reader,
            immutable,
            last: Key::ZERO,
            current_id,
            force_flush: false,
        }));

        let (segment_flusher, segment_flusher_future) = {
            let (compactor, compactor_future) = scheduler.register(Compactor {
                coordinator: coordinator.clone(),
                shared: Arc::clone(&shared),
            });
            scheduler.register(MemorySegmentFlusher {
                coordinator: coordinator.clone(),
                shared: Arc::clone(&shared),
                directory: directory.clone(),
                writer,
                compactor,
                compactor_future,
            })
        };

        let (op_sender, op_receiver) = crossbeam::channel::unbounded();

        let (op_processor, op_processor_future) = {
            let shared = Arc::clone(&shared);
            scheduler.register(OpProcessor {
                op_receiver,
                shared,
                coordinator,
                working_set,
                working_set_ids: Default::default(),
                id_generator,
                segment_flusher,
                segment_flusher_future,
                blocked_dequeues: BlockedDequeues::new(),
                timer,
                timer_id: None,
            })
        };

        Ok(Queue {
            name,
            op_sender,
            op_processor,
            op_processor_future,
            directory,
        })
    }

    fn enqueue(&self, req: EnqueueReq, resp: Promise<EnqueueResp>) {
        self.op_sender.send(Op::Enqueue(req, resp)).unwrap();
        self.op_processor.schedule();
    }

    fn dequeue(&self, req: DequeueReq, resp: Promise<DequeueResp>) {
        self.op_sender.send(Op::Dequeue(req, resp)).unwrap();
        self.op_processor.schedule();
    }

    fn requeue(&self, req: RequeueReq, resp: Promise<RequeueResp>) {
        self.op_sender.send(Op::Requeue(req, resp)).unwrap();
        self.op_processor.schedule();
    }

    fn release(&self, req: ReleaseReq, resp: Promise<ReleaseResp>) {
        self.op_sender.send(Op::Release(req, resp)).unwrap();
        self.op_processor.schedule();
    }

    fn info(&self, req: InfoReq, resp: Promise<InfoResp>) {
        self.op_sender.send(Op::Info(req, resp)).unwrap();
        self.op_processor.schedule();
    }

    fn peek(&self, req: PeekReq, resp: Promise<PeekResp>) {
        self.op_sender.send(Op::Peek(req, resp)).unwrap();
        self.op_processor.schedule();
    }

    fn mark_deleted(&self) -> Result<()> {
        File::create(self.directory.join("deleted"))?;
        Ok(())
    }

    fn delete(&self, req: DeleteReq, resp: Promise<DeleteResp>) {
        self.op_sender.send(Op::Delete(req, resp)).unwrap();
        self.op_processor.schedule();
    }

    fn compact(&self, req: CompactReq, resp: Promise<CompactResp>) {
        self.op_sender.send(Op::Compact(req, resp)).unwrap();
        self.op_processor.schedule();
    }
}

#[derive(Clone)]
pub struct Qrono {
    inner: Arc<Mutex<Inner>>,
    queues: Arc<DashMap<String, Queue, BuildHasherDefault<FxHasher>>>,
    queues_directory: PathBuf,
    working_set: WorkingSet,
    deletion_scheduler: Scheduler,
    deletion_backlog: Arc<AtomicUsize>,
}

struct Inner {
    scheduler: Scheduler,
    timer: timer::Scheduler,
    id_generator: IdGenerator,
}

#[derive(Debug)]
pub enum Error {
    NoSuchQueue,
    NoItemReady,
    ItemNotDequeued,
    Internal(&'static str),
    IOError(io::Error, Backtrace),
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Self::IOError(err, Backtrace::new())
    }
}

impl From<ReadError> for Error {
    fn from(err: ReadError) -> Self {
        match err {
            ReadError::Truncated => Error::Internal("log truncated"),
            ReadError::Checksum => Error::Internal("log checksum failure"),
            ReadError::IO(err) => Error::from(err),
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::NoSuchQueue => write!(f, "no such queue"),
            Error::NoItemReady => write!(f, "no item ready"),
            Error::ItemNotDequeued => write!(f, "item not dequeued"),
            Error::Internal(err) => write!(f, "internal: {}", err),
            Error::IOError(err, backtrace) => write!(f, "io error: {}\n{:?}", err, backtrace),
        }
    }
}

pub type Result<T> = result::Result<T, Error>;
pub type Future<T> = promise::Future<Result<T>>;
pub type Promise<T> = promise::Promise<Result<T>>;

impl Qrono {
    // Queue directory
    //  *.seg
    //  *.log
    //  deleted

    // Data directory
    //  queues/
    //  working/
    //  id

    // Queues directory
    //  <name>-<counter>/
    //  counter

    pub fn new<P: AsRef<Path>>(
        scheduler: Scheduler,
        timer: timer::Scheduler,
        id_generator: IdGenerator,
        working_set: WorkingSet,
        queues_directory: P,
        deletion_scheduler: Scheduler,
    ) -> Qrono {
        let inner = Arc::new(Mutex::new(Inner {
            scheduler: scheduler.clone(),
            timer: timer.clone(),
            id_generator: id_generator.clone(),
        }));

        let queues_directory = queues_directory.as_ref();
        fs::create_dir_all(queues_directory).expect("TODO: Handle error");

        let qrono = Qrono {
            inner,
            queues: Arc::new(Default::default()),
            queues_directory: queues_directory.to_path_buf(),
            working_set: working_set.clone(),
            deletion_scheduler,
            deletion_backlog: Default::default(),
        };

        for entry in queues_directory.read_dir().expect("TODO: Handle errors") {
            let entry = entry.expect("TODO");
            if DeletionMarker.to_path(entry.path()).exists() {
                continue;
            }

            let name = entry
                .file_name()
                .to_str()
                .expect("TODO")
                .rsplit_once("-")
                .expect("TODO")
                .0
                .to_string();

            debug!("Opening queue {:?}...", name);

            let queue = Queue::open(
                name.clone(),
                scheduler.clone(),
                timer.clone(),
                id_generator.clone(),
                working_set.clone(),
                entry.path(),
            )
            .expect("TODO: Handle error");

            qrono.queues.insert(name, queue);
        }

        qrono
    }

    pub fn enqueue(&self, queue_name: &str, req: EnqueueReq, resp: Promise<EnqueueResp>) {
        let queue = match self.queues.get(queue_name) {
            None => {
                let inner = self.inner.lock().unwrap();
                match self.queues.get(queue_name) {
                    None => {
                        let scheduler = inner.scheduler.clone();
                        let timer = inner.timer.clone();
                        let id_generator = inner.id_generator.clone();
                        let queue_id = id_generator.generate_id();
                        let queue_path = self
                            .queues_directory
                            .join(format!("{}-{}", queue_name, queue_id));

                        let queue = match Queue::open(
                            queue_name.to_string(),
                            scheduler,
                            timer,
                            id_generator,
                            self.working_set.clone(),
                            queue_path,
                        ) {
                            Ok(queue) => queue,
                            Err(err) => {
                                error!("Error creating queue: {}", err);
                                return resp.complete(Err(Error::Internal("error creating queue")));
                            }
                        };

                        self.queues.insert(queue_name.to_string(), queue);
                        self.queues.get(queue_name).unwrap()
                    }
                    Some(queue) => queue,
                }
            }
            Some(queue) => queue,
        };

        queue.enqueue(req, resp)
    }

    pub fn dequeue(&self, queue: &str, req: DequeueReq, resp: Promise<DequeueResp>) {
        if let Some(queue) = self.queues.get(queue) {
            return queue.dequeue(req, resp);
        }
        resp.complete(Err(Error::NoSuchQueue))
    }

    pub fn requeue(&self, queue: &str, req: RequeueReq, resp: Promise<RequeueResp>) {
        if let Some(queue) = self.queues.get(queue) {
            return queue.requeue(req, resp);
        }
        resp.complete(Err(Error::NoSuchQueue))
    }

    pub fn release(&self, queue: &str, req: ReleaseReq, resp: Promise<ReleaseResp>) {
        if let Some(queue) = self.queues.get(queue) {
            return queue.release(req, resp);
        }
        resp.complete(Err(Error::NoSuchQueue))
    }

    pub fn info(&self, queue: &str, req: InfoReq, resp: Promise<InfoResp>) {
        if let Some(queue) = self.queues.get(queue) {
            return queue.info(req, resp);
        }
        resp.complete(Err(Error::NoSuchQueue))
    }

    pub fn peek(&self, queue: &str, req: PeekReq, resp: Promise<PeekResp>) {
        if let Some(queue) = self.queues.get(queue) {
            return queue.peek(req, resp);
        }
        resp.complete(Err(Error::NoSuchQueue))
    }

    pub fn delete(&self, queue_name: &str, req: DeleteReq, resp: Promise<DeleteResp>) {
        let inner = self.inner.lock().unwrap();
        let queue = if let Some(queue) = self.queues.get(queue_name) {
            if let Err(err) = queue.mark_deleted() {
                error!("Error marking queue deleted: {}", err);
                return resp.complete(Err(Error::Internal("error marking queue deleted")));
            }

            drop(queue);
            self.queues.remove(queue_name).unwrap().1
        } else {
            return resp.complete(Err(Error::NoSuchQueue));
        };

        let deletion_scheduler = self.deletion_scheduler.clone();
        let deletion_backlog = Arc::clone(&self.deletion_backlog);
        deletion_backlog.fetch_add(1, Ordering::SeqCst);

        let (promise, future) = Future::transferable();
        queue.delete(req, promise);
        future.transfer_async(&inner.scheduler, move |_| {
            let start = Instant::now();

            // The queue is now idle. We can cancel the OpProcessor and retake ownership of it.
            queue.op_processor.cancel();
            let op_processor = queue.op_processor_future.take().0;

            // Wait for the segment write to stop. Stopping the segment writer avoids a segment
            // being written at the same time we are deleting the queue's directory. Waiting also
            // ensures the queue will be dropped (and the associated files closed) before we
            // enqueue the slower cleanup task below.
            op_processor.segment_flusher.cancel();
            // Wait for the writer to stop, but ignore the result; a "Canceled" error is to be
            // expected.
            let segment_flusher = op_processor.segment_flusher_future.take().0;

            // Cancel compactor as well.
            segment_flusher.compactor.cancel();
            let _ = segment_flusher.compactor_future.take();

            // Move out fields from the Queue and OpProcessor that we still need
            let name = queue.name;
            let directory = queue.directory;
            let working_set = op_processor.working_set;
            let mut working_set_ids = op_processor.working_set_ids;

            // The remaining cleanup steps may take a while so execute them on a separate executor.
            deletion_scheduler.spawn(move || {
                // Release all items from the working set
                for id in working_set_ids.drain(..) {
                    working_set
                        .get(id)
                        .expect("FIXME: Error retrieving working set entry")
                        .expect("BUG: working_set_ids inconsistent with working_set")
                        .release();
                }

                let dir = directory.into_os_string();
                let dir_deleted = {
                    let mut dir = dir.clone();
                    dir.push(".deleted");
                    dir
                };

                fs::rename(&dir, &dir_deleted).expect("FIXME: Error renaming queue directory");
                fs::remove_dir_all(&dir_deleted).expect("FIXME: Error delete queue directory");
                let outstanding = deletion_backlog.fetch_sub(1, Ordering::SeqCst) - 1;
                let duration = Instant::now() - start;
                debug!(
                    "Deleted {:?} in {:?}; {} queues left to delete",
                    name, duration, outstanding
                );
            });
        });

        resp.complete(Ok(()))
    }

    pub fn compact(&self, queue: &str, req: CompactReq, resp: Promise<CompactResp>) {
        if let Some(queue) = self.queues.get(queue) {
            return queue.compact(req, resp);
        }
        resp.complete(Err(Error::NoSuchQueue))
    }

    pub fn list(&self) -> Vec<String> {
        self.queues.iter().map(|e| e.key().to_string()).collect()
    }
}

mod blocked_dequeues {
    use crate::data::Timestamp;
    use crate::ops::DequeueResp;
    use crate::service::slab_deque::SlabDeque;
    use crate::service::Promise;
    use slab::Slab;
    use std::collections::BTreeSet;

    #[derive(Default)]
    pub struct BlockedDequeues {
        by_arrival: SlabDeque<(Timestamp, u64, Promise<DequeueResp>)>,
        by_deadline: BTreeSet<(Timestamp, usize)>,
    }

    impl BlockedDequeues {
        pub fn new() -> Self {
            Self::default()
        }

        pub fn push_back(&mut self, timeout: Timestamp, count: u64, resp: Promise<DequeueResp>) {
            let key = self.by_arrival.push_back((timeout, count, resp));
            self.by_deadline.insert((timeout, key));
        }

        pub fn pop_front(&mut self) -> Option<(Timestamp, u64, Promise<DequeueResp>)> {
            match self.by_arrival.pop_front() {
                Some((key, (timeout, count, resp))) => {
                    self.by_deadline.remove(&(timeout, key));
                    Some((timeout, count, resp))
                }
                None => None,
            }
        }

        pub fn front(&mut self) -> Option<(Timestamp, u64, &Promise<DequeueResp>)> {
            match self.by_arrival.front() {
                Some((_, (timeout, count, resp))) => Some((*timeout, *count, resp)),
                None => None,
            }
        }

        pub fn expire(&mut self, now: Timestamp) -> Vec<(Timestamp, u64, Promise<DequeueResp>)> {
            let removed = self
                .by_deadline
                .range(..(now, usize::MAX))
                .cloned()
                .collect::<Vec<_>>();

            let mut expired = vec![];
            for (timeout, key) in removed {
                expired.push(self.by_arrival.remove(key).unwrap());
                self.by_deadline.remove(&(timeout, key));
            }
            expired
        }

        pub fn next_timeout(&self) -> Option<Timestamp> {
            self.by_deadline.iter().next().map(|(t, _)| *t)
        }
    }
}

mod slab_deque {
    use std::mem;

    /// An optional pointer to a entry. Roughly equivalent to `Option<usize>`
    /// without the discriminant overhead.
    #[derive(Copy, Clone, Debug, Default)]
    struct Pointer(usize);

    impl Pointer {
        #[inline(always)]
        const fn none() -> Pointer {
            Pointer(0)
        }

        fn get(self) -> Option<usize> {
            match self.0 {
                0 => None,
                val => Some(val - 1),
            }
        }

        fn is_none(self) -> bool {
            self.0 == 0
        }
    }

    impl From<Pointer> for Option<usize> {
        fn from(val: Pointer) -> Self {
            val.get()
        }
    }

    impl From<Option<usize>> for Pointer {
        fn from(val: Option<usize>) -> Self {
            match val {
                None => Pointer(0),
                Some(val) => Pointer(val + 1),
            }
        }
    }

    impl From<usize> for Pointer {
        fn from(val: usize) -> Self {
            Some(val).into()
        }
    }

    struct Occupied<T> {
        val: T,
        next: Pointer,
        prev: Pointer,
    }

    struct Free {
        next: Pointer,
    }

    enum Slot<T> {
        Occupied(Occupied<T>),
        Free(Free),
    }

    impl<T> Slot<T> {
        fn unwrap_occupied(self) -> Occupied<T> {
            match self {
                Slot::Occupied(occupied) => occupied,
                Slot::Free(_) => panic!(),
            }
        }

        fn unwrap_occupied_mut(&mut self) -> &mut Occupied<T> {
            match self {
                Slot::Occupied(occupied) => occupied,
                Slot::Free(_) => panic!(),
            }
        }

        fn unwrap_occupied_ref(&self) -> &Occupied<T> {
            match self {
                Slot::Occupied(occupied) => occupied,
                Slot::Free(_) => panic!(),
            }
        }

        fn unwrap_free_mut(&mut self) -> &mut Free {
            match self {
                Slot::Free(free) => free,
                Slot::Occupied(_) => panic!(),
            }
        }
    }

    impl<T> Default for Slot<T> {
        fn default() -> Self {
            Slot::Free(Free {
                next: Pointer::none(),
            })
        }
    }

    pub struct SlabDeque<T> {
        slots: Vec<Slot<T>>,
        head: Pointer,
        tail: Pointer,
        free: Pointer,
        len: usize,
    }

    impl<T> Default for SlabDeque<T> {
        fn default() -> Self {
            SlabDeque {
                slots: vec![],
                head: Pointer::none(),
                tail: Pointer::none(),
                free: Pointer::none(),
                len: 0,
            }
        }
    }

    impl<T> SlabDeque<T> {
        pub fn new() -> Self {
            Self::default()
        }

        pub fn push_back(&mut self, val: T) -> usize {
            let idx = if let Some(idx) = self.free.get() {
                let slot = &mut self.slots[idx];
                self.free = slot.unwrap_free_mut().next;
                idx
            } else {
                self.slots.push(Slot::default());
                self.slots.len() - 1
            };

            let pointer = Pointer::from(idx);
            let occupied = Occupied {
                val,
                prev: self.tail,
                next: Pointer::none(),
            };

            if let Some(idx) = self.tail.get() {
                let tail = self.slots[idx].unwrap_occupied_mut();
                tail.next = pointer;
            }

            if occupied.prev.is_none() {
                self.head = pointer;
            }

            self.tail = pointer;
            self.slots[idx] = Slot::Occupied(occupied);
            self.len += 1;
            idx
        }

        pub fn front(&self) -> Option<(usize, &T)> {
            if let Some(idx) = self.head.get() {
                Some((idx, &self.slots[idx].unwrap_occupied_ref().val))
            } else {
                None
            }
        }

        pub fn pop_front(&mut self) -> Option<(usize, T)> {
            if let Some(idx) = self.head.get() {
                self.removed_unchecked(idx).map(|val| (idx, val))
            } else {
                None
            }
        }

        pub fn remove(&mut self, idx: usize) -> Option<T> {
            if let Some(Slot::Occupied(_)) = self.slots.get(idx) {
                self.removed_unchecked(idx)
            } else {
                None
            }
        }

        fn removed_unchecked(&mut self, idx: usize) -> Option<T> {
            let free = Slot::Free(Free { next: self.free });
            let slot = mem::replace(&mut self.slots[idx], free);
            self.free = Pointer::from(idx);
            self.len -= 1;

            let occupied = slot.unwrap_occupied();

            if self.len == 0 {
                self.clear();
                return Some(occupied.val);
            }

            if let Some(prev) = occupied.prev.get() {
                let next = self.slots[prev].unwrap_occupied_mut();
                next.next = occupied.next;
            } else {
                self.head = occupied.next;
            }

            if let Some(next) = occupied.next.get() {
                let next = self.slots[next].unwrap_occupied_mut();
                next.prev = occupied.prev;
            } else {
                self.tail = occupied.prev;
            }

            Some(occupied.val)
        }

        fn clear(&mut self) {
            self.slots.clear();
            self.head = Pointer::none();
            self.tail = Pointer::none();
            self.free = Pointer::none();
            self.len = 0;
        }
    }
}
