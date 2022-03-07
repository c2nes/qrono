use crate::data::{Key, SegmentID, Stats, Timestamp, ID};
use crate::error::QronoError;
use crate::id_generator::IdGenerator;
use crate::ops::{
    CompactReq, CompactResp, DeleteReq, DeleteResp, DequeueReq, DequeueResp, EnqueueReq,
    EnqueueResp, IdPattern, InfoReq, InfoResp, PeekReq, PeekResp, ReleaseReq, ReleaseResp,
    RequeueReq, RequeueResp,
};
use crate::promise::QronoPromise;
use crate::queue::blocked_dequeues::BlockedDequeues;
use crate::queue::writer::{MemorySegmentWriter, SEGMENT_FLUSH_THRESHOLD};
use crate::queue::{DequeueError, Shared};
use crate::result::IgnoreErr;
use crate::scheduler::{Scheduler, State, Task, TaskContext, TaskFuture, TaskHandle};
use crate::timer;
use crate::working_set::{WorkingItem, WorkingSet};
use indexmap::IndexMap;
use log::{debug, error, trace};
use parking_lot::Mutex;
use qrono_channel::batch;
use qrono_channel::batch::{Receiver, Sender};
use rustc_hash::FxHasher;
use std::hash::BuildHasherDefault;
use std::result;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub(super) enum Op {
    Enqueue(EnqueueReq, QronoPromise<EnqueueResp>),
    Dequeue(DequeueReq, QronoPromise<DequeueResp>),
    Requeue(RequeueReq, QronoPromise<RequeueResp>),
    Release(ReleaseReq, QronoPromise<ReleaseResp>),
    Info(InfoReq, QronoPromise<InfoResp>),
    Peek(PeekReq, QronoPromise<PeekResp>),
    Delete(DeleteReq, QronoPromise<DeleteResp>),
    Compact(CompactReq, QronoPromise<CompactResp>),
}

enum Response {
    Enqueue(
        QronoPromise<EnqueueResp>,
        crate::result::QronoResult<EnqueueResp>,
    ),
    Dequeue(
        QronoPromise<DequeueResp>,
        crate::result::QronoResult<DequeueResp>,
    ),
    Requeue(
        QronoPromise<RequeueResp>,
        crate::result::QronoResult<RequeueResp>,
    ),
    Release(
        QronoPromise<ReleaseResp>,
        crate::result::QronoResult<ReleaseResp>,
    ),
    Info(QronoPromise<InfoResp>, crate::result::QronoResult<InfoResp>),
    Peek(QronoPromise<PeekResp>, crate::result::QronoResult<PeekResp>),
    Delete(
        QronoPromise<DeleteResp>,
        crate::result::QronoResult<DeleteResp>,
    ),
    Compact(
        QronoPromise<CompactResp>,
        crate::result::QronoResult<CompactResp>,
    ),
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

pub(super) struct OpProcessor {
    op_receiver: Receiver<Op>,
    shared: Arc<Mutex<Shared>>,
    working_set: WorkingSet,
    working_set_ids: IndexMap<ID, (Timestamp, SegmentID), BuildHasherDefault<FxHasher>>,
    id_generator: IdGenerator,
    segment_writer: TaskHandle<MemorySegmentWriter>,
    segment_writer_future: TaskFuture<MemorySegmentWriter>,

    blocked_dequeues: BlockedDequeues,
    timer: timer::Scheduler,
    timer_id: Option<timer::ID>,
}

impl OpProcessor {
    pub(super) fn new(
        shared: Arc<Mutex<Shared>>,
        scheduler: &Scheduler,
        writer: MemorySegmentWriter,
        working_set: WorkingSet,
        id_generator: IdGenerator,
        timer: timer::Scheduler,
    ) -> (Sender<Op>, Self) {
        let (op_sender, op_receiver) = batch::channel();
        let (segment_writer, segment_writer_future) = scheduler.register(writer);
        let processor = Self {
            op_receiver,
            shared,
            working_set,
            working_set_ids: Default::default(),
            id_generator,
            segment_writer,
            segment_writer_future,
            blocked_dequeues: BlockedDequeues::new(),
            timer,
            timer_id: None,
        };
        (op_sender, processor)
    }

    pub(super) fn shutdown(mut self) {
        // Wait for the segment write to stop. Stopping the segment writer avoids a segment
        // being written at the same time we are deleting the queue's directory. Waiting also
        // ensures the queue will be dropped (and the associated files closed) before we
        // enqueue the slower cleanup task below.
        self.segment_writer.cancel();

        // Wait for the writer to stop, but ignore the result; a "Canceled" error is to be
        // expected.
        let segment_writer = self.segment_writer_future.take().0;

        // Shut down the writer which will handle gracefully stopping the compactor as well.
        segment_writer.shutdown();

        // Release all items from the working set
        for (id, _) in self.working_set_ids.drain(..) {
            self.working_set
                .get(id)
                .expect("FIXME: Error retrieving working set entry")
                .expect("BUG: working_set_ids inconsistent with working_set")
                .release();
        }
    }
}

impl Task for OpProcessor {
    type Value = ();
    type Error = ();

    fn run(&mut self, ctx: &TaskContext<Self>) -> result::Result<State<()>, ()> {
        let batch = self.op_receiver.recv(256);
        let more_ready = !batch.is_empty();

        /*
        1. Execute read-only operations (peek, info).
        2. Prepare release, requeue, and enqueue operations.
        3. Write entry batch.
        4. Complete release, requeue and enqueue operations.
        5. Execute dequeue operations.
         */

        let mut enqueue_count = 0;
        let mut dequeue_count = 0;
        let mut deleted = false;
        for op in batch.iter() {
            match op {
                Op::Enqueue(_, _) => enqueue_count += 1,
                Op::Dequeue(_, _) => dequeue_count += 1,
                Op::Delete(_, _) => deleted = true,
                _ => {}
            }
        }

        let mut ids = self.id_generator.generate_ids(enqueue_count);
        let now = Timestamp::now();
        let mut dequeues = Vec::with_capacity(dequeue_count);
        let mut working_set_tx = self.working_set.tx();
        let mut responses = Vec::with_capacity(batch.len());

        let mut locked = self.shared.lock();
        let last = locked.last;
        let mut tx = locked.mutable.new_transaction();
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
                    let stats = Stats {
                        enqueue_time: now,
                        ..Default::default()
                    };

                    let val = Ok(EnqueueResp { id, deadline });

                    responses.push(Response::Enqueue(resp, val));
                    tx.add_pending(id, deadline, stats, req.value);
                }
                Op::Dequeue(mut req, resp) => {
                    // TODO: Make this configurable
                    const MAX_BATCH_SIZE: u64 = 100;
                    if req.count > MAX_BATCH_SIZE {
                        req.count = MAX_BATCH_SIZE;
                    }
                    dequeues.push((req, resp))
                }
                Op::Requeue(req, resp) => {
                    let working_entry = match req.id {
                        IdPattern::Any => self.working_set_ids.pop(),
                        IdPattern::Id(id) => self.working_set_ids.remove_entry(&id),
                    };

                    if let Some((id, (deadline, segment_id))) = working_entry {
                        // Push tombstone with original deadline
                        tx.add_tombstone(id, deadline, segment_id);

                        let deadline = adjust_deadline(id, req.deadline.resolve(now));
                        let item_ref = self
                            .working_set
                            .get(id)
                            .expect("TODO: Handle error")
                            .expect("BUG: id in working_set_ids but not in working_set");
                        let WorkingItem {
                            mut stats, value, ..
                        } = item_ref.load().unwrap();
                        stats.dequeue_count += 1;
                        stats.requeue_time = now;
                        tx.add_pending(id, deadline, stats, value);
                        let val = Ok(RequeueResp { deadline });
                        responses.push(Response::Requeue(resp, val));
                        working_set_tx.release(id);
                    } else {
                        let val = Err(QronoError::ItemNotDequeued);
                        responses.push(Response::Requeue(resp, val));
                    }
                }
                Op::Release(req, resp) => {
                    // TODO: Handle duplicate Requeue/Release in batch!
                    let working_entry = match req.id {
                        IdPattern::Any => self.working_set_ids.pop(),
                        IdPattern::Id(id) => self.working_set_ids.remove_entry(&id),
                    };

                    if let Some((id, (deadline, segment_id))) = working_entry {
                        tx.add_tombstone(id, deadline, segment_id);
                        responses.push(Response::Release(resp, Ok(())));
                        working_set_tx.release(id);
                    } else {
                        let val = Err(QronoError::ItemNotDequeued);
                        responses.push(Response::Release(resp, val));
                    }
                }
                Op::Peek(_, resp) => {
                    let val = match locked.peek().unwrap() {
                        Some(item) => Ok(item),
                        None => Err(QronoError::NoItemReady),
                    };

                    responses.push(Response::Peek(resp, val));
                }
                Op::Info(_, resp) => {
                    let val = info_resp.get_or_insert_with(|| {
                        let mut total = locked.mutable.metadata();
                        debug!(
                            "Op::Info: (current) pending={}, tombstone={}",
                            total.pending_count, total.tombstone_count
                        );

                        for (key, segment) in locked.immutable.iter() {
                            let meta = segment.metadata();
                            debug!(
                                "Op::Info: ({}) pending={}, tombstone={}",
                                key, meta.pending_count, meta.tombstone_count
                            );
                            total += meta;
                        }

                        let dequeued = self.working_set_ids.len() as u64;
                        let pending = total.pending_count - total.tombstone_count - dequeued;

                        InfoResp { pending, dequeued }
                    });

                    responses.push(Response::Info(resp, Ok(val.clone())));
                }
                Op::Delete(_, resp) => {
                    responses.push(Response::Delete(resp, Ok(())));
                }
                Op::Compact(_, resp) => {
                    locked.force_flush = true;
                    self.segment_writer.schedule().ignore_err();
                    responses.push(Response::Compact(resp, Ok(())));
                }
            };
        }

        locked.mutable.commit(tx).unwrap();

        // TODO: This threshold should be configurable. We should also
        //   have a size limit on the WAL itself to ensure it does not
        //   grow too large regardless of in-memory size.
        if locked.mutable.size() > SEGMENT_FLUSH_THRESHOLD {
            self.segment_writer.schedule().ignore_err();
        }

        // Dequeue all of the items we need
        let dequeue_total_count = (self.blocked_dequeues.total_count()
            + dequeues.iter().map(|(req, _)| req.count).sum::<u64>())
            as usize;
        let (mut dequeued_items, dequeue_err) = locked.dequeue(now, dequeue_total_count);

        // Remember the most recently dequeued key
        if let Some(item) = dequeued_items.iter().last() {
            locked.last = Key::Pending {
                id: item.id,
                deadline: item.deadline,
            };
        }

        drop(locked);

        // Add all of the items to the working set
        for item in &dequeued_items {
            working_set_tx.add(item);
            self.working_set_ids
                .insert(item.id, (item.deadline, item.segment_id));
        }

        working_set_tx.commit().unwrap();

        // Deliver to blocked dequeues first
        while let Some((_, _, resp)) = self.blocked_dequeues.front() {
            // Drop cancelled dequeues
            if resp.is_cancelled() {
                self.blocked_dequeues.pop_front();
                continue;
            }

            match dequeued_items.pop_front() {
                Some(item) => {
                    let (_, count, resp) = self.blocked_dequeues.pop_front().unwrap();
                    let mut items = vec![item];
                    while items.len() < count as usize {
                        match dequeued_items.pop_front() {
                            Some(item) => items.push(item),
                            None => break,
                        }
                    }
                    responses.push(Response::Dequeue(resp, Ok(items)));
                }
                None => match &dequeue_err {
                    Some(DequeueError::IOError(err)) => {
                        error!("IO error on dequeue: {:?}", err);
                        let (_, _, resp) = self.blocked_dequeues.pop_front().unwrap();
                        responses.push(Response::Dequeue(resp, Err(QronoError::Internal)));
                    }
                    Some(DequeueError::Empty | DequeueError::Pending(_)) => {
                        break;
                    }
                    None => panic!("BUG: too few items in dequeued_items"),
                },
            }
        }

        // Handle new dequeues next
        for (req, resp) in dequeues {
            let val = match dequeued_items.pop_front() {
                Some(item) => {
                    let mut items = vec![item];
                    while items.len() < req.count as usize {
                        match dequeued_items.pop_front() {
                            Some(item) => items.push(item),
                            None => break,
                        }
                    }
                    Ok(items)
                }
                None => match &dequeue_err {
                    Some(DequeueError::IOError(err)) => {
                        error!("IO error on dequeue: {:?}", err);
                        Err(QronoError::Internal)
                    }
                    Some(DequeueError::Empty | DequeueError::Pending(_)) => {
                        if req.timeout.is_zero() {
                            Err(QronoError::NoItemReady)
                        } else {
                            self.blocked_dequeues
                                .push_back(now + req.timeout, req.count, resp);
                            continue;
                        }
                    }
                    None => panic!("BUG: too few items in dequeued_items"),
                },
            };

            responses.push(Response::Dequeue(resp, val));
        }

        // Complete blocked dequeues that have timed out
        for (_, _, resp) in self.blocked_dequeues.expire(now) {
            responses.push(Response::Dequeue(resp, Err(QronoError::NoItemReady)));
        }

        for resp in responses {
            resp.complete();
        }

        let next_timeout = self.blocked_dequeues.next_timeout();
        let next_wakeup = match dequeue_err {
            Some(DequeueError::Pending(deadline)) => match next_timeout {
                Some(timeout) => Some(timeout.min(deadline)),
                None => Some(deadline),
            },
            _ => next_timeout,
        };

        if let Some(deadline) = next_wakeup {
            trace!("Scheduling wake up in {:?}", deadline - now);
            if let Some(id) = self.timer_id.take() {
                self.timer.cancel(id);
            }

            let deadline = Instant::now() + (deadline - now);
            self.timer_id = {
                let ctx = ctx.clone();
                Some(self.timer.schedule(deadline, move || {
                    ctx.schedule().ignore_err();
                }))
            };
        }

        if deleted {
            while let Some((_, _, resp)) = self.blocked_dequeues.pop_front() {
                resp.complete(Err(QronoError::NoItemReady));
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
