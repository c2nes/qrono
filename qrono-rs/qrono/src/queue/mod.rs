use crate::channel::batch::Sender;
use crate::data::{Entry, Item, Key, Timestamp};
use crate::id_generator::IdGenerator;
use crate::ops::{
    CompactReq, CompactResp, DeleteReq, DeleteResp, DequeueReq, DequeueResp, EnqueueReq,
    EnqueueResp, InfoReq, InfoResp, PeekReq, PeekResp, ReleaseReq, ReleaseResp, RequeueReq,
    RequeueResp,
};

use crate::promise::{QronoFuture, QronoPromise};
use crate::queue::compactor::Compactor;
use crate::queue::coordinator::SegmentCoordinator;
use crate::queue::filenames::{QueueFile, SegmentKey};
use crate::queue::mutable_segment::MutableSegment;
use crate::queue::operations::{Op, OpProcessor};
use crate::queue::writer::MemorySegmentWriter;
use crate::result::IgnoreErr;
use crate::scheduler::{Scheduler, TaskFuture, TaskHandle, TransferAsync};
use crate::segment::{MergedSegmentReader, SegmentReader};
use crate::timer;
use crate::working_set::WorkingSet;
use log::{debug, trace};
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::ffi::OsStr;
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{fs, io, result};

mod blocked_dequeues;
mod compactor;
mod coordinator;
mod filenames;
mod mutable_segment;
mod operations;
mod segment_set;
mod slab_deque;
mod writer;

pub(crate) struct Queue {
    name: String,
    op_sender: Sender<Op>,
    op_processor: TaskHandle<OpProcessor>,
    op_processor_future: TaskFuture<OpProcessor>,
    directory: PathBuf,

    scheduler: Scheduler,
    deletion_scheduler: Scheduler,
    deletion_backlog: Arc<AtomicUsize>,
}

impl Queue {
    pub(crate) fn is_deleted<P: AsRef<Path>>(directory: P) -> bool {
        directory.as_ref().extension() == Some(OsStr::new("deleted"))
            || QueueFile::DeletionMarker.to_path(directory).exists()
    }

    pub(crate) fn safe_delete<P: AsRef<Path>>(directory: P) -> io::Result<()> {
        let to_delete = if directory.as_ref().extension() == Some(OsStr::new("deleted")) {
            directory.as_ref().to_path_buf()
        } else {
            let mut dir_deleted = directory.as_ref().as_os_str().to_os_string();
            dir_deleted.push(".deleted");
            let dir_deleted = PathBuf::from(dir_deleted);
            fs::rename(directory, &dir_deleted)?;
            dir_deleted
        };

        fs::remove_dir_all(to_delete)
    }

    // TODO: Consider some cleanup options here
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn open<P: AsRef<Path>>(
        name: String,
        scheduler: Scheduler,
        deletion_scheduler: Scheduler,
        deletion_backlog: Arc<AtomicUsize>,
        timer: timer::Scheduler,
        id_generator: IdGenerator,
        working_set: WorkingSet,
        directory: P,
        wal_sync_period: Option<Duration>,
    ) -> io::Result<Queue> {
        let directory = directory.as_ref().to_path_buf();
        fs::create_dir_all(&directory)?;

        let mut files = vec![];
        for entry in directory.read_dir()? {
            if let Some(file) = QueueFile::from_path(entry?.path()) {
                files.push(file);
            }
        }

        if files.contains(&QueueFile::DeletionMarker) {
            panic!("queue deleted")
        }

        let next_segment_id = files
            .iter()
            .filter_map(|file| match file {
                QueueFile::Segment(key) => Some(key.end() + 1),
                QueueFile::WriteAheadLog(id) => Some(id + 1),
                QueueFile::TemporarySegmentDirectory(id) => Some(id + 1),
                _ => None,
            })
            .max()
            .unwrap_or(0);

        let mutable = MutableSegment::open(&directory, next_segment_id, wal_sync_period)?;
        let immutable = MergedSegmentReader::new();
        let shared = Arc::new(Mutex::new(Shared {
            mutable,
            immutable,
            last: Key::ZERO,
            force_flush: false,
        }));

        let coordinator = Arc::new(Mutex::new(SegmentCoordinator::open(
            &directory,
            Arc::clone(&shared),
        )?));

        let (compactor, compactor_future) =
            scheduler.register(Compactor::new(&directory, Arc::clone(&coordinator)));

        let writer = MemorySegmentWriter::open(
            &directory,
            Arc::clone(&shared),
            Arc::clone(&coordinator),
            compactor,
            compactor_future,
        )?;

        // It is important that "files" is built before MutableSegment is opened so
        // the writer does not attempt to "recover" the newly opened WAL.
        writer.recover(&files)?;

        let (op_sender, processor) = OpProcessor::new(
            Arc::clone(&shared),
            &scheduler,
            writer,
            working_set,
            id_generator,
            timer,
        );

        let (op_processor, op_processor_future) = scheduler.register(processor);

        Ok(Queue {
            name,
            op_sender,
            op_processor,
            op_processor_future,
            directory,
            scheduler,
            deletion_scheduler,
            deletion_backlog,
        })
    }

    pub(crate) fn enqueue(&self, req: EnqueueReq, resp: QronoPromise<EnqueueResp>) {
        self.op_sender.send(Op::Enqueue(req, resp));
        self.op_processor.schedule().ignore_err();
    }

    pub(crate) fn dequeue(&self, req: DequeueReq, resp: QronoPromise<DequeueResp>) {
        self.op_sender.send(Op::Dequeue(req, resp));
        self.op_processor.schedule().ignore_err();
    }

    pub(crate) fn requeue(&self, req: RequeueReq, resp: QronoPromise<RequeueResp>) {
        self.op_sender.send(Op::Requeue(req, resp));
        self.op_processor.schedule().ignore_err();
    }

    pub(crate) fn release(&self, req: ReleaseReq, resp: QronoPromise<ReleaseResp>) {
        self.op_sender.send(Op::Release(req, resp));
        self.op_processor.schedule().ignore_err();
    }

    pub(crate) fn info(&self, req: InfoReq, resp: QronoPromise<InfoResp>) {
        self.op_sender.send(Op::Info(req, resp));
        self.op_processor.schedule().ignore_err();
    }

    pub(crate) fn peek(&self, req: PeekReq, resp: QronoPromise<PeekResp>) {
        self.op_sender.send(Op::Peek(req, resp));
        self.op_processor.schedule().ignore_err();
    }

    pub(crate) fn mark_deleted(&self) -> io::Result<()> {
        File::create(self.directory.join("deleted"))?;
        Ok(())
    }

    pub(crate) fn delete(self, req: DeleteReq, resp: QronoPromise<DeleteResp>) {
        let (tx, rx) = QronoFuture::transferable();
        self.op_sender.send(Op::Delete(req, tx));
        self.op_processor.schedule().ignore_err();
        self.deletion_backlog.fetch_add(1, Ordering::SeqCst);

        rx.transfer_async(&self.scheduler, move |result| {
            // Let the caller know that the deletion has completed. We still have
            // cleanup to perform, but this work should be invisible to the client.
            // We wait until this point to complete the future to ensure that any
            // other operations (enqueues, dequeues, etc.) have been completed before
            // completing the deletion operation.
            resp.complete(result);

            // The remaining cleanup steps may take a while so execute them on a separate executor.
            self.deletion_scheduler.spawn(move || {
                let start = Instant::now();

                // The queue is now idle. We can cancel the OpProcessor and retake ownership of it.
                self.op_processor.cancel();
                let op_processor = self.op_processor_future.take().0;
                op_processor.shutdown();

                // Delete queue data from filesystem
                Queue::safe_delete(&self.directory).expect("FIXME: Error delete queue directory");

                let outstanding = self.deletion_backlog.fetch_sub(1, Ordering::SeqCst) - 1;
                let duration = Instant::now() - start;
                debug!(
                    "Deleted {:?} in {:?}; {} queues left to delete",
                    self.name, duration, outstanding
                );
            });
        })
    }

    pub(crate) fn compact(&self, req: CompactReq, resp: QronoPromise<CompactResp>) {
        self.op_sender.send(Op::Compact(req, resp));
        self.op_processor.schedule().ignore_err();
    }
}

#[derive(Debug)]
enum DequeueError {
    Empty,
    Pending(Timestamp),
    IOError(io::Error),
}

impl Display for DequeueError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for DequeueError {}

impl From<io::Error> for DequeueError {
    fn from(err: io::Error) -> Self {
        DequeueError::IOError(err)
    }
}

struct Shared {
    mutable: MutableSegment,
    immutable: MergedSegmentReader<SegmentKey>,
    last: Key,
    force_flush: bool,
}

impl Shared {
    fn head(&mut self) -> Option<(&mut dyn SegmentReader, Key, &'static str)> {
        let key_current = self.mutable.peek_key();
        let key_immutable = self.immutable.peek_key();

        match (key_current, key_immutable) {
            (Some(key_current), Some(key_immutable)) => {
                if key_current < key_immutable {
                    Some((&mut self.mutable, key_current, "current"))
                } else {
                    Some((&mut self.immutable, key_immutable, "immutable"))
                }
            }
            (Some(key), None) => Some((&mut self.mutable, key, "current")),
            (None, Some(key)) => Some((&mut self.immutable, key, "immutable")),
            (None, None) => None,
        }
    }

    fn dequeue_one(&mut self, now: Timestamp) -> result::Result<Item, DequeueError> {
        if let Some((reader, key, head_name)) = self.head() {
            if key.deadline() <= now {
                match reader.next()?.unwrap() {
                    Entry::Pending(item) => {
                        trace!("Dequeued {item:?} from {head_name}");
                        Ok(item)
                    }
                    Entry::Tombstone { id, deadline, .. } => {
                        panic!(
                            "BUG: dequeued tombstone from {head_name}; id={}, deadline={:?}",
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

    fn dequeue(&mut self, now: Timestamp, count: usize) -> (VecDeque<Item>, Option<DequeueError>) {
        let mut items = VecDeque::with_capacity(count);
        while items.len() < count {
            match self.dequeue_one(now) {
                Ok(item) => items.push_back(item),
                Err(err) => return (items, Some(err)),
            }
        }
        (items, None)
    }

    fn peek(&mut self) -> io::Result<Option<Item>> {
        if let Some((reader, _, head_name)) = self.head() {
            match reader.next()?.unwrap() {
                Entry::Pending(item) => Ok(Some(item)),
                Entry::Tombstone { id, deadline, .. } => {
                    panic!(
                        "BUG: peeked tombstone from {head_name}; id={}, deadline={:?}",
                        id, deadline
                    );
                }
            }
        } else {
            Ok(None)
        }
    }
}
