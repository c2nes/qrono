use std::hash::BuildHasherDefault;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{fs, io};

use dashmap::mapref::one::Ref;
use dashmap::DashMap;
use log::{debug, error, info, trace};
use parking_lot::Mutex;
use rayon::ThreadPoolBuilder;
use rustc_hash::FxHasher;

use crate::error::QronoError;
use crate::id_generator::IdGenerator;
use crate::ops::{
    CompactReq, CompactResp, DeleteReq, DeleteResp, DequeueReq, DequeueResp, EnqueueReq,
    EnqueueResp, InfoReq, InfoResp, PeekReq, PeekResp, ReleaseReq, ReleaseResp, RequeueReq,
    RequeueResp,
};
use crate::promise::QronoPromise;
use crate::queue::Queue;
use crate::scheduler::Scheduler;
use crate::timer;
use crate::wait_group::WaitGroup;
use crate::working_set::WorkingSet;

type BuildHasher = BuildHasherDefault<FxHasher>;

pub struct Qrono {
    queues: Arc<DashMap<String, Queue, BuildHasher>>,
    queues_directory: PathBuf,
    working_set: WorkingSet,
    id_generator: IdGenerator,
    timer: timer::Scheduler,
    scheduler: Scheduler,
    deletion_scheduler: Scheduler,
    deletion_backlog: Arc<WaitGroup>,
    wal_sync_period: Option<Duration>,
    lock: Arc<Mutex<()>>,
}

impl Qrono {
    pub fn new<P: AsRef<Path>>(
        scheduler: Scheduler,
        timer: timer::Scheduler,
        id_generator: IdGenerator,
        working_set: WorkingSet,
        queues_directory: P,
        deletion_scheduler: Scheduler,
        wal_sync_period: Option<Duration>,
    ) -> Qrono {
        let queues_directory = queues_directory.as_ref();
        fs::create_dir_all(queues_directory).expect("TODO: Handle error");

        let qrono = Qrono {
            queues: Arc::new(Default::default()),
            queues_directory: queues_directory.to_path_buf(),
            working_set,
            id_generator,
            timer,
            scheduler,
            deletion_scheduler,
            deletion_backlog: Default::default(),
            wal_sync_period,
            lock: Default::default(),
        };

        // Open queues in parallel
        rayon::scope(|scope| {
            for entry in queues_directory.read_dir().expect("TODO: Handle errors") {
                let qrono = &qrono;
                scope.spawn(move |_| {
                    let entry = entry.expect("TODO");
                    let path = entry.path();

                    if Queue::is_deleted(&path) {
                        info!("Removing queue previously marked for deletion, {:?}", path);
                        Queue::safe_delete(&path).expect("TODO: Handle error");
                        return;
                    }

                    let name = entry
                        .file_name()
                        .to_str()
                        .expect("TODO")
                        .rsplit_once('-')
                        .expect("TODO")
                        .0
                        .to_string();

                    debug!("Opening queue {:?}...", name);
                    let queue = qrono.open_queue(&name, &path).expect("TODO: Handle error");
                    qrono.queues.insert(name, queue);
                })
            }
        });

        qrono
    }

    pub fn scheduler(&self) -> &Scheduler {
        &self.scheduler
    }

    fn open_queue(&self, queue_name: &str, queue_path: &Path) -> io::Result<Queue> {
        Queue::open(
            queue_name.to_string(),
            self.scheduler.clone(),
            self.deletion_scheduler.clone(),
            self.deletion_backlog.clone(),
            self.timer.clone(),
            self.id_generator.clone(),
            self.working_set.clone(),
            queue_path,
            self.wal_sync_period,
        )
    }

    fn ensure_queue(&self, queue_name: &str) -> io::Result<Ref<String, Queue, BuildHasher>> {
        if let Some(queue) = self.queues.get(queue_name) {
            return Ok(queue);
        }

        let _lock = self.lock.lock();
        if let Some(queue) = self.queues.get(queue_name) {
            return Ok(queue);
        }

        let id_generator = self.id_generator.clone();
        let queue_id = id_generator.generate_id();
        let queue_path = self
            .queues_directory
            .join(format!("{}-{}", queue_name, queue_id));

        let queue = self.open_queue(queue_name, &queue_path)?;
        self.queues.insert(queue_name.to_string(), queue);
        Ok(self.queues.get(queue_name).unwrap())
    }

    pub fn enqueue(&self, queue_name: &str, req: EnqueueReq, resp: QronoPromise<EnqueueResp>) {
        match self.ensure_queue(queue_name) {
            Ok(queue) => {
                queue.enqueue(req, resp);
            }
            Err(err) => {
                error!("Error creating queue: {}", err);
                resp.complete(Err(QronoError::Internal));
            }
        }
    }

    pub fn dequeue(&self, queue: &str, req: DequeueReq, resp: QronoPromise<DequeueResp>) {
        if let Some(queue) = self.queues.get(queue) {
            trace!("dequeue: req={req:?}");
            return queue.dequeue(req, resp);
        }
        trace!("No queue!?!");
        resp.complete(Err(QronoError::NoSuchQueue))
    }

    pub fn requeue(&self, queue: &str, req: RequeueReq, resp: QronoPromise<RequeueResp>) {
        if let Some(queue) = self.queues.get(queue) {
            return queue.requeue(req, resp);
        }
        resp.complete(Err(QronoError::NoSuchQueue))
    }

    pub fn release(&self, queue: &str, req: ReleaseReq, resp: QronoPromise<ReleaseResp>) {
        if let Some(queue) = self.queues.get(queue) {
            return queue.release(req, resp);
        }
        resp.complete(Err(QronoError::NoSuchQueue))
    }

    pub fn info(&self, queue: &str, req: InfoReq, resp: QronoPromise<InfoResp>) {
        if let Some(queue) = self.queues.get(queue) {
            return queue.info(req, resp);
        }
        resp.complete(Err(QronoError::NoSuchQueue))
    }

    pub fn peek(&self, queue: &str, req: PeekReq, resp: QronoPromise<PeekResp>) {
        if let Some(queue) = self.queues.get(queue) {
            return queue.peek(req, resp);
        }
        resp.complete(Err(QronoError::NoSuchQueue))
    }

    pub fn delete(&self, queue_name: &str, req: DeleteReq, resp: QronoPromise<DeleteResp>) {
        // Take lock to avoid race between multiple delete requests which could
        // both see the existing queue and attempt to delete it.
        let _lock = self.lock.lock();
        let queue = if let Some(queue) = self.queues.get(queue_name) {
            if let Err(err) = queue.mark_deleted() {
                error!("Error marking queue deleted: {}", err);
                return resp.complete(Err(QronoError::Internal));
            }

            drop(queue);
            self.queues.remove(queue_name).unwrap().1
        } else {
            return resp.complete(Err(QronoError::NoSuchQueue));
        };

        queue.delete(req, resp);
    }

    pub fn compact(&self, queue: &str, req: CompactReq, resp: QronoPromise<CompactResp>) {
        if let Some(queue) = self.queues.get(queue) {
            return queue.compact(req, resp);
        }
        resp.complete(Err(QronoError::NoSuchQueue))
    }

    pub fn poke(&self, queue: &str) {
        if let Some(queue) = self.queues.get(queue) {
            queue.poke();
        }
    }

    pub fn list(&self) -> Vec<String> {
        self.queues.iter().map(|e| e.key().to_string()).collect()
    }
}

impl Drop for Qrono {
    fn drop(&mut self) {
        // Wait for any in-progress deletions to complete
        self.deletion_backlog.wait();
        // Close queues in parallel
        ThreadPoolBuilder::new().build().unwrap().scope(|s| {
            let queue_names: Vec<_> = self.queues.iter().map(|r| r.key().clone()).collect();
            for queue_name in queue_names {
                if let Some((queue_name, queue)) = self.queues.remove(&queue_name) {
                    s.spawn(move |_| {
                        let start = Instant::now();
                        info!(r#"Closing queue "{queue_name}"..."#);
                        queue.shutdown();
                        let elapsed = start.elapsed();
                        info!(r#"Closed queue "{queue_name}" in {elapsed:?}"#);
                    });
                }
            }
        });
    }
}
