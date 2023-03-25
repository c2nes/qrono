use std::collections::hash_map::DefaultHasher;
use std::collections::VecDeque;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::hash::Hasher;
use std::path::Path;
use std::sync::Once;
use std::time::Duration;

use qrono::error::QronoError;
use rand::seq::SliceRandom;
use rand::RngCore;
use tempfile::tempdir;

use qrono::bytes::Bytes;
use qrono::data::{Timestamp, ID};
use qrono::id_generator::IdGenerator;
use qrono::ops::IdPattern::Id;
use qrono::ops::{
    DeadlineReq, DeleteReq, DeleteResp, DequeueReq, DequeueResp, EnqueueReq, EnqueueResp, InfoReq,
    InfoResp, PeekReq, PeekResp, ReleaseReq, ReleaseResp, RequeueReq, RequeueResp, ValueReq,
};
use qrono::promise::QronoFuture;
use qrono::scheduler::{Scheduler, StaticPool};
use qrono::service::Qrono;
use qrono::timer;
use qrono::working_set::WorkingSet;

mod test_alloc {
    use jemallocator::usable_size;
    use qrono::{alloc::QronoAllocator, scheduler::Spawn};
    use std::{
        alloc::{GlobalAlloc, Layout},
        cell::Cell,
        collections::{hash_map::Entry, HashMap},
        sync::{
            atomic::{AtomicUsize, Ordering::Relaxed},
            Mutex,
        },
    };

    #[global_allocator]
    static GLOBAL: TestAlloc = TestAlloc(QronoAllocator::new());

    thread_local! {
        static THREAD_ID: Cell<usize> = Cell::new(0);
        static GUARD: Cell<bool> = Cell::new(false);
    }

    static NEXT_ID: AtomicUsize = AtomicUsize::new(1);
    static ALLOCS: Mutex<Option<HashMap<usize, isize>>> = Mutex::new(None);

    struct TestAlloc(QronoAllocator);

    impl TestAlloc {
        fn record_delta(&self, size: isize) {
            guard(|| {
                let id = THREAD_ID.with(|v| v.get());
                with_allocs(|allocs| {
                    if allocs.is_none() {
                        *allocs = Some(HashMap::new());
                    }
                    match allocs.as_mut().unwrap().entry(id) {
                        Entry::Occupied(mut e) => {
                            e.insert(*e.get() + size);
                        }
                        Entry::Vacant(e) => {
                            e.insert(size);
                        }
                    }
                })
            });
        }

        fn add(&self, size: usize) {
            self.record_delta(size as isize)
        }

        fn sub(&self, size: usize) {
            self.record_delta(-(size as isize))
        }
    }

    fn guard<F: FnOnce()>(f: F) {
        if THREAD_ID.with(|v| v.get()) == 0 {
            return;
        }
        GUARD.with(|v| {
            if !v.replace(true) {
                f();
                v.set(false);
            }
        })
    }

    unsafe impl GlobalAlloc for TestAlloc {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let res = self.0.alloc(layout);
            if !res.is_null() {
                self.add(usable_size(res));
            }
            res
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            self.sub(usable_size(ptr));
            self.0.dealloc(ptr, layout)
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            let res = self.0.alloc_zeroed(layout);
            if !res.is_null() {
                self.add(usable_size(res));
            }
            res
        }

        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            let old_size = usable_size(ptr);
            let res = self.0.realloc(ptr, layout, new_size);
            if !res.is_null() {
                self.add(usable_size(res));
                self.sub(old_size);
            }
            res
        }
    }

    fn with_allocs<F: FnOnce(&mut Option<HashMap<usize, isize>>) -> R, R>(f: F) -> R {
        let mut allocs = ALLOCS.lock().unwrap();
        f(&mut allocs)
    }

    pub fn allocated() -> isize {
        with_allocs(|allocs| match allocs.as_ref() {
            Some(allocs) => {
                let id = THREAD_ID.with(|v| v.get());
                allocs.get(&id).copied().unwrap_or(0)
            }
            None => 0,
        })
    }

    pub fn wrap_spawn<S: Spawn>(spawn: S) -> AllocTrackingSpawn<S> {
        let id = THREAD_ID.with(|v| v.get());
        AllocTrackingSpawn { id, spawn }
    }

    pub struct AllocTrackingSpawn<S: Spawn> {
        id: usize,
        spawn: S,
    }

    impl<S: Spawn> Spawn for AllocTrackingSpawn<S> {
        fn spawn(&self, op: Box<dyn FnOnce() + Send>) {
            let id = self.id;
            self.spawn.spawn(Box::new(move || {
                THREAD_ID.with(|v| v.set(id));
                op()
                // We intentionally leave THREAD_ID set to `id` rather
                // than saving and restoring its value after calling `op`.
                // Spawning tasks requires allocating memory (as we did to
                // construct this very boxed fn) that is dropped and freed
                // with the Spawn impl after the op is invoked. Restoring
                // the original THREAD_ID (which is likely unset) causes
                // memory allocated on the spawning thread to be counted
                // against one thread while the corresponding deallocation
                // is counted againt another (or none at all assuming the
                // default THREAD_ID of 0 is restored).
            }));
        }
    }

    pub fn start_test() {
        let id = NEXT_ID.fetch_add(1, Relaxed);
        THREAD_ID.with(|val| val.set(id));
    }
}

static LOG_INIT: Once = Once::new();

fn init_logging() {
    LOG_INIT.call_once(|| {
        env_logger::init();
    })
}

fn build_service(dir: &Path) -> anyhow::Result<Qrono> {
    init_logging();
    let working_set_dir = dir.join("working_set");
    let queues_dir = dir.join("queues");
    std::fs::create_dir_all(&working_set_dir)?;
    std::fs::create_dir_all(&queues_dir)?;
    let scheduler = Scheduler::new(test_alloc::wrap_spawn(StaticPool::new(1)));
    let id_generator = IdGenerator::new(dir.join("id"), scheduler.clone())?;
    let working_set = WorkingSet::new(vec![(working_set_dir, scheduler.clone())])?;
    Ok(Qrono::new(
        scheduler.clone(),
        timer::Scheduler::new(),
        id_generator,
        working_set,
        queues_dir,
        scheduler,
        None,
    ))
}

// Basic smoke test. Ensure we can get an item from enqueued to released.
#[test]
fn test_enqueue_dequeue_release() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let qrono = build_service(dir.path())?;

    let enqueue_res = enqueue(&qrono, "q", "Hello, world!", DeadlineReq::Now).take()?;
    assert!(enqueue_res.deadline.millis() > 0);

    let mut dequeue_res = dequeue(&qrono, "q", 1, Duration::ZERO).take()?;
    assert_eq!(1, dequeue_res.len());
    let item = dequeue_res.remove(0);
    assert_eq!(b"Hello, world!".as_ref(), item.value.as_ref());

    assert!(release(&qrono, "q", item.id).take().is_ok());

    let info = info(&qrono, "q").take()?;
    assert_eq!(0, info.pending);
    assert_eq!(0, info.dequeued);

    assert!(delete(&qrono, "q").take().is_ok());

    Ok(())
}

#[test]
fn test_ordered_by_deadline() -> anyhow::Result<()> {
    init_logging();
    let dir = tempdir()?;
    let qrono = build_service(dir.path())?;

    const N: u64 = 100;
    let base = Timestamp::now();
    let mut reqs = Vec::new();
    // Generate N enqueue requests in 1ms deadline steps.
    for i in 0..N {
        reqs.push(EnqueueReq {
            value: ValueReq::String(format!("{i}")),
            deadline: DeadlineReq::Absolute(base + Duration::from_millis(i)),
        });
    }

    // Shuffle and enqueue in random order
    reqs.shuffle(&mut rand::thread_rng());
    for req in reqs {
        let (promise, resp) = QronoFuture::new();
        qrono.enqueue("q", req, promise);
        resp.take()?;
    }

    // Ensure dequeues happen in order
    for i in 0..N {
        let items = dequeue(&qrono, "q", 1, Duration::from_secs(10)).take()?;
        assert_eq!(1, items.len());
        let s = String::from_utf8(items[0].value.to_vec())?;
        assert_eq!(format!("{}", i), s);
    }

    // Verify that all N items are now dequeued.
    let info = info(&qrono, "q").take()?;
    assert_eq!(0, info.pending);
    assert_eq!(N, info.dequeued);

    Ok(())
}

#[test]
fn test_requeue() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let qrono = build_service(dir.path())?;

    enqueue(&qrono, "q", "Hello, world", DeadlineReq::Now).take()?;
    let mut items = dequeue(&qrono, "q", 1, Duration::ZERO).take()?;
    let item0 = items.pop().ok_or_else(|| fail("item expected"))?;
    assert_eq!(Timestamp::ZERO, item0.stats.requeue_time);
    assert_eq!(1, item0.stats.dequeue_count, "invalid dequeue_count");

    requeue(&qrono, "q", item0.id, DeadlineReq::Now).take()?;

    let mut items = dequeue(&qrono, "q", 1, Duration::ZERO).take()?;
    let item1 = items.pop().ok_or_else(|| fail("item expected"))?;
    assert_eq!(item0.value, item1.value);
    assert_ne!(Timestamp::ZERO, item1.stats.requeue_time);
    assert_eq!(2, item1.stats.dequeue_count);

    Ok(())
}

#[test]
fn test_peek() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let qrono = build_service(dir.path())?;

    // Enqueue item with deadline 1 hour in the future
    enqueue(
        &qrono,
        "q",
        "Hello, world",
        DeadlineReq::Relative(Duration::from_secs(3600)),
    )
    .take()?;

    // Verify no items are immediately available for dequeue
    assert!(matches!(
        dequeue(&qrono, "q", 1, Duration::ZERO).take(),
        Err(QronoError::NoItemReady)
    ));

    // Verify peek still shows us the value
    let item = peek(&qrono, "q").take()?;
    assert_eq!("Hello, world", std::str::from_utf8(&item.value).unwrap());
    assert!(item.deadline > Timestamp::now());

    Ok(())
}

#[test]
fn test_blocking_dequeue() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let qrono = build_service(dir.path())?;

    // Create the queue by enqueueing an item an hour into the future.
    enqueue(
        &qrono,
        "q",
        "",
        DeadlineReq::Relative(Duration::from_secs(3600)),
    )
    .take()?;

    // Start a blocking dequeue with 10 second timeout.
    let deq = dequeue(&qrono, "q", 1, Duration::from_secs(10));

    // Meanwhile, enqueue a value 100ms into the future
    enqueue(
        &qrono,
        "q",
        "Hello, world!",
        DeadlineReq::Relative(Duration::from_millis(100)),
    )
    .take()?;

    // Verify the dequeue waits for and returns the newly enqueued item.
    let items = deq.take()?;
    assert_eq!(1, items.len());
    assert_eq!(
        "Hello, world!",
        std::str::from_utf8(&items[0].value).unwrap()
    );

    Ok(())
}

#[test]
fn test_memory_estimate() -> anyhow::Result<()> {
    test_memory_estimate_n(0)?;
    test_memory_estimate_n(10)?;
    test_memory_estimate_n(100)?;
    test_memory_estimate_n(1000)?;
    Ok(())
}

fn test_memory_estimate_n(n: usize) -> anyhow::Result<()> {
    test_alloc::start_test();

    let dir = tempdir()?;
    let qrono = build_service(dir.path())?;

    // Ensure the queue exists before measuring the baseline memory usage.
    enqueue(&qrono, "q", "", DeadlineReq::Now).take()?;
    let items = dequeue(&qrono, "q", 1, Duration::ZERO).take()?;
    release(&qrono, "q", items[0].id).take()?;

    const M: usize = 100;
    let value = vec![0u8; n];
    eprintln!("Starting test...");

    let baseline = test_alloc::allocated();
    let mut count = 0;
    for _ in 0..1000 {
        enqueue_many(&qrono, "q", M, || {
            (Bytes::from(&value[..]), DeadlineReq::Now)
        })?;
        count += M;
        let info = info(&qrono, "q").take()?;
        let actual = info.mem_size;
        let expected = (test_alloc::allocated() - baseline) as f64;
        let actual = actual as f64;
        let diff = actual - expected;
        let diff_percent = 100.0 * diff / actual;

        println!("{n}\t{count}\t{actual}\t{expected}\t{diff}\t{diff_percent:.2}%");
        assert!(
            diff.abs() < 100.0 * 1024.0 || diff_percent.abs() < 1.0,
            "expected memory estimate to be within 1.0%, actually |{actual} - {expected} = {diff}| > {diff_percent:.2}%",
        );
    }

    Ok(())
}

// Slow. Do not run by default.
#[ignore = "slow"]
#[test]
fn test_large_queue() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let qrono = build_service(dir.path())?;

    // Push ~1GB of data into the queue
    let mut enqueue_hasher = DefaultHasher::new();
    let mut enqueue_count = 0;
    let mut pipeline = VecDeque::new();
    for _ in 0..(100 * 1000) {
        let mut value = vec![0u8; 10000];
        rand::thread_rng().fill_bytes(&mut value);
        enqueue_hasher.write(&value);
        pipeline.push_back(enqueue(&qrono, "q", value, DeadlineReq::Now));
        enqueue_count += 1;
        if pipeline.len() == 100 {
            pipeline.pop_front().unwrap().take()?;
        }
    }
    for enqueue in pipeline {
        enqueue.take()?;
    }

    // Dequeue and release all items in batches
    let mut dequeue_hasher = DefaultHasher::new();
    let mut dequeue_count = 0;
    loop {
        let items = match dequeue(&qrono, "q", 100, Duration::ZERO).take() {
            Ok(items) => items,
            Err(QronoError::NoItemReady) => break,
            Err(err) => Err(err)?,
        };
        for item in &items {
            dequeue_hasher.write(&item.value);
            dequeue_count += 1;
        }
        let releases = items
            .into_iter()
            .map(|item| release(&qrono, "q", item.id))
            .collect::<Vec<_>>();
        for release in releases {
            release.take()?;
        }
    }

    // Verify queue is once again empty
    let info = info(&qrono, "q").take()?;
    assert_eq!(0, info.pending);
    assert_eq!(0, info.dequeued);
    assert_eq!(enqueue_count, dequeue_count);
    assert_eq!(enqueue_hasher.finish(), dequeue_hasher.finish());

    Ok(())
}

fn enqueue_many<F, B>(
    qrono: &Qrono,
    queue: &str,
    count: usize,
    mut producer: F,
) -> anyhow::Result<()>
where
    F: FnMut() -> (B, DeadlineReq),
    B: Into<Bytes>,
{
    const PIPELINE_LENGTH: usize = 100;
    let mut pipeline = VecDeque::new();
    for _ in 0..count {
        let (value, deadline) = producer();
        pipeline.push_back(enqueue(qrono, queue, value, deadline));
        if pipeline.len() == PIPELINE_LENGTH {
            pipeline.pop_front().unwrap().take()?;
        }
    }
    for enqueue in pipeline {
        enqueue.take()?;
    }
    Ok(())
}

fn enqueue<B: Into<Bytes>>(
    qrono: &Qrono,
    queue: &str,
    value: B,
    deadline: DeadlineReq,
) -> QronoFuture<EnqueueResp> {
    let (promise, resp) = QronoFuture::new();
    qrono.enqueue(
        queue,
        EnqueueReq {
            value: ValueReq::Bytes(value.into()),
            deadline,
        },
        promise,
    );
    resp
}

fn dequeue(qrono: &Qrono, queue: &str, count: u64, timeout: Duration) -> QronoFuture<DequeueResp> {
    let (promise, resp) = QronoFuture::new();
    qrono.dequeue(queue, DequeueReq { timeout, count }, promise);
    resp
}

fn requeue(qrono: &Qrono, queue: &str, id: ID, deadline: DeadlineReq) -> QronoFuture<RequeueResp> {
    let (promise, resp) = QronoFuture::new();
    qrono.requeue(
        queue,
        RequeueReq {
            id: Id(id),
            deadline,
        },
        promise,
    );
    resp
}

fn release(qrono: &Qrono, queue: &str, id: ID) -> QronoFuture<ReleaseResp> {
    let (promise, resp) = QronoFuture::new();
    qrono.release(queue, ReleaseReq { id: Id(id) }, promise);
    resp
}

fn info(qrono: &Qrono, queue: &str) -> QronoFuture<InfoResp> {
    let (promise, resp) = QronoFuture::new();
    qrono.info(queue, InfoReq, promise);
    resp
}

fn delete(qrono: &Qrono, queue: &str) -> QronoFuture<DeleteResp> {
    let (promise, resp) = QronoFuture::new();
    qrono.delete(queue, DeleteReq, promise);
    resp
}

fn peek(qrono: &Qrono, queue: &str) -> QronoFuture<PeekResp> {
    let (promise, resp) = QronoFuture::new();
    qrono.peek(queue, PeekReq, promise);
    resp
}

fn fail<S: Into<String>>(msg: S) -> Fail {
    Fail(msg.into())
}

#[derive(Debug)]
struct Fail(String);

impl Display for Fail {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self, f)
    }
}

impl Error for Fail {}
