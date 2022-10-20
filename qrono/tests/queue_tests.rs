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
    let scheduler = Scheduler::new(StaticPool::new(1));
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
