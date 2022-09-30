use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::mem::MaybeUninit;
use std::path::Path;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::Ordering::Release;
use std::time::{Duration, Instant};

use log::{debug, info};
use rand::seq::SliceRandom;
use tempfile::tempdir;

use qrono::bytes::Bytes;
use qrono::data::{Timestamp, ID};
use qrono::id_generator::IdGenerator;
use qrono::ops::IdPattern::Id;
use qrono::ops::{
    DeadlineReq, DeleteReq, DequeueReq, DequeueResp, EnqueueReq, EnqueueResp, IdPattern, InfoReq,
    InfoResp, PeekReq, PeekResp, ReleaseReq, ReleaseResp, RequeueReq, RequeueResp, ValueReq,
};
use qrono::promise::QronoFuture;
use qrono::result::QronoResult;
use qrono::scheduler::{Scheduler, StaticPool};
use qrono::service::Qrono;
use qrono::timer;
use qrono::working_set::WorkingSet;
use qrono_promise::Future;

fn build_service(dir: &Path) -> anyhow::Result<Qrono> {
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
        scheduler.clone(),
        None,
    ))
}

// Basic smoke test. Ensure we can get an item from enqueued to released.
#[test]
fn test_enqueue_dequeue_release() -> anyhow::Result<()> {
    let dir = tempdir()?;
    let qrono = build_service(dir.path())?;

    let (promise, res) = Future::new();
    qrono.enqueue(
        "q",
        EnqueueReq {
            value: "Hello, world!".into(),
            deadline: Default::default(),
        },
        promise,
    );
    let enqueue_res = res.take()?;
    assert!(enqueue_res.deadline.millis() > 0);

    let (promise, res) = Future::new();
    qrono.dequeue(
        "q",
        DequeueReq {
            timeout: Default::default(),
            count: 1,
        },
        promise,
    );
    let mut dequeue_res = res.take()?;
    assert_eq!(1, dequeue_res.len());
    let item = dequeue_res.remove(0);
    assert_eq!(b"Hello, world!".as_ref(), item.value.as_ref());

    let (promise, res) = Future::new();
    qrono.release("q", ReleaseReq { id: Id(item.id) }, promise);
    assert!(res.take().is_ok());

    let (promise, res) = Future::new();
    qrono.info("q", InfoReq {}, promise);
    let info = res.take()?;
    assert_eq!(0, info.pending);
    assert_eq!(0, info.dequeued);

    let (promise, res) = Future::new();
    qrono.delete("q", DeleteReq, promise);
    assert!(res.take().is_ok());

    Ok(())
}

#[test]
fn test_ordered_by_deadline() -> anyhow::Result<()> {
    env_logger::builder().format_timestamp_micros().init();

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
        let (promise, resp) = QronoFuture::new();
        qrono.dequeue(
            "q",
            DequeueReq {
                timeout: Duration::from_secs(10),
                count: 1,
            },
            promise,
        );
        let items = resp.take()?;
        assert_eq!(1, items.len());
        let s = String::from_utf8(items[0].value.to_vec())?;
        assert_eq!(format!("{}", i), s);
    }

    // Verify that all N items are now dequeued.
    let (promise, resp) = QronoFuture::new();
    qrono.info("q", InfoReq, promise);
    let info = resp.take()?;
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
    let item0 = items.pop().ok_or(fail("item expected"))?;
    assert_eq!(Timestamp::ZERO, item0.stats.requeue_time);
    assert_eq!(1, item0.stats.dequeue_count, "invalid dequeue_count");
    requeue(&qrono, "q", item0.id, DeadlineReq::Now).take()?;
    let mut items = match dequeue(&qrono, "q", 1, Duration::ZERO).take() {
        Ok(items) => items,
        Err(_) => {
            std::thread::sleep(Duration::from_secs(1));

            return Err(fail(format!(
                "{:?} / {:?} / {:?}",
                info(&qrono, "q").take(),
                dequeue(&qrono, "q", 1, Duration::ZERO).take(),
                peek(&qrono, "q").take(),
            )))?;
        }
    };

    let item1 = items.pop().ok_or(fail("item expected"))?;

    assert_eq!(item0.value, item1.value);
    assert_ne!(Timestamp::ZERO, item1.stats.requeue_time);
    assert_eq!(2, item1.stats.dequeue_count);

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
