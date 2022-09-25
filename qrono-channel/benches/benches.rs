use criterion::{black_box, criterion_group, criterion_main, Criterion};
use crossbeam::queue::SegQueue;
use crossbeam_utils::Backoff;
use std::sync::Barrier;
use std::time::Instant;

#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

const THREADS: u64 = 4;

pub fn batch_channel(c: &mut Criterion) {
    let mut group = c.benchmark_group("qrono_channel::batch");
    group.bench_function("spsc", |b| {
        b.iter_custom(|iters| {
            let barrier = Barrier::new(2);
            let (tx, mut rx) = qrono_channel::batch::channel();

            crossbeam::scope(|s| {
                s.spawn(|_| {
                    barrier.wait();
                    for i in 0..iters {
                        tx.send(i);
                    }
                });
                barrier.wait();
                let start = Instant::now();
                let mut remaining = iters;
                while remaining > 0 {
                    let batch = rx.recv(256);
                    if batch.is_empty() {
                        std::thread::yield_now();
                    } else {
                        for val in batch {
                            remaining -= 1;
                            black_box(val);
                        }
                    }
                }
                start.elapsed()
            })
            .unwrap()
        });
    });

    group.bench_function("mpsc", |b| {
        b.iter_custom(|iters| {
            let backoff = Backoff::new();
            let barrier = Barrier::new((THREADS + 1) as usize);
            let (tx, mut rx) = qrono_channel::batch::channel();

            crossbeam::scope(|s| {
                for i in 0..THREADS {
                    let count = if i < iters % THREADS {
                        (iters / THREADS) + 1
                    } else {
                        iters / THREADS
                    };

                    let barrier = &barrier;
                    let tx = &tx;
                    s.spawn(move |_| {
                        barrier.wait();
                        for i in 0..count {
                            tx.send(i);
                        }
                    });
                }
                barrier.wait();
                let start = Instant::now();
                let mut remaining = iters;
                while remaining > 0 {
                    let batch = rx.recv(256);
                    if batch.is_empty() {
                        backoff.spin();
                    } else {
                        let cnt = batch.len();
                        for val in batch {
                            remaining -= 1;
                            black_box(val);
                        }
                        if remaining > 0 && cnt < 100 {
                            backoff.spin();
                        }
                    }
                }
                start.elapsed()
            })
            .unwrap()
        });
    });

    group.bench_function("recv", |b| {
        b.iter_custom(|iters| {
            let (tx, mut rx) = qrono_channel::batch::channel();
            for i in 0..iters {
                tx.send(i);
            }

            let start = Instant::now();
            let mut remaining = iters;
            while remaining > 0 {
                let batch = rx.recv(256);
                if batch.is_empty() {
                    std::thread::yield_now();
                } else {
                    for val in batch {
                        remaining -= 1;
                        black_box(val);
                    }
                }
            }
            start.elapsed()
        });
    });

    group.bench_function("send", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            let (tx, rx) = qrono_channel::batch::channel();
            for i in 0..iters {
                tx.send(i);
            }
            black_box(rx);
            start.elapsed()
        });
    });

    group.finish();
}

pub fn seq_queue(c: &mut Criterion) {
    let mut group = c.benchmark_group("seq_queue");
    group.bench_function("spsc", |b| {
        b.iter_custom(|iters| {
            let barrier = Barrier::new(2);
            let queue = SegQueue::new();

            crossbeam::scope(|s| {
                s.spawn(|_| {
                    barrier.wait();
                    for i in 0..iters {
                        queue.push(i);
                    }
                });
                barrier.wait();
                let start = Instant::now();
                let mut remaining = iters;
                while remaining > 0 {
                    match queue.pop() {
                        None => std::thread::yield_now(),
                        Some(val) => {
                            remaining -= 1;
                            black_box(val);
                        }
                    }
                }
                start.elapsed()
            })
            .unwrap()
        });
    });

    group.bench_function("mpsc", |b| {
        b.iter_custom(|iters| {
            let barrier = Barrier::new((THREADS + 1) as usize);
            let queue = SegQueue::new();

            crossbeam::scope(|s| {
                for i in 0..THREADS {
                    let count = if i < iters % THREADS {
                        (iters / THREADS) + 1
                    } else {
                        iters / THREADS
                    };

                    let barrier = &barrier;
                    let queue = &queue;
                    s.spawn(move |_| {
                        barrier.wait();
                        for i in 0..count {
                            queue.push(i);
                        }
                    });
                }
                barrier.wait();
                let start = Instant::now();
                let mut remaining = iters;
                while remaining > 0 {
                    match queue.pop() {
                        None => std::thread::yield_now(),
                        Some(val) => {
                            remaining -= 1;
                            black_box(val);
                        }
                    }
                }
                start.elapsed()
            })
            .unwrap()
        });
    });

    group.bench_function("recv", |b| {
        b.iter_custom(|iters| {
            let queue = SegQueue::new();
            for i in 0..iters {
                queue.push(i);
            }

            let start = Instant::now();
            let mut remaining = iters;
            while remaining > 0 {
                match queue.pop() {
                    None => std::thread::yield_now(),
                    Some(val) => {
                        remaining -= 1;
                        black_box(val);
                    }
                }
            }
            start.elapsed()
        });
    });

    group.bench_function("send", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            let queue = SegQueue::new();
            for i in 0..iters {
                queue.push(i);
            }
            black_box(queue);
            start.elapsed()
        });
    });

    group.finish();
}

criterion_group!(benches, batch_channel, seq_queue);
criterion_main!(benches);
