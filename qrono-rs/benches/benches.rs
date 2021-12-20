use std::io::Cursor;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use qrono::data::{Entry, Item, Key, Stats, Timestamp, ID};
use qrono::hash;
use qrono::id_generator::IdGenerator;
use qrono::promise::Future;
use qrono::redis::Value;
use qrono::scheduler::{Scheduler, StaticPool};
use qrono::segment::mock::{MockSegment, MockSegmentReader};
use qrono::segment::{
    ImmutableSegment, MemorySegment, MergedSegmentReader, Segment, SegmentReader,
};
use qrono::wal::WriteAheadLog;
use rand::Rng;

#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

pub fn parse_command(c: &mut Criterion) {
    let input = b"*3\r\n$7\r\nENQUEUE\r\n$1\r\nq\r\n$8\r\nAAAAAAAA\r\n";
    let input_bytes = Bytes::copy_from_slice(input);

    c.bench_function("copy/bytes", |b| {
        b.iter(|| Bytes::copy_from_slice(input).clone().clone())
    });

    c.bench_function("copy/vec", |b| b.iter(|| input.to_vec().clone().clone()));

    c.bench_function("Value::from_bytes", |b| {
        b.iter(|| Value::from_bytes(input).unwrap())
    });

    c.bench_function("Value::from_buf", |b| {
        b.iter(|| Value::from_buf(input_bytes.clone()).unwrap())
    });

    c.bench_function("redis::parse_redis_value", |b| {
        b.iter(|| redis::parse_redis_value(input).unwrap())
    });
}

pub fn mem_segment(c: &mut Criterion) {
    c.bench_function("MemorySegment::add", |b| {
        b.iter_custom(|iters| {
            let mut s = MemorySegment::new(None);
            let start = Instant::now();
            for i in 0..iters {
                s.add(Entry::Pending(Item {
                    id: i,
                    deadline: Timestamp::ZERO,
                    stats: Stats {
                        enqueue_time: Timestamp::ZERO,
                        requeue_time: Timestamp::ZERO,
                        dequeue_count: 0,
                    },
                    value: Default::default(),
                    segment_id: 0,
                }))
                .unwrap();
            }
            start.elapsed()
        })
    });

    for batch in [1, 5, 10, 100] {
        let id = BenchmarkId::new("MemorySegment::add with WAL", batch);
        c.bench_with_input(id, &batch, |b, m| {
            b.iter_custom(move |iters| {
                let wal_path = "/tmp/qrono.log";
                let wal = WriteAheadLog::new(wal_path).unwrap();
                let mut s = MemorySegment::new(Some(wal));
                let value = Bytes::from(vec![0; 64]);
                let start = Instant::now();
                for i in 0..(iters / m) {
                    let mut batch = Vec::with_capacity(10);
                    for j in 0..*m {
                        batch.push(Entry::Pending(Item {
                            id: i * m + j,
                            deadline: Timestamp::ZERO,
                            stats: Stats {
                                enqueue_time: Timestamp::ZERO,
                                requeue_time: Timestamp::ZERO,
                                dequeue_count: 0,
                            },
                            value: value.clone(),
                            segment_id: 0,
                        }));
                    }
                    s.add_all(batch).unwrap();
                }
                let duration = start.elapsed();
                // println!(
                //     "count={}, size={}",
                //     iters,
                //     std::fs::metadata(wal_path).unwrap().len()
                // );
                std::fs::remove_file(wal_path).unwrap();
                duration
            })
        });
    }

    c.bench_function("MemorySegmentReader::next", |b| {
        b.iter_custom(|iters| {
            let mut s = MemorySegment::new(None);
            for i in 0..iters {
                s.add(Entry::Pending(Item {
                    id: i,
                    deadline: Timestamp::ZERO,
                    stats: Stats {
                        enqueue_time: Timestamp::ZERO,
                        requeue_time: Timestamp::ZERO,
                        dequeue_count: 0,
                    },
                    value: Default::default(),
                    segment_id: 0,
                }))
                .unwrap();
            }
            let mut reader = s.open_reader(Key::ZERO).unwrap();
            let start = Instant::now();
            for _i in 0..iters {
                black_box(reader.next().unwrap().unwrap());
            }
            start.elapsed()
        })
    });
}

pub fn adler32(c: &mut Criterion) {
    let mut group = c.benchmark_group("adler32");
    for size in &[100, 1000] {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, size| {
            let data = vec![b'A'; *size];
            b.iter(|| hash::adler32(&data))
        });
    }
}

pub fn murmur3(c: &mut Criterion) {
    let mut group = c.benchmark_group("murmur3::murmur3");
    for size in &[100, 1000] {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, size| {
            let data = vec![b'A'; *size];
            b.iter(|| murmur3::murmur3_32(&mut Cursor::new(&data), 0).unwrap())
        });
    }
    drop(group);

    let mut group = c.benchmark_group("hash::murmur3");
    for size in &[100, 1000] {
        group.throughput(Throughput::Bytes(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, size| {
            let data = vec![b'A'; *size];
            b.iter(|| hash::murmur3(&data, 0))
        });
    }
    drop(group);
}

pub fn promise(c: &mut Criterion) {
    c.bench_function("promise/future", |b| {
        b.to_async(tokio::runtime::Runtime::new().unwrap())
            .iter(|| tokio::spawn(std::future::ready(1)))
    });

    c.bench_function("promise/rayon", |b| {
        b.iter(|| {
            let (rx, tx) = Future::new();
            rayon::spawn(move || tx.complete(1));
            rx.take();
        })
    });

    c.bench_function("promise/promise", |b| {
        b.iter(|| {
            let (rx, tx) = Future::new();
            tx.complete(());
            rx.take()
        })
    });

    c.bench_function("promise/atomic", |b| {
        b.iter(|| {
            let x = AtomicUsize::new(0);
            x.swap(1, Ordering::Relaxed);
            x.swap(1, Ordering::Relaxed)
        })
    });

    c.bench_function("promise/promise/transfer", |b| {
        b.iter(|| {
            let (rx, tx) = Future::new();
            rx.transfer(|v| {
                black_box(v);
            });
            tx.complete(())
        })
    });

    c.bench_function("promise/promise/map", |b| {
        b.iter(|| {
            let (rx, tx) = Future::new();
            let rx = rx.map(|v| v + 1);
            tx.complete(1);
            rx.take()
        })
    });

    c.bench_function("promise/mpsc", |b| {
        b.iter(|| {
            let (tx, rx) = std::sync::mpsc::channel();
            tx.send(1).unwrap();
            rx.recv().unwrap()
        })
    });

    c.bench_function("promise/crossbeam", |b| {
        b.iter(|| {
            let (tx, rx) = crossbeam::channel::bounded(1);
            tx.send(1).unwrap();
            rx.recv().unwrap()
        })
    });
}

pub fn id_generator(c: &mut Criterion) {
    c.bench_function("IdGenerator::generate_id", |b| {
        let generator = IdGenerator::new(
            format!("/tmp/id-{}", Timestamp::now().millis()),
            Scheduler::new(StaticPool::new(1)),
        )
        .unwrap();
        b.iter(move || generator.generate_id());
    });
}

pub fn indexed_segment(c: &mut Criterion) {
    let tmpdir = tempfile::tempdir().unwrap();
    let segment_path = {
        let mut p = tmpdir.path().to_path_buf();
        p.push("qrono.seg");
        dbg!(p)
    };

    let value = Bytes::from("A".repeat(64));
    let n = 10_000_000;
    let reader = MockSegmentReader::new(|i| {
        if i < n {
            Some(Entry::Pending(Item {
                id: i as ID,
                deadline: Timestamp::from_millis((i + 10000) as i64),
                stats: Default::default(),
                value: value.clone(),
                segment_id: 0,
            }))
        } else {
            None
        }
    });
    let segment = ImmutableSegment::write_pending(segment_path, reader, 0).unwrap();
    let mut reader = segment.open_reader(Key::ZERO).unwrap();
    let mut keys = Vec::with_capacity(100);
    let mut cnt = 0;
    let mut rng = rand::thread_rng();
    while let Some(entry) = reader.next().unwrap() {
        if cnt < keys.capacity() {
            keys.push(entry.key());
        } else {
            let idx = rng.gen_range(0..cnt);
            if idx < keys.len() {
                keys[idx] = entry.key();
            }
        }
        cnt += 1;
    }

    c.bench_function("IndexedSegment::open_reader_at", |b| {
        b.iter_custom(|n| {
            let start = Instant::now();
            for i in 0..(n as usize) {
                let key = keys[i % keys.len()];
                black_box(segment.open_reader(key).unwrap());
            }
            start.elapsed()
        });
    });
}

fn merged_segment(c: &mut Criterion) {
    //  0  1  2  3  4    5  6  7  8  9   10 11 12 13 14     (i)
    // ---------------------------------------------------------
    //  0  1  2  3  4   30 31 32 33 34   60 61 62 63 64   (j=0)
    //  5  6  7  8  9   35 36 37 38 39   65 66 67 68 69   (j=1)
    // 10 11 12 13 14   40 41 42 43 44   70 71 72 73 74   (j=2)
    // 15 16 17 18 19   45 46 47 48 49   75 76 77 78 79   (j=3)
    // 20 21 22 23 24   50 51 52 53 54   80 81 82 83 84   (j=4)
    // 25 26 27 28 29   55 56 57 58 59   85 86 87 88 89   (j=5)
    //                                               (r=5, m=6)

    fn generate_id(r: usize, m: usize, j: usize, i: usize) -> ID {
        (((i / r) * r * m) + (j * r) + (i % r)) as ID
    }

    fn generate_entry<F>(i: usize, id: F) -> Option<Entry>
    where
        F: Fn(usize) -> ID,
    {
        let id = id(i);
        Some(Entry::Pending(Item {
            id,
            deadline: Timestamp::from_millis(id as i64 + 100000),
            stats: Default::default(),
            value: Bytes::from("AAAAAAAA"),
            segment_id: 0,
        }))
    }

    fn make_merged_reader(r: usize, m: usize) -> MergedSegmentReader<usize> {
        let mut merged = MergedSegmentReader::new();
        for j in 0..m {
            let generator = move |i| generate_entry(i, |i| generate_id(r, m, j, i));
            let src = MockSegment::new(generator);
            merged.add(j, src, Key::ZERO).unwrap();
        }
        merged
    }

    let mut group = c.benchmark_group("MergedSegmentReader::next");
    group.throughput(Throughput::Elements(1));

    let mut bench = |r: usize, m: usize| {
        group.bench_with_input(format!("r={},m={}", r, m), &(r, m), |b, (r, m)| {
            let mut merged = make_merged_reader(*r, *m);
            b.iter(|| merged.next().unwrap().unwrap())
        });
    };

    bench(1, 1);
    bench(1, 5);
    bench(10, 5);
    bench(1_000_000, 10);
}

criterion_group!(
    benches,
    parse_command,
    mem_segment,
    adler32,
    murmur3,
    promise,
    id_generator,
    indexed_segment,
    merged_segment,
);

criterion_main!(benches);
