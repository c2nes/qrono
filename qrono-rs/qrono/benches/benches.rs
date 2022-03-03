use bytes::{Bytes, BytesMut};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use qrono::bytes::Bytes as QronoBytes;
use qrono::data::{Entry, Item, Key, Stats, Timestamp, ID};
use qrono::hash;
use qrono::id_generator::IdGenerator;
use qrono::redis::protocol::{RedisBuf, Value};
use qrono::scheduler::{Scheduler, StaticPool};
use qrono::segment::mock::{MockSegment, MockSegmentReader};
use qrono::segment::{
    ImmutableSegment, MemorySegment, MergedSegmentReader, Segment, SegmentReader,
};
use qrono::wal::WriteAheadLog;
use rand::Rng;
use std::io::Cursor;
use std::iter;
use std::time::Instant;

#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

pub fn parse_command(c: &mut Criterion) {
    let input = b"*3\r\n$7\r\nENQUEUE\r\n$1\r\nq\r\n$8\r\nAAAAAAAA\r\n";
    let input_bytes = QronoBytes::from(&input[..]);

    c.bench_function("copy/bytes", |b| b.iter(|| Bytes::copy_from_slice(input)));

    c.bench_function("copy/vec", |b| b.iter(|| input.to_vec()));

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
                let wal = WriteAheadLog::new(wal_path, None).unwrap();
                let mut s = MemorySegment::new(Some(wal));
                let value = QronoBytes::from(vec![0; 64]);
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

    let value = QronoBytes::from("A".repeat(64));
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
            value: QronoBytes::from("AAAAAAAA").clone(),
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

pub fn redis_serde(c: &mut Criterion) {
    let mut group = c.benchmark_group("redis");
    let deqeue = b"*4\r\n\
    $7\r\n\
    DEQUEUE\r\n\
    $1\r\n\
    q\r\n\
    $7\r\n\
    TIMEOUT\r\n\
    $4\r\n\
    1000\r\n";

    group.bench_with_input("decode/dequeue", &deqeue[..], |b, x| {
        b.iter(|| Value::from_bytes(x).unwrap());
    });

    let ping = b"*1\r\n\
    $4\r\n\
    PING\r\n";

    group.bench_with_input("decode/ping", &ping[..], |b, x| {
        b.iter(|| Value::from_bytes(x).unwrap());
    });

    let millis = Timestamp::now().millis();
    let resp_one = Value::Array(vec![
        Value::Integer(1000000000),
        Value::Integer(millis),
        Value::Integer(millis),
        Value::Integer(millis),
        Value::Integer(1),
        Value::BulkString(QronoBytes::from(&b"01234567"[..])),
    ]);
    let resp = Value::Array(vec![resp_one.clone()]);
    let resp_multi = Value::Array(iter::repeat(resp_one.clone()).take(5).collect());

    let mut buf = Vec::new();

    group.bench_with_input("encode/dequeue_resp", &resp, |b, x| {
        b.iter(|| {
            buf.clear();
            x.put(&mut buf);
        });
    });

    group.bench_with_input("encode/dequeue_resp/one", &resp_one, |b, x| {
        b.iter(|| {
            buf.clear();
            x.put(&mut buf);
        });
    });

    group.bench_with_input("encode/dequeue_resp/multi", &resp_multi, |b, x| {
        b.iter(|| {
            buf.clear();
            x.put(&mut buf);
        });
    });

    group.bench_with_input("encode/timestamp", &Value::Integer(millis), |b, x| {
        b.iter(|| {
            buf.clear();
            x.put(&mut buf);
        });
    });

    group.bench_function("encode/put_u8", |b| {
        b.iter(|| {
            buf.clear();
            buf.put_u8(b':');
            buf.put_u8(b':');
            buf.put_u8(b':');
        });
    });

    group.bench_with_input("encode/u32/5", &5, |b, x| {
        b.iter(|| {
            buf.clear();
            qrono::redis::protocol::put_u32(&mut buf, *x);
        });
    });

    group.bench_with_input("encode/u32/1643041326", &1643041326, |b, x| {
        b.iter(|| {
            buf.clear();
            qrono::redis::protocol::put_u32(&mut buf, *x);
        });
    });

    group.bench_with_input("encode/u64/1643041326432", &1643041326432, |b, x| {
        b.iter(|| {
            buf.clear();
            qrono::redis::protocol::put_i64(&mut buf, *x);
        });
    });

    group.bench_with_input("encoded_length/dequeue_resp", &resp, |b, x| {
        b.iter(|| x.encoded_length());
    });

    group.bench_with_input("i64_len/4", &4, |b, x| {
        b.iter(|| qrono::redis::protocol::i64_len(*x));
    });

    group.bench_with_input("i64_len/1642893604833", &1642893604833, |b, x| {
        b.iter(|| qrono::redis::protocol::i64_len(*x));
    });

    group.bench_with_input("put_slice/bytes", b"0124", |b, x| {
        b.iter(|| {
            buf.clear();
            buf.put_slice(x);
            buf.len()
        })
    });

    let mut buf_vec = Vec::new();
    group.bench_with_input("put_slice/vec", b"0123", |b, x| {
        b.iter(|| {
            buf_vec.clear();
            buf_vec.extend_from_slice(x);
            buf_vec.len()
        })
    });
}

pub fn bytes(c: &mut Criterion) {
    let data = "Hello, world".to_string().into_bytes();
    let src = &data[..];

    let mut group = c.benchmark_group("Bytes");
    group.bench_with_input("create", src, |b, src| {
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(16);
            buf.put_slice(src);
            buf.freeze()
        })
    });

    group.bench_with_input("copy", src, |b, src| b.iter(|| Bytes::copy_from_slice(src)));

    group.bench_with_input("copy_clone", src, |b, src| {
        b.iter(|| Bytes::copy_from_slice(src).clone())
    });

    group.bench_with_input("clone", src, |b, src| {
        let bytes = Bytes::copy_from_slice(src);
        b.iter(|| bytes.clone())
    });
}

pub fn qrono_bytes(c: &mut Criterion) {
    let data = "Hello, world".to_string().into_bytes();
    let src = &data[..];

    let mut group = c.benchmark_group("QronoBytes");
    group.bench_with_input("create", src, |b, src| {
        b.iter(|| {
            let mut buf = Vec::with_capacity(16);
            buf.extend_from_slice(src);
            QronoBytes::from(buf)
        })
    });

    group.bench_with_input("copy", src, |b, src| b.iter(|| QronoBytes::from(src)));

    group.bench_with_input("copy_clone", src, |b, src| {
        b.iter(|| QronoBytes::from(src).clone())
    });

    group.bench_with_input("clone", src, |b, src| {
        let bytes = QronoBytes::from(src);
        b.iter(|| bytes.clone())
    });
}

criterion_group!(
    benches,
    parse_command,
    mem_segment,
    adler32,
    murmur3,
    id_generator,
    indexed_segment,
    merged_segment,
    redis_serde,
    bytes,
    qrono_bytes,
);

criterion_main!(benches);
