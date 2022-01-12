use criterion::{black_box, criterion_group, criterion_main, Criterion};
use qrono_promise::Future;

#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

pub fn promise(c: &mut Criterion) {
    let mut group = c.benchmark_group("promise");
    group.bench_with_input("take", &0, |b, val| {
        b.iter(|| {
            let (rx, tx) = Future::new();
            tx.complete(*val);
            rx.take()
        })
    });

    group.bench_with_input("transfer", &0, |b, val| {
        b.iter(|| {
            let tx = Future::transfer(|v| {
                black_box(v);
            });
            tx.complete(*val)
        })
    });

    group.bench_with_input("callback", &0, |b, val| {
        b.iter(|| {
            let (rx, mut tx) = Future::new();
            tx.on_complete(|| {});
            tx.complete(*val);
            rx.take()
        })
    });

    group.bench_with_input("map", &1, |b, val| {
        b.iter(|| {
            let (rx, tx) = Future::map(|v| v + 1);
            tx.complete(*val);
            rx.take()
        })
    });

    group.finish();
}

pub fn reference(c: &mut Criterion) {
    let mut group = c.benchmark_group("reference");
    group.bench_with_input("std::sync::mpsc", &1, |b, val| {
        b.iter(|| {
            let (tx, rx) = std::sync::mpsc::channel();
            tx.send(val).unwrap();
            rx.recv().unwrap()
        })
    });
    group.bench_with_input("crossbeam::channel::bounded", &1, |b, val| {
        b.iter(|| {
            let (tx, rx) = crossbeam::channel::bounded(1);
            tx.send(val).unwrap();
            rx.recv().unwrap()
        })
    });
    group.bench_with_input("crossbeam::channel::unbounded", &1, |b, val| {
        b.iter(|| {
            let (tx, rx) = crossbeam::channel::unbounded();
            tx.send(val).unwrap();
            rx.recv().unwrap()
        })
    });
    group.finish();
}

criterion_group!(benches, promise, reference);
criterion_main!(benches);
