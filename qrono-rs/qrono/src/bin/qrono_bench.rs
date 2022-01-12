use core::panic;
use std::cmp::Ordering;
use std::convert::TryInto;
use std::fmt::{Display, Formatter};
use std::io::Error;
use std::str::FromStr;
use std::sync::{Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use clap::{App, Arg, ArgMatches};
use hdrhistogram::Histogram;
use redis::{
    ConnectionAddr, ConnectionInfo, FromRedisValue, IntoConnectionInfo, RedisResult, Value,
};

#[derive(Debug)]
struct EnqueueResult {
    id: i64,
    deadline: i64,
}

impl FromRedisValue for EnqueueResult {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        FromRedisValue::from_redis_value(v).map(|x: (i64, i64)| EnqueueResult {
            id: x.0,
            deadline: x.1,
        })
    }
}

#[derive(Debug)]
struct DequeueResult {
    id: i64,
    deadline: i64,
    enqueue_time: i64,
    requeue_time: i64,
    dequeue_count: i64,
    data: Vec<u8>,
}

impl FromRedisValue for DequeueResult {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        FromRedisValue::from_redis_value(v).map(|x: (i64, i64, i64, i64, i64, Vec<u8>)| {
            DequeueResult {
                id: x.0,
                deadline: x.1,
                enqueue_time: x.2,
                requeue_time: x.3,
                dequeue_count: x.4,
                data: x.5,
            }
        })
    }
}

#[derive(Debug, Clone)]
struct HostAndPort(String, u16);

impl FromStr for HostAndPort {
    type Err = &'static str;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.starts_with('[') {
            if let Some(idx) = s.find(']') {
                let (host, rest) = s.split_at(idx);
                let port: u16 = match rest.strip_prefix("]:").map(str::parse::<u16>) {
                    Some(Ok(port)) => port,
                    _ => return Err("invalid port"),
                };
                return Ok(HostAndPort(host.to_string(), port));
            }
            return Err("invalid IPv6 literal");
        }

        if let Some(idx) = s.find(':') {
            let (host, rest) = s.split_at(idx);
            let port = match rest[1..].parse::<u16>() {
                Ok(port) => port,
                _ => return Err("invalid port"),
            };
            return Ok(HostAndPort(host.to_string(), port));
        }

        Err("port required")
    }
}

impl IntoConnectionInfo for &HostAndPort {
    fn into_connection_info(self) -> RedisResult<ConnectionInfo> {
        Ok(ConnectionInfo {
            addr: Box::new(ConnectionAddr::Tcp(self.0.clone(), self.1)),
            db: 0,
            username: None,
            passwd: None,
        })
    }
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Ord, Eq)]
struct HumanSize(u64);

impl Display for HumanSize {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for HumanSize {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        const UNITS: [(&str, u64); 15] = [
            // IEC
            ("PiB", u64::pow(1024, 5)),
            ("TiB", u64::pow(1024, 4)),
            ("GiB", u64::pow(1024, 3)),
            ("MiB", u64::pow(1024, 2)),
            ("KiB", u64::pow(1024, 1)),
            // Metric
            ("PB", u64::pow(1000, 5)),
            ("TB", u64::pow(1000, 4)),
            ("GB", u64::pow(1000, 3)),
            ("MB", u64::pow(1000, 2)),
            ("KB", u64::pow(1000, 1)),
            // Informal
            ("T", u64::pow(1000, 4)),
            ("B", u64::pow(1000, 3)),
            ("M", u64::pow(1000, 2)),
            ("K", u64::pow(1000, 1)),
            // No suffix
            ("", 1),
        ];

        for (suffix, scaler) in UNITS {
            if let Some(prefix) = s.strip_suffix(suffix) {
                return match u64::from_str(prefix) {
                    Ok(base) => Ok(HumanSize(base * scaler)),
                    Err(err) => Err(err.to_string()),
                };
            }
        }

        Err(String::from("unrecognized suffix"))
    }
}

// Disable warning about these modes all beginning with "Publish".
#[allow(clippy::enum_variant_names)]
enum Mode {
    PublishThenDelete,
    PublishThenConsume,
    PublishAndConsume,
}

impl Mode {
    fn has_consumer(&self) -> bool {
        match &self {
            Mode::PublishThenDelete => false,
            Mode::PublishThenConsume => true,
            Mode::PublishAndConsume => true,
        }
    }
}

struct RateLimiter {
    tick: Duration,
    last: Instant,
}

impl RateLimiter {
    fn new(rate: f64) -> RateLimiter {
        let tick = if rate <= 0.0 {
            Duration::ZERO
        } else {
            Duration::from_secs_f64(1.0 / rate)
        };

        RateLimiter {
            tick,
            last: Instant::now(),
        }
    }

    fn acquire(&mut self, n: u32) {
        if self.tick.is_zero() {
            return;
        }

        let now = Instant::now();
        let target = self.last + n * self.tick;
        if now < target {
            thread::sleep(target - now);
        }
        self.last = target;
    }
}

struct PartitionIter<T: num::Integer> {
    // Splits `n` into `m` partitions.
    n: T,
    m: T,
    i: T,
}

impl<T: num::Integer + Copy> Iterator for PartitionIter<T> {
    type Item = (T, T);

    fn next(&mut self) -> Option<Self::Item> {
        if self.i >= self.m {
            return None;
        }

        let mut size = self.n / self.m;
        let idx = self.i;
        if idx < self.n % self.m {
            size = size + T::one();
        }

        self.i = self.i + T::one();

        Some((idx, size))
    }
}

struct BatchIter<T: num::Integer> {
    // Split `n` into batches of nominal size `size`
    n: T,
    size: T,
    i: T,
}

impl<T: num::Integer + Copy> Iterator for BatchIter<T> {
    type Item = (T, T);

    fn next(&mut self) -> Option<Self::Item> {
        if self.i >= self.n {
            return None;
        }

        let idx = self.i;
        let size = self.size.min(self.n - idx);
        self.i = idx + size;

        Some((idx, size))
    }
}

trait Batching: num::Integer + Copy {
    fn partition(self, m: Self) -> PartitionIter<Self> {
        PartitionIter {
            n: self,
            m,
            i: Self::zero(),
        }
    }

    fn batches(self, size: Self) -> BatchIter<Self> {
        BatchIter {
            n: self,
            size,
            i: Self::zero(),
        }
    }
}

impl<T: num::Integer + Copy> Batching for T {}

struct Benchmark {
    target: HostAndPort,
    queue_names: Vec<String>,
    count: HumanSize,
    size: usize,
    // value size
    consumer_count: u64,
    publisher_count: u64,
    // TODO: Replace with --num-threads
    publish_rate: f64,
    pipeline: u32,

    // --publish-then-delete
    // --publish-then-consume
    // --publish-and-consume

    // --consumer-mode (delete,wait,concurrent)
    mode: Mode,

    // Latency histograms
    // Enqueue, Dequeue, Release, End-to-end
    hist_enqueue: Mutex<Histogram<u64>>,
    hist_dequeue: Mutex<Histogram<u64>>,
    hist_release: Mutex<Histogram<u64>>,
    hist_end_to_end: Mutex<Histogram<u64>>,

    // Base time for end-to-end latencies
    base_time: Instant,
}

impl Benchmark {
    fn record_latency(hist: &Mutex<Histogram<u64>>, start: Instant) {
        let nanos = start.elapsed().as_nanos() as u64;
        hist.lock().unwrap().saturating_record(nanos);
    }

    fn run_consumer(
        &self,
        client: &redis::Client,
        queue_name: String,
        count: u64,
    ) -> Result<(), Error> {
        let mut conn = client.get_connection().unwrap();
        let pipeline = self.pipeline;

        let mut remaining = count;
        while remaining > 0 {
            let mut pipe = redis::pipe();
            for _ in 0..remaining.min(pipeline as u64) {
                pipe.cmd("DEQUEUE").arg(&queue_name);
            }

            let dequeue_start = Instant::now();
            let resp: Vec<Option<DequeueResult>> = pipe.query(&mut conn).unwrap();
            let resp: Vec<DequeueResult> = resp.into_iter().flatten().collect();

            if !resp.is_empty() {
                Self::record_latency(&self.hist_dequeue, dequeue_start);

                let mut pipe = redis::pipe();
                for dequeue in &resp {
                    pipe.cmd("RELEASE").arg(&queue_name).arg(dequeue.id);
                }

                let release_start = Instant::now();
                pipe.query::<Vec<bool>>(&mut conn).unwrap();
                Self::record_latency(&self.hist_release, release_start);

                for dequeue in &resp {
                    // Extract timestamp and record end-to-end latency
                    let enqueue_offset_bytes = dequeue.data[0..8].try_into().unwrap();
                    let enqueue_offset_nanos = u64::from_be_bytes(enqueue_offset_bytes);
                    let enqueue_offset = Duration::from_nanos(enqueue_offset_nanos);
                    let enqueue_time = self.base_time + enqueue_offset;
                    Self::record_latency(&self.hist_end_to_end, enqueue_time);
                }

                remaining -= resp.len() as u64;
            } else {
                thread::sleep(Duration::from_millis(10));
            }
        }

        Ok(())
    }

    fn run_publisher(
        &self,
        client: &redis::Client,
        queue_name: String,
        n: u64,
    ) -> Result<(), Error> {
        let mut limiter = RateLimiter::new(self.publish_rate / self.publisher_count as f64);
        let mut value = vec![b'A'; self.size];
        let mut conn = client.get_connection().unwrap();

        for (_, count) in n.batches(self.pipeline as u64) {
            limiter.acquire(count as u32);

            // Write timestamp prefix to value
            let timestamp = self.base_time.elapsed().as_nanos() as u64;
            value[0..8].copy_from_slice(&timestamp.to_be_bytes());

            let mut pipe = redis::pipe();
            for _ in 0..count {
                pipe.cmd("ENQUEUE").arg(&queue_name).arg(&value[..]);
            }

            let start = Instant::now();
            pipe.query::<Vec<EnqueueResult>>(&mut conn).unwrap();
            Self::record_latency(&self.hist_enqueue, start);
        }

        Ok(())
    }

    fn run(&self) -> Result<(), Error> {
        crossbeam::scope(|s| {
            for queue_name in &self.queue_names {
                s.spawn(move |_| {
                    self.run_queue(queue_name.clone()).unwrap();
                });
            }
        })
        .unwrap();

        Ok(())
    }

    fn run_queue(&self, queue_name: String) -> Result<(), Error> {
        let client = redis::Client::open(&self.target).unwrap();
        let n = self.count.0;

        let consumer_barrier = Barrier::new(self.consumer_count as usize + 1);
        let publisher_barrier = Barrier::new(self.publisher_count as usize + 1);

        crossbeam::scope(|s| {
            // Need to dequeue as many items as are enqueued
            let mut consumers = Vec::new();
            if self.mode.has_consumer() {
                for (_, count) in n.partition(self.consumer_count) {
                    let client = &client;
                    let consumer_barrier = &consumer_barrier;
                    let queue_name = queue_name.clone();
                    let handle = s.spawn(move |_| {
                        consumer_barrier.wait();
                        self.run_consumer(client, queue_name, count).unwrap();
                    });

                    consumers.push(handle);
                }
            }

            let mut publishers = Vec::new();
            for (_, n) in n.partition(self.publisher_count) {
                let client = &client;
                let publisher_barrier = &publisher_barrier;
                let queue_name = queue_name.clone();

                publishers.push(s.spawn(move |_| {
                    publisher_barrier.wait();
                    self.run_publisher(client, queue_name, n).unwrap();
                }));
            }

            // Start consumers before publishers if they are meant to run concurrently.
            if let Mode::PublishAndConsume = self.mode {
                consumer_barrier.wait();
            }

            // Start publishers and wait for them to complete.
            publisher_barrier.wait();
            let start_production = Instant::now();
            for publisher in publishers {
                publisher.join().unwrap();
            }
            let end_production = Instant::now();

            match self.mode {
                Mode::PublishThenDelete => {
                    let mut conn = client.get_connection().unwrap();
                    redis::cmd("DELETE")
                        .arg(&queue_name)
                        .query::<bool>(&mut conn)
                        .unwrap();
                }
                Mode::PublishThenConsume => {
                    // Start consumers
                    consumer_barrier.wait();
                }
                _ => {}
            }

            let produce_duration = end_production - start_production;
            let per_second = n as f64 / produce_duration.as_secs_f64();
            println!(
                "Producer done; duration={:.3}, rate={:.3}",
                produce_duration.as_secs_f64(),
                per_second
            );

            if self.mode.has_consumer() {
                for consumer in consumers {
                    consumer.join().unwrap();
                }

                let start_consume = match self.mode {
                    Mode::PublishThenConsume => end_production,
                    _ => start_production,
                };

                let consume_duration = start_consume.elapsed();
                let per_second = n as f64 / consume_duration.as_secs_f64();
                println!(
                    "Consumer done; duration={:.3}, rate={:.3}",
                    consume_duration.as_secs_f64(),
                    per_second
                );
            }
        })
        .unwrap();

        Ok(())
    }
}

fn arg<T: FromStr>(matches: &ArgMatches, name: &str) -> T {
    match T::from_str(matches.value_of(name).unwrap()) {
        Ok(v) => v,
        Err(_) => panic!("missing validator for {}", name),
    }
}

fn arg_or<T: FromStr, F: Fn() -> T>(matches: &ArgMatches, name: &str, f: F) -> T {
    match matches.value_of(name) {
        Some(v) => match T::from_str(v) {
            Ok(v) => v,
            Err(_) => panic!("missing validator for {}", name),
        },
        None => f(),
    }
}

fn validate<T, E>(s: String) -> Result<(), String>
where
    T: FromStr<Err = E>,
    E: Display,
{
    match T::from_str(&s) {
        Ok(_) => Ok(()),
        Err(e) => Err(e.to_string()),
    }
}

fn validate_min<T, E>(s: String, min: T) -> Result<(), String>
where
    T: FromStr<Err = E> + PartialOrd + Display,
    E: Display,
{
    match T::from_str(&s) {
        Ok(x) => match min.partial_cmp(&x) {
            Some(Ordering::Equal) | Some(Ordering::Less) => Ok(()),
            _ => Err(format!("must be at least {}", min)),
        },
        Err(e) => Err(e.to_string()),
    }
}

fn new_latency_histogram(max_seconds: u64) -> Histogram<u64> {
    Histogram::new_with_bounds(
        Duration::from_micros(1).as_nanos() as u64,
        Duration::from_secs(max_seconds).as_nanos() as u64,
        3,
    )
    .unwrap()
}

fn format_histogram(hist: &Histogram<u64>) -> String {
    let p50 = Duration::from_nanos(hist.value_at_quantile(0.5));
    let p90 = Duration::from_nanos(hist.value_at_quantile(0.9));
    let p99 = Duration::from_nanos(hist.value_at_quantile(0.99));
    let p999 = Duration::from_nanos(hist.value_at_quantile(0.999));
    fn fmt(duration: Duration) -> String {
        format!("{:.3?}", duration)
    }
    format!(
        "p50={:>width$}, p90={:>width$}, p99={:>width$}, p999={:>width$}",
        fmt(p50),
        fmt(p90),
        fmt(p99),
        fmt(p999),
        width = 9
    )
}

fn main() {
    let matches = App::new("Qrono Benchmark Utility")
        .arg(
            Arg::with_name("target")
                .short("t")
                .long("target")
                .default_value("localhost:16379")
                .validator(validate::<HostAndPort, _>)
                .help("Qrono server address")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("count")
                .short("n")
                .long("publish-count")
                .default_value("1000")
                .validator(|x| validate_min::<HumanSize, _>(x, HumanSize(1)))
                .help("Number of values to enqueue")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("size")
                .short("s")
                .long("size")
                .default_value("8")
                .validator(|x| validate_min::<usize, _>(x, 8))
                .help("Size of each value (min 8)")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("consumer_count")
                .short("C")
                .long("consumer-count")
                .default_value("1")
                .validator(|x| validate_min::<u64, _>(x, 1))
                .help("Number of consumers")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("publisher_count")
                .short("P")
                .long("publisher-count")
                .default_value("1")
                .validator(|x| validate_min::<u64, _>(x, 1))
                .help("Number of publishers")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("publish_rate")
                .short("r")
                .long("publish-rate")
                .validator(|x| validate_min::<f64, _>(x, 1.0))
                .help("Publish (enqueue) rate")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("queue_names")
                .short("q")
                .long("queue-names")
                .help("Queue names (default random)")
                .multiple(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("pipeline")
                .long("pipeline")
                .help("Pipeline length (i.e. batch size)")
                .default_value("1")
                .validator(|x| validate_min::<u64, _>(x, 1))
                .takes_value(true),
        )
        .arg(
            Arg::with_name("mode")
                .long("mode")
                .takes_value(true)
                .default_value("publish-and-consume")
                .possible_values(&[
                    "publish-then-delete",
                    "publish-and-consume",
                    "publish-then-consume",
                ]),
        )
        .get_matches();

    let names = match matches.values_of("queue_names") {
        Some(args) => args
            .flat_map(|s| s.split(&[',', ' '][..]))
            .map(|s| s.to_string())
            .collect::<Vec<_>>(),
        None => {
            let name = format!("q-{}", rand::random::<u32>());
            println!("Generated queue name: {}", name);
            vec![name]
        }
    };

    let benchmark = Benchmark {
        target: arg(&matches, "target"),
        queue_names: names.clone(),
        count: arg(&matches, "count"),
        size: arg(&matches, "size"),
        consumer_count: arg(&matches, "consumer_count"),
        publisher_count: arg(&matches, "publisher_count"),
        publish_rate: arg_or(&matches, "publish_rate", || -1.0),
        mode: {
            let mode = matches.value_of("mode").unwrap();
            match mode {
                "publish-then-delete" => Mode::PublishThenDelete,
                "publish-then-consume" => Mode::PublishThenConsume,
                "publish-and-consume" => Mode::PublishAndConsume,
                _ => panic!("unsupported mode {:?}", mode),
            }
        },
        // Latency histograms
        hist_enqueue: Mutex::new(new_latency_histogram(60)),
        hist_dequeue: Mutex::new(new_latency_histogram(60)),
        hist_release: Mutex::new(new_latency_histogram(60)),
        hist_end_to_end: Mutex::new(new_latency_histogram(300)),
        pipeline: arg(&matches, "pipeline"),
        base_time: Instant::now(),
    };

    benchmark.run().unwrap();
    if names.len() > 1 {
        let total_count = names.len() as u64 * benchmark.count.0;
        let total_time = benchmark.base_time.elapsed();
        println!(
            "Overall rate: count={}, duration={:?}, rate={:.3}",
            total_count,
            total_time,
            total_count as f64 / total_time.as_secs_f64(),
        );
    }

    println!(
        "Enqueue:    {}",
        format_histogram(&benchmark.hist_enqueue.lock().unwrap())
    );
    println!(
        "Dequeue:    {}",
        format_histogram(&benchmark.hist_dequeue.lock().unwrap())
    );
    println!(
        "Release:    {}",
        format_histogram(&benchmark.hist_release.lock().unwrap())
    );
    println!(
        "End-to-end: {}",
        format_histogram(&benchmark.hist_end_to_end.lock().unwrap())
    );
}
