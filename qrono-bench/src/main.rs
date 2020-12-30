use std::cmp::Ordering;
use std::fmt::Display;
use std::io::Error;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;

use clap::{App, Arg, ArgMatches};
use redis::{
    ConnectionAddr, ConnectionInfo, FromRedisValue, IntoConnectionInfo, RedisResult, Value,
};
use time::Duration;
use tokio::sync::{mpsc, Barrier};
use tokio::task;
use tokio::time;

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
        if s.starts_with("[") {
            if let Some(idx) = s.find("]") {
                let (host, rest) = s.split_at(idx);
                let port: u16 = match rest.strip_prefix("]:").map(str::parse::<u16>) {
                    Some(Ok(port)) => port,
                    _ => return Err("invalid port"),
                };
                return Ok(HostAndPort(host.to_string(), port));
            }
            return Err("invalid IPv6 literal");
        }

        if let Some(idx) = s.find(":") {
            let (host, rest) = s.split_at(idx);
            let port = match rest[1..].parse::<u16>() {
                Ok(port) => port,
                _ => return Err("invalid port"),
            };
            return Ok(HostAndPort(host.to_string(), port));
        }

        return Err("port required");
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

// TODO: Add address
struct Benchmark {
    target: HostAndPort,
    queue_name: String,
    count: u64,
    size: usize,
    consumer_count: u64,
    publish_rate: f64,
    wait_to_consume: bool,
}

impl Benchmark {
    async fn run(&self) -> Result<(), Error> {
        let client = redis::Client::open(&self.target).unwrap();
        let conn = client.get_multiplexed_async_connection().await.unwrap();

        // Make copies of these to avoid issues with Sending &self
        let n = self.count;
        let m = self.consumer_count;

        let (tx, mut rx) = mpsc::unbounded_channel();
        let handle = task::spawn(async move {
            for _ in 0..n {
                rx.recv().await.unwrap();
            }
        });

        // Need to dequeue as many items as are enqueued
        let mut consumers = Vec::new();
        // Consumer barrier. If wait_to_consume is true then wait for all consumers to
        // be ready and for production to be complete. Otherwise, only wait for other consumers.
        let consumer_start = Arc::new(Barrier::new(
            self.consumer_count as usize + if self.wait_to_consume { 1 } else { 0 },
        ));

        for i in 0..m {
            let mut my_count = n / m;
            if i < n % m {
                my_count += 1;
            }
            let mut conn = conn.clone();
            let queue_name = self.queue_name.clone();
            let consumer_start = Arc::clone(&consumer_start);
            let handle = task::spawn(async move {
                consumer_start.wait().await;
                while my_count > 0 {
                    let resp: Option<DequeueResult> = redis::cmd("DEQUEUE")
                        .arg(&queue_name)
                        .query_async(&mut conn)
                        .await
                        .unwrap();
                    if let Some(res) = resp {
                        redis::cmd("RELEASE")
                            .arg(&queue_name)
                            .arg(res.id)
                            .query_async::<_, bool>(&mut conn)
                            .await
                            .unwrap();
                        my_count -= 1;
                    } else {
                        time::sleep(Duration::from_millis(10)).await;
                    }
                }
            });

            consumers.push(handle);
        }

        let start_production = Instant::now();
        let mut rate = time::interval(Duration::from_secs_f64(1.0 / self.publish_rate));
        let value = vec![b'A'; self.size];

        for _ in 0..n {
            rate.tick().await;

            let mut conn = conn.clone();
            let tx = tx.clone();
            let queue_name = self.queue_name.clone();
            let value = value.clone();

            task::spawn(async move {
                redis::cmd("ENQUEUE")
                    .arg(&queue_name)
                    .arg(value)
                    .query_async::<_, EnqueueResult>(&mut conn)
                    .await
                    .unwrap();
                tx.send(()).unwrap();
            });
        }

        // Wait for producers to complete
        handle.await.unwrap();

        // Signal consumer to start if necessary
        if self.wait_to_consume {
            consumer_start.wait().await;
        }

        let produce_duration = start_production.elapsed();
        let per_second = n as f64 / produce_duration.as_secs_f64();
        println!(
            "Producer done; duration={}, rate={}",
            produce_duration.as_secs_f64(),
            per_second
        );

        for consumer in consumers {
            consumer.await.unwrap();
        }

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

#[tokio::main]
async fn main() {
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
                .validator(|x| validate_min::<u64, _>(x, 1))
                .help("Number of values to enqueue")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("size")
                .short("s")
                .long("size")
                .default_value("16")
                .validator(|x| validate_min::<usize, _>(x, 16))
                .help("Size of each value (min 16)")
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
            Arg::with_name("publish_rate")
                .short("r")
                .long("publish-rate")
                .default_value("50")
                .validator(|x| validate_min::<f64, _>(x, 1.0))
                .help("Publish (enqueue) rate")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("queue_name")
                .long("queue-name")
                .help("Queue name (default random)")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("wait_to_consume")
                .long("wait-to-consume")
                .help("Wait for publishing to complete before starting consumers"),
        )
        .get_matches();

    let name = arg_or(&matches, "queue_name", || {
        let name = format!("q-{}", rand::random::<u32>());
        println!("Generated queue name: {}", name);
        name
    });

    let benchmark = Benchmark {
        target: arg(&matches, "target"),
        queue_name: name,
        count: arg(&matches, "count"),
        size: arg(&matches, "size"),
        consumer_count: arg(&matches, "consumer_count"),
        publish_rate: arg(&matches, "publish_rate"),
        wait_to_consume: matches.is_present("wait_to_consume"),
    };

    benchmark.run().await.unwrap();
    println!("Done!");
}
