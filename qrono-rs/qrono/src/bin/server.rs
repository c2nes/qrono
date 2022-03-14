use log::info;
use qrono::id_generator::IdGenerator;
use qrono::redis::server::RedisServer;
use qrono::scheduler::{Scheduler, StaticPool};
use qrono::service::Qrono;
use qrono::timer;
use qrono::working_set::WorkingSet;
use rayon::ThreadPoolBuilder;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use std::{fs, io};
use structopt::StructOpt;

#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

// TODO:
//  - Error handling (audit unwrap calls)
//  - Working item timeouts and TTLs
//  - Metrics & instrumentation

#[derive(Debug, StructOpt)]
#[structopt(name = "qrono")]
struct Opts {
    /// Listen address
    #[structopt(long, default_value = "0.0.0.0:16389")]
    listen: String,

    /// Data directory
    #[structopt(long, parse(from_os_str), default_value = "/tmp/qrono")]
    data: PathBuf,

    /// Number of CPU worker threads to use
    #[structopt(long)]
    workers: Option<usize>,

    /// Use Rayon thread pools.
    #[structopt(long)]
    rayon: bool,

    /// Number of working set stripes.
    #[structopt(long, default_value = "1")]
    working_set_stripes: usize,

    /// Period between WAL syncs in milliseconds (-1 to disable).
    #[structopt(long, default_value = "1000")]
    wal_sync_period: i64,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    env_logger::builder().format_timestamp_micros().init();

    let opts: Opts = Opts::from_args();

    let start = Instant::now();
    info!("Starting...");

    fs::create_dir_all(&opts.data)?;

    let scheduler = if opts.rayon {
        Scheduler::new(
            ThreadPoolBuilder::new()
                .num_threads(opts.workers.unwrap_or(0))
                .build()
                .unwrap(),
        )
    } else {
        Scheduler::new(StaticPool::new(opts.workers.unwrap_or_else(|| {
            let n = num_cpus::get();
            info!("Using {} scheduler threads.", n);
            n
        })))
    };

    let timer = timer::Scheduler::new();
    timer.start();

    let deletion_scheduler = Scheduler::new(StaticPool::new(1));
    let id_generator = IdGenerator::new(opts.data.join("id"), scheduler.clone()).unwrap();
    let working_set_scheduler = Scheduler::new(StaticPool::new(1));
    let working_set_dir = opts.data.join("working");
    let working_set_stripes = (0..opts.working_set_stripes)
        .map(|_| (working_set_dir.clone(), working_set_scheduler.clone()))
        .collect::<Vec<_>>();
    let working_set = WorkingSet::new(working_set_stripes).unwrap();
    let wal_sync_period = if opts.wal_sync_period < 0 {
        None
    } else {
        Some(Duration::from_millis(opts.wal_sync_period as u64))
    };
    let qrono = Qrono::new(
        scheduler.clone(),
        timer,
        id_generator,
        working_set,
        opts.data.join("queues"),
        deletion_scheduler,
        wal_sync_period,
    );

    // Start HTTP server in background.
    tokio::spawn(qrono::http::run(qrono.clone()));

    info!("Start up completed in {:?}.", start.elapsed());
    RedisServer::new(&qrono, &scheduler).run(opts.listen)
}
