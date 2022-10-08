use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use log::info;
use rayon::ThreadPoolBuilder;
use structopt::StructOpt;

use tokio::runtime::Builder;
use tonic::transport::Server;

use qrono::id_generator::IdGenerator;
use qrono::redis::server::RedisServer;
use qrono::scheduler::{Scheduler, StaticPool};
use qrono::service::Qrono;
use qrono::timer;
use qrono::working_set::WorkingSet;
use qrono_promise::Future;

#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

// TODO:
//  - Error handling (audit unwrap calls)
//  - Working item timeouts and TTLs
//  - Metrics & instrumentation
//  - Compact large WALs?

#[derive(Debug, StructOpt)]
#[structopt(name = "qrono")]
struct Opts {
    /// RESP2 listen address
    #[structopt(long, default_value = "0.0.0.0:16379")]
    resp2_listen: SocketAddr,

    /// HTTP listen address
    #[structopt(long, default_value = "0.0.0.0:16380")]
    http_listen: SocketAddr,

    /// gRPC listen address
    #[structopt(long, default_value = "0.0.0.0:16381")]
    grpc_listen: SocketAddr,

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

fn register_shutdown_handler() -> Result<Future<()>, ctrlc::Error> {
    let (shutdown_promise, shutdown_future) = Future::new();
    let mut shutdown_promise = Some(shutdown_promise);
    ctrlc::set_handler(move || {
        if let Some(shutdown_promise) = shutdown_promise.take() {
            shutdown_promise.complete(())
        }
    })?;
    Ok(shutdown_future)
}

fn main() -> anyhow::Result<()> {
    let shutdown_future = register_shutdown_handler()?;
    env_logger::builder().format_timestamp_micros().init();
    let opts: Opts = Opts::from_args();
    let start = Instant::now();
    info!("Starting...");

    fs::create_dir_all(&opts.data)?;

    // Scheduler needs to be shut down
    // All schedule tasks need to be canceled
    // Qrono needs to be dropped
    // Timer thread needs to be stopped; all schedules canceled
    // ID generator shut down
    // Working set needs to be shut down

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
    let qrono = Arc::new(Qrono::new(
        scheduler.clone(),
        timer,
        id_generator,
        working_set,
        opts.data.join("queues"),
        deletion_scheduler,
        wal_sync_period,
    ));

    let tokio_runtime = Builder::new_multi_thread().enable_all().build()?;
    let _tokio_runtime_guard = tokio_runtime.enter();

    // Start HTTP server in background.
    let http_server = axum::Server::bind(&opts.http_listen);
    let http_router = qrono::http::router(qrono.clone());
    let (http_shutdown, http_shutdown_signal) = Future::new_std();
    let http_future = http_server
        .serve(http_router.into_make_service())
        .with_graceful_shutdown(http_shutdown_signal);
    info!("Accepting HTTP connections on {}", &opts.http_listen);

    // Start gRPC server
    let grpc_service = qrono::grpc::service(qrono.clone());
    let grpc_reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(include_bytes!("../grpc/generated/qrono.bin"))
        .build()
        .unwrap();
    let (grpc_shutdown, grpc_shutdown_signal) = Future::new_std();
    let grpc_future = tokio::spawn(
        Server::builder()
            .add_service(grpc_service)
            .add_service(grpc_reflection_service)
            .serve_with_shutdown(opts.grpc_listen, grpc_shutdown_signal),
    );
    info!("Accepting gRPC connections on {}", &opts.grpc_listen);

    let redis_server = RedisServer::new(qrono.clone(), &scheduler);
    let redis_handle = redis_server.start(opts.resp2_listen)?;
    info!("Accepting RESP2 connections on {}", &opts.resp2_listen);
    info!("Start up completed in {:?}.", start.elapsed());

    // Wait for shutdown signal
    shutdown_future.take();
    info!("Shutting down...");

    redis_handle.shutdown()?;

    http_shutdown.complete(());
    tokio_runtime.block_on(http_future)?;

    grpc_shutdown.complete(());
    tokio_runtime.block_on(grpc_future)??;

    drop(_tokio_runtime_guard);
    drop(tokio_runtime);

    if let Ok(qrono) = Arc::try_unwrap(qrono) {
        drop(qrono);
    } else {
        panic!("BUG: All Qrono references should be dropped at this point");
    }

    Ok(())
}
