use std::error::Error;
use std::fs;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use log::info;
use rayon::ThreadPoolBuilder;
use structopt::StructOpt;

use tokio::task::JoinHandle;
use tonic::transport::Server;

use qrono::id_generator::IdGenerator;
use qrono::redis::server::RedisServer;
use qrono::scheduler::{Scheduler, StaticPool};
use qrono::service::Qrono;
use qrono::timer;
use qrono::working_set::WorkingSet;
use qrono_promise::{Future, Promise};

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

async fn register_shutdown_handler() {
    let (shutdown_promise, shutdown_future) = Future::new_std();
    let mut shutdown_promise = Some(shutdown_promise);
    ctrlc::set_handler(move || {
        if let Some(shutdown_promise) = shutdown_promise.take() {
            shutdown_promise.complete(())
        }
    })
    .unwrap();
    shutdown_future.await
}

fn build_scheduler(opts: &Opts) -> Scheduler {
    if opts.rayon {
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
    }
}

fn build_qrono_service(opts: &Opts, scheduler: Scheduler) -> std::io::Result<Qrono> {
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
    Ok(Qrono::new(
        scheduler,
        timer,
        id_generator,
        working_set,
        opts.data.join("queues"),
        deletion_scheduler,
        wal_sync_period,
    ))
}

struct Shutdown<T> {
    signal: Promise<()>,
    handler: JoinHandle<T>,
}

impl<T> Shutdown<T> {
    async fn shutdown(self) -> T {
        self.signal.complete(());
        self.handler.await.unwrap()
    }
}

fn start_http_server(opts: &Opts, qrono: Arc<Qrono>) -> Shutdown<Result<(), impl Error>> {
    let server = axum::Server::bind(&opts.http_listen);
    let router = qrono::http::router(qrono);
    let (shutdown, shutdown_signal) = Future::new_std();
    let server_future = tokio::spawn(
        server
            .serve(router.into_make_service())
            .with_graceful_shutdown(shutdown_signal),
    );

    info!("Accepting HTTP connections on {}", &opts.http_listen);
    Shutdown {
        signal: shutdown,
        handler: server_future,
    }
}

fn start_grpc_server(opts: &Opts, qrono: Arc<Qrono>) -> Shutdown<Result<(), impl Error>> {
    let service = qrono::grpc::service(qrono);
    let reflection_service = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(include_bytes!("../grpc/generated/qrono.bin"))
        .build()
        .unwrap();
    let (shutdown, shutdown_signal) = Future::new_std();
    let server_future = tokio::spawn(
        Server::builder()
            .add_service(service)
            .add_service(reflection_service)
            .serve_with_shutdown(opts.grpc_listen, shutdown_signal),
    );
    info!("Accepting gRPC connections on {}", &opts.grpc_listen);
    Shutdown {
        signal: shutdown,
        handler: server_future,
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::builder().format_timestamp_micros().init();
    let shutdown_signal = register_shutdown_handler();
    let opts: Opts = Opts::from_args();
    let start = Instant::now();
    info!("Starting...");

    fs::create_dir_all(&opts.data)?;

    let scheduler = build_scheduler(&opts);
    let qrono = Arc::new(build_qrono_service(&opts, scheduler.clone())?);

    let http_server = start_http_server(&opts, qrono.clone());
    let grpc_server = start_grpc_server(&opts, qrono.clone());
    let redis_server = RedisServer::new(qrono.clone(), &scheduler);
    let redis_handle = redis_server.start(opts.resp2_listen)?;
    info!("Accepting RESP2 connections on {}", &opts.resp2_listen);
    info!("Start up completed in {:?}.", start.elapsed());

    // Wait for shutdown signal
    shutdown_signal.await;

    info!("Shutting down...");
    let shutdown_start = Instant::now();

    redis_handle.shutdown()?;
    http_server.shutdown().await?;
    grpc_server.shutdown().await?;

    if let Ok(qrono) = Arc::try_unwrap(qrono) {
        drop(qrono);
    } else {
        panic!("BUG: All Qrono references should be dropped at this point");
    }

    info!("Shutdown completed in {:?}", shutdown_start.elapsed());

    Ok(())
}
