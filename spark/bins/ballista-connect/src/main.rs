//! Spark Connect server for Ballista.
//!
//! PySpark connects with `SparkSession.builder.remote("sc://host:15002")`.
//! Requests are parsed and planned by the Spark front end (copied from Sail),
//! and every physical plan is executed as a job on the Ballista scheduler given
//! by `--scheduler-url`.
//!
//! Sail settings (`SAIL_*` environment variables) still apply to planning,
//! except the execution mode, which is always `local` here: Ballista, not
//! Sail, owns the cluster, so no Sail driver or workers are started.

use std::net::IpAddr;
use std::sync::Arc;

use ballista_spark_runner::{BallistaClusterOptions, BallistaJobRunnerFactory};
use ballista_spark_session::spark_session_factory;
use clap::Parser;
use log::info;
use sail_common::config::{AppConfig, ExecutionMode};
use sail_common::runtime::RuntimeManager;
use sail_spark_connect::entrypoint::serve_with_factories;
use sail_telemetry::telemetry::{init_telemetry, shutdown_telemetry};
use sail_telemetry::{ResourceKind, ResourceOptions};
use tokio::net::TcpListener;

#[derive(Debug, Parser)]
#[command(version, about)]
struct Args {
    /// Address to listen on for Spark Connect clients.
    #[arg(long, default_value = "0.0.0.0")]
    ip: IpAddr,
    /// Spark Connect port (Spark's default is 15002).
    #[arg(long, default_value_t = 15002)]
    port: u16,
    /// Ballista scheduler gRPC URL.
    #[arg(long, default_value = "http://localhost:50050")]
    scheduler_url: String,
}

/// Initializes Sail telemetry (also required by its session manager) and
/// shuts it down on drop.
struct TelemetryGuard;

impl TelemetryGuard {
    fn try_new(config: &AppConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let resource = ResourceOptions {
            kind: ResourceKind::Server,
        };
        init_telemetry(&config.telemetry, &config.catalog.system, resource)?;
        Ok(Self)
    }
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        shutdown_telemetry();
    }
}

async fn shutdown() {
    let _ = tokio::signal::ctrl_c().await;
    info!("Shutting down the Spark Connect server...");
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Before any threads exist: the Spark front end calls into Python.
    ballista_spark_python::bootstrap()?;
    let args = Args::parse();

    let mut config = AppConfig::load()?;
    config.mode = ExecutionMode::Local;
    let config = Arc::new(config);

    let runtime = RuntimeManager::try_new(&config.runtime)?;
    let primary = runtime.handle().primary().clone();
    let _telemetry = primary.block_on(async { TelemetryGuard::try_new(&config) })?;

    let job_runner_factory = Box::new(BallistaJobRunnerFactory::new(
        BallistaClusterOptions::new(args.scheduler_url.clone()),
    ));
    let handle = runtime.handle();
    primary.block_on(async move {
        let listener = TcpListener::bind((args.ip, args.port)).await?;
        info!(
            "Starting the Spark Connect server on {} (Ballista scheduler: {})",
            listener.local_addr()?,
            args.scheduler_url
        );
        serve_with_factories(
            listener,
            shutdown(),
            config,
            handle,
            spark_session_factory,
            job_runner_factory,
        )
        .await
    })
}
