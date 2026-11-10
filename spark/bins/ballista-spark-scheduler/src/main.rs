//! Ballista scheduler with Spark support.
//!
//! Accepts the same flags as the stock `ballista-scheduler`. In addition it
//! decodes and re-encodes plans containing Spark operators and functions
//! (planned by `ballista-connect`) using [`SparkPhysicalCodec`].

use std::sync::Arc;

use ballista_core::error::BallistaError;
use ballista_core::utils::shutdown_signal;
use ballista_scheduler::cluster::BallistaCluster;
use ballista_scheduler::config::{Config, SchedulerConfig};
use ballista_scheduler::scheduler_process::start_server_with_shutdown;
use ballista_spark_codec::SparkPhysicalCodec;
use ballista_core::utils::default_session_builder;
use ballista_spark_session::{execution_session_config, object_store_registry};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::execution::session_state::SessionStateBuilder;
use clap::Parser;
use tracing_subscriber::EnvFilter;

fn main() -> ballista_core::error::Result<()> {
    // Before any threads exist: embedded Python for Spark operators/UDFs.
    ballista_spark_python::bootstrap()
        .map_err(|e| BallistaError::General(format!("failed to initialize Python: {e}")))?;
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run())
}

async fn run() -> ballista_core::error::Result<()> {
    let opt = Config::parse();

    let log_filter = std::env::var(EnvFilter::DEFAULT_ENV)
        .map(EnvFilter::new)
        .unwrap_or_else(|_| EnvFilter::new(opt.log_level_setting.clone()));
    tracing_subscriber::fmt()
        .with_ansi(false)
        .with_env_filter(log_filter)
        .init();
    // Mutual TLS on the gRPC port, HTTPS and a token on the web UI (BALLISTA_TLS_*,
    // BALLISTA_UI_TOKEN).
    ballista_core::security::init_from_env()?;

    let addr = format!("{}:{}", opt.bind_host, opt.bind_port)
        .parse()
        .map_err(|e: std::net::AddrParseError| BallistaError::Configuration(e.to_string()))?;

    let mut config: SchedulerConfig = opt.try_into()?;
    config.override_physical_codec = Some(SparkPhysicalCodec::shared());
    config.override_config_producer = Some(Arc::new(execution_session_config));
    // Ballista's default session, with Sail's object stores, so table paths on
    // S3 and other stores resolve on the scheduler too (BS-011).
    let registry = object_store_registry();
    config.override_session_builder = Some(Arc::new(move |session_config| {
        let state = default_session_builder(session_config)?;
        let runtime = RuntimeEnvBuilder::new()
            .with_object_store_registry(registry.clone())
            .build_arc()?;
        Ok(SessionStateBuilder::new_from_existing(state)
            .with_runtime_env(runtime)
            .build())
    }));

    let cluster = BallistaCluster::new_from_config(&config).await?;
    start_server_with_shutdown(cluster, addr, Arc::new(config), shutdown_signal).await
}
