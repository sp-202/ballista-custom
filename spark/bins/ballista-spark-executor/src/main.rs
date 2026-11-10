//! Ballista executor with Spark support.
//!
//! Accepts the same flags as the stock `ballista-executor`. Tasks are decoded
//! with [`SparkPhysicalCodec`], and task sessions carry the extensions Spark
//! operators read at run time (see `ballista_spark_session`). The memory pool,
//! spill directory and work directory behave exactly as in the stock executor.

use std::net::SocketAddr;
use std::sync::Arc;

use ballista_core::error::BallistaError;
use ballista_executor::config::Config;
use ballista_executor::executor_process::{ExecutorProcessConfig, start_executor_process};
use ballista_executor::health::spawn_health_server;
use ballista_spark_codec::SparkPhysicalCodec;
use ballista_spark_session::{execution_session_config, object_store_registry};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
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
    let health_addr: SocketAddr = format!("{}:{}", opt.bind_host, opt.bind_health_port)
        .parse()
        .map_err(|e| BallistaError::General(format!("invalid --bind-health-port address: {e}")))?;

    let mut config: ExecutorProcessConfig = opt.try_into()?;
    config.override_physical_codec = Some(SparkPhysicalCodec::shared());
    config.override_config_producer = Some(Arc::new(execution_session_config));
    // Ballista's default runtime, with Sail's object stores (BS-011).
    let registry = object_store_registry();
    let work_dir = config.work_dir.clone();
    config.override_runtime_producer = Some(Arc::new(move |_| {
        let builder = RuntimeEnvBuilder::new().with_object_store_registry(registry.clone());
        match &work_dir {
            Some(dir) => builder.with_temp_file_path(dir.clone()),
            None => builder,
        }
        .build_arc()
    }));

    let log_filter = std::env::var(EnvFilter::DEFAULT_ENV)
        .map(EnvFilter::new)
        .unwrap_or_else(|_| EnvFilter::new(config.special_mod_log_level.clone()));
    tracing_subscriber::fmt()
        .with_ansi(false)
        .with_env_filter(log_filter)
        .init();
    // Mutual TLS for task launch, shuffle reads and scheduler calls (BALLISTA_TLS_*).
    ballista_core::security::init_from_env()?;

    // `/healthz` (liveness) and `/readyz` (heartbeat state), as in the stock binary.
    let (health_shutdown_tx, health_shutdown_rx) = tokio::sync::oneshot::channel();
    let health_handle = spawn_health_server(health_addr, config.health.clone(), health_shutdown_rx);

    let result = start_executor_process(Arc::new(config)).await;

    let _ = health_shutdown_tx.send(());
    if let Err(e) = health_handle.await {
        log::warn!("health server task join error: {e}");
    }
    result
}
