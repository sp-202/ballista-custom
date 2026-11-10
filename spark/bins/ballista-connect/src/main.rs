//! Spark Connect server for Ballista.
//!
//! PySpark connects with `SparkSession.builder.remote("sc://host:15002")`.
//! Requests are parsed and planned by the Spark front end (copied from Sail),
//! and every physical plan is executed as a job on the Ballista scheduler given
//! by `--scheduler-url`.
//!
//! Security, as in Spark Connect: with `SPARK_CONNECT_AUTHENTICATE_TOKEN` (or
//! `--conf spark.connect.authenticate.token=...`) set, clients must send
//! `authorization: Bearer <token>` (PySpark: `sc://host:15002/;token=...`).
//! `--tls` serves TLS with the cluster certificate, which PySpark requires
//! before it sends a token to any host but localhost. The server's own calls
//! to the scheduler and executors use the cluster's mutual TLS when
//! `BALLISTA_TLS_*` is set.
//!
//! Sail settings (`SAIL_*` environment variables) still apply to planning,
//! except the execution mode, which is always `local` here: Ballista, not
//! Sail, owns the cluster, so no Sail driver or workers are started.

use std::net::IpAddr;
use std::sync::Arc;

use ballista_spark_runner::{BallistaClusterOptions, BallistaJobRunnerFactory};
use ballista_spark_session::spark_session_factory;
use clap::Parser;
use log::{info, warn};
use sail_common::config::{AppConfig, ExecutionMode};
use sail_common::runtime::RuntimeManager;
use sail_common::server::ServerSecurity;
use sail_spark_connect::entrypoint::serve_with_factories_secure;
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
    /// Spark configuration for every session, as `key=value` (repeatable), like
    /// `start-connect-server.sh --conf`. Set `spark.sql.warehouse.dir` to storage that
    /// the server and every executor share.
    #[arg(long = "conf", value_parser = parse_conf)]
    conf: Vec<(String, String)>,
    /// Serve TLS with the cluster certificate (`BALLISTA_TLS_CERT`,
    /// `BALLISTA_TLS_KEY`); clients verify it with the cluster CA. Also enabled
    /// by `BALLISTA_CONNECT_TLS=true`.
    #[arg(long)]
    tls: bool,
}

/// Spark's setting for the Spark Connect token, and its environment variable.
const TOKEN_CONF: &str = "spark.connect.authenticate.token";
const TOKEN_ENV: &str = "SPARK_CONNECT_AUTHENTICATE_TOKEN";

/// Takes the token out of the session settings, so no session can read it back
/// with `spark.conf.get`. The environment variable wins over `--conf`.
fn take_token(conf: &mut Vec<(String, String)>) -> Option<String> {
    let from_conf = conf
        .iter()
        .rev()
        .find(|(key, _)| key == TOKEN_CONF)
        .map(|(_, value)| value.clone());
    conf.retain(|(key, _)| key != TOKEN_CONF);
    std::env::var(TOKEN_ENV)
        .ok()
        .or(from_conf)
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty())
}

fn env_flag(name: &str) -> bool {
    std::env::var(name)
        .is_ok_and(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
}

/// What Spark Connect clients must present, and over what.
fn client_security(
    args: &Args,
    token: Option<String>,
) -> Result<ServerSecurity, Box<dyn std::error::Error>> {
    let cluster = ballista_core::security::init_from_env()?;
    let tls = if args.tls || env_flag("BALLISTA_CONNECT_TLS") {
        Some(cluster.public_server_tls_config().ok_or(
            "--tls needs the cluster certificate: set BALLISTA_TLS_CERT, \
             BALLISTA_TLS_KEY and BALLISTA_TLS_CA",
        )?)
    } else {
        None
    };
    match &token {
        Some(token) if token.len() < 16 => {
            warn!("The Spark Connect token is shorter than 16 characters: use a random one")
        }
        Some(_) => info!("Spark Connect clients must present the authentication token"),
        None if !args.ip.is_loopback() => warn!(
            "Spark Connect on {} accepts any client: set {TOKEN_ENV} to require a token",
            args.ip
        ),
        None => {}
    }
    if tls.is_none() && token.is_some() && !args.ip.is_loopback() {
        info!(
            "Spark Connect is not using TLS: tokens cross the network in clear text, and \
             PySpark sends a token only to localhost or over TLS (--tls)"
        );
    }
    Ok(ServerSecurity {
        tls,
        bearer_token: token.map(Arc::from),
    })
}

fn parse_conf(s: &str) -> Result<(String, String), String> {
    match s.split_once('=') {
        Some((key, value)) if !key.trim().is_empty() => {
            Ok((key.trim().to_string(), value.trim().to_string()))
        }
        _ => Err(format!("expected key=value, got `{s}`")),
    }
}

/// Warns when managed tables would be written to storage that only one machine sees.
/// Executors write table files, and the server lists them when planning reads, so a
/// node-local warehouse makes tables written on one executor invisible to queries
/// (Spark has the same requirement: the warehouse must be on shared storage).
fn check_warehouse(conf: &[(String, String)]) {
    let warehouse = conf
        .iter()
        .rev()
        .find(|(key, _)| key == "spark.sql.warehouse.dir")
        .map(|(_, value)| value.as_str());
    match warehouse {
        None => warn!(
            "spark.sql.warehouse.dir is not set: managed tables go to `spark-warehouse` \
             relative to each process's working directory, so executors and this server \
             do not see the same files. Set --conf spark.sql.warehouse.dir to shared storage."
        ),
        Some(dir) if !dir.contains("://") || dir.starts_with("file://") => info!(
            "Warehouse {dir} is a local path: it must be the same shared mount on this \
             server and every executor."
        ),
        Some(dir) => info!("Warehouse: {dir}"),
    }
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
    let mut args = Args::parse();
    let token = take_token(&mut args.conf);
    ballista_spark_session::set_server_spark_conf(args.conf.clone());

    let mut config = AppConfig::load()?;
    config.mode = ExecutionMode::Local;
    let config = Arc::new(config);

    let runtime = RuntimeManager::try_new(&config.runtime)?;
    let primary = runtime.handle().primary().clone();
    let _telemetry = primary.block_on(async { TelemetryGuard::try_new(&config) })?;

    check_warehouse(&args.conf);
    let security = client_security(&args, token)?;

    let job_runner_factory = Box::new(BallistaJobRunnerFactory::new(
        BallistaClusterOptions::new(args.scheduler_url.clone()),
    ));
    let handle = runtime.handle();
    primary.block_on(async move {
        let listener = TcpListener::bind((args.ip, args.port)).await?;
        info!(
            "Starting the Spark Connect server on {}{} (Ballista scheduler: {})",
            listener.local_addr()?,
            if security.tls.is_some() { " with TLS" } else { "" },
            args.scheduler_url
        );
        serve_with_factories_secure(
            listener,
            shutdown(),
            config,
            handle,
            spark_session_factory,
            job_runner_factory,
            security,
        )
        .await
    })
}
