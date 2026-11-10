// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Standalone Ballista history server binary: loads completed event logs from
//! a directory and serves the same `/api/*` responses the live scheduler does,
//! so the existing TUI can connect to it unchanged. It also serves the web UI at `/`.

use ballista_core::error::{BallistaError, Result};
use ballista_scheduler::history::{HistoryStore, history_router, spawn_refresh_task};
use ballista_scheduler::http_security::{TlsListener, require_ui_token};
use clap::Parser;
use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tracing_subscriber::EnvFilter;

#[derive(Debug, clap::Parser)]
#[command(
    name = "ballista-history-server",
    version,
    about = "Ballista history server"
)]
struct Args {
    /// Directory containing per-job event logs.
    #[arg(long)]
    event_log_dir: PathBuf,
    /// Host to bind the HTTP server to.
    #[arg(long, default_value = "0.0.0.0")]
    bind_host: String,
    /// Port to bind the HTTP server to.
    #[arg(long, default_value_t = 50060)]
    bind_port: u16,
    /// How often to rescan the event-log directory for jobs that finished
    /// since the last pass, in seconds. Set to 0 to scan only at startup.
    #[arg(long, default_value_t = 10)]
    update_interval_seconds: u64,
}

fn main() -> Result<()> {
    let rust_log = env::var(EnvFilter::DEFAULT_ENV);
    let log_filter = EnvFilter::new(rust_log.unwrap_or_else(|_| "info".to_string()));
    tracing_subscriber::fmt()
        .with_ansi(false)
        .with_writer(std::io::stdout)
        .with_env_filter(log_filter)
        .init();

    let args = Args::parse();
    // HTTPS and the UI token, as on the scheduler's web UI (spark.ssl.historyServer
    // and the history server's ui.filters in Spark).
    ballista_core::security::init_from_env()?;

    // `HistoryStore::load` walks the log directory with blocking file I/O, and
    // how long it takes scales with the number of stored jobs. Run it here,
    // before the runtime exists, rather than parking a runtime worker on it.
    let store = Arc::new(HistoryStore::load(&args.event_log_dir)?);
    tracing::info!(
        "Indexed {} completed job(s) from {}",
        store.len(),
        args.event_log_dir.display()
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_io()
        .enable_time()
        .build()
        .map_err(BallistaError::IoError)?;

    runtime.block_on(serve(args, store))
}

async fn serve(args: Args, store: Arc<HistoryStore>) -> Result<()> {
    // Schedulers keep writing to this directory while the history server is
    // up, so without a rescan the list is frozen at whatever had finished when
    // the process started.
    if args.update_interval_seconds > 0 {
        let interval = Duration::from_secs(args.update_interval_seconds);
        tracing::info!("Rescanning the event-log directory every {interval:?}");
        spawn_refresh_task(Arc::clone(&store), interval);
    } else {
        tracing::info!(
            "Rescanning is disabled; only jobs indexed at startup will be served"
        );
    }

    // The same web UI as the scheduler's, over the replayed `/api` responses.
    let security = ballista_core::security::current();
    let app = require_ui_token(
        history_router(store).merge(ballista_scheduler::api::web_ui_assets()),
        security.ui_token(),
    );
    let tls = if security.ui_tls() {
        security.rustls_server_config(false)?
    } else {
        None
    };

    let addr: SocketAddr = format!("{}:{}", args.bind_host, args.bind_port)
        .parse()
        .map_err(|e: std::net::AddrParseError| {
            BallistaError::Configuration(e.to_string())
        })?;

    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .map_err(BallistaError::IoError)?;
    tracing::info!(
        "History server listening on {}://{addr}{}",
        if tls.is_some() { "https" } else { "http" },
        if security.ui_token().is_some() {
            " (token required)"
        } else {
            ""
        }
    );

    match tls {
        Some(tls) => {
            axum::serve(TlsListener::new(listener, tls)?, app.into_make_service())
                .await
        }
        None => axum::serve(listener, app.into_make_service()).await,
    }
    .map_err(BallistaError::IoError)
}
