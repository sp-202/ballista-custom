//! Session setup for running Spark plans on Ballista.
//!
//! Two kinds of sessions exist:
//!
//! - **Planning sessions** live in the Spark Connect server. Sail's Spark
//!   front end builds them; [`spark_session_factory`] adds Ballista's
//!   restricted DataFusion configuration so that the physical plans it
//!   produces are safe to split into Ballista stages.
//! - **Execution sessions** live in the scheduler and executors.
//!   [`execution_session_config`] is Ballista's configuration plus the session
//!   extensions that Sail operators read while they run.

use std::sync::{Arc, OnceLock};

use ballista_core::extension::{SessionConfigExt, SessionConfigHelperExt};
use datafusion::common::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::SessionConfig;
use sail_common::config::AppConfig;
use sail_common::runtime::RuntimeHandle;
use sail_common_datafusion::session::repartition::RepartitionBufferConfig;
use sail_delta_lake::session_extension::DeltaTableCache;
use sail_session::session_factory::{
    ServerSessionFactory, ServerSessionInfo, ServerSessionMutator, SessionFactory,
};
use sail_spark_connect::session_manager::SparkSessionMutator;

/// Sail's Spark session mutator, followed by Ballista's restrictions.
///
/// Sail applies this mutator after its own execution, Parquet and optimizer
/// settings, so the restrictions take precedence. They must match what
/// Ballista enforces for its own plans, e.g.: no physical scalar subqueries
/// (their shared result container cannot cross stages), no dynamic filter
/// pushdown (a filter produced in another stage would never arrive and the
/// scan would block), no round-robin repartition and no Utf8View types.
pub struct BallistaSparkSessionMutator {
    spark: SparkSessionMutator,
}

impl BallistaSparkSessionMutator {
    pub fn new(config: Arc<AppConfig>) -> Self {
        let default_conf = SERVER_SPARK_CONF.get().cloned().unwrap_or_default();
        Self {
            spark: SparkSessionMutator::new(config).with_default_conf(default_conf),
        }
    }
}

/// Spark configuration the server applies to every session (`ballista-connect --conf`).
static SERVER_SPARK_CONF: OnceLock<Vec<(String, String)>> = OnceLock::new();

/// Sets the Spark configuration applied to every new session, like `--conf` for Spark's
/// `start-connect-server.sh`. Call once, before the server starts; later calls are ignored
/// because the session factory is a plain function that cannot carry its own settings.
pub fn set_server_spark_conf(conf: Vec<(String, String)>) {
    let _ = SERVER_SPARK_CONF.set(conf);
}

impl ServerSessionMutator for BallistaSparkSessionMutator {
    fn mutate_config(
        &self,
        config: SessionConfig,
        info: &ServerSessionInfo,
    ) -> Result<SessionConfig> {
        let config = self.spark.mutate_config(config, info)?;
        Ok(config
            .ballista_restricted_configuration()
            // Spark plans reach the scheduler as physical plans; plan them
            // adaptively like Ballista's own SQL jobs (BS-007).
            .with_ballista_adaptive_physical_plans(true))
    }

    fn mutate_state(
        &self,
        builder: SessionStateBuilder,
        info: &ServerSessionInfo,
    ) -> Result<SessionStateBuilder> {
        self.spark.mutate_state(builder, info)
    }

    fn mutate_runtime_env(
        &self,
        builder: RuntimeEnvBuilder,
        info: &ServerSessionInfo,
    ) -> Result<RuntimeEnvBuilder> {
        self.spark.mutate_runtime_env(builder, info)
    }
}

/// Session factory for the Spark Connect server
/// (a `sail_session::session_manager::ServerSessionFactoryFn`).
pub fn spark_session_factory(
    config: Arc<AppConfig>,
    runtime: RuntimeHandle,
) -> Box<dyn SessionFactory<ServerSessionInfo>> {
    let mutator = Box::new(BallistaSparkSessionMutator::new(config.clone()));
    Box::new(ServerSessionFactory::new(config, runtime, mutator))
}

/// Session configuration for Ballista schedulers and executors
/// (a `ballista_core::ConfigProducer`).
///
/// Mirrors the extensions of Sail's worker sessions
/// (`sail_session::session_factory::WorkerSessionFactory`) on top of
/// Ballista's own configuration.
pub fn execution_session_config() -> SessionConfig {
    SessionConfig::new_with_ballista()
        .with_extension(Arc::new(DeltaTableCache::default()))
        .with_extension(Arc::new(RepartitionBufferConfig::default()))
}
