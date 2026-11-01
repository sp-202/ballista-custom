use std::sync::Arc;
use std::time::Duration;

use datafusion::common::{Result, internal_datafusion_err};
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::SessionConfig;
use sail_common::actor::ActorSystem;
use sail_common::config::AppConfig;
use sail_common::runtime::RuntimeHandle;
use sail_common_datafusion::catalog::display::DefaultCatalogDisplay;
use sail_common_datafusion::session::config::SessionConfigService;
use sail_common_datafusion::session::plan::PlanService;
use sail_plan::catalog::SparkCatalogObjectDisplay;
use sail_plan::formatter::SparkPlanFormatter;
use sail_session::session_factory::{
    ServerSessionFactory, ServerSessionInfo, ServerSessionMutator, SessionFactory,
    SessionJobRunnerFactory,
};
use sail_session::session_manager::{
    ServerSessionFactoryFn, SessionManager, create_session_manager,
    create_session_manager_with_job_runner_factory,
};

use crate::error::SparkResult;
use crate::config::ConfigKeyValue;
use crate::session::{SparkSession, SparkSessionOptions};

/// The session mutator that makes a server session a Spark session.
///
/// An embedder that needs to extend server sessions (for example, to register
/// a data source or a table function) can wrap this mutator in its own
/// [`ServerSessionMutator`] and pass the resulting session factory to
/// [`create_spark_session_manager_with_factory`].
pub struct SparkSessionMutator {
    config: Arc<AppConfig>,
    default_conf: Vec<(String, String)>,
}

impl SparkSessionMutator {
    pub fn new(config: Arc<AppConfig>) -> Self {
        Self {
            config,
            default_conf: vec![],
        }
    }

    /// Spark configuration set on every new session before the client's own settings,
    /// like `--conf` for Spark's `start-connect-server.sh` (e.g. `spark.sql.warehouse.dir`).
    pub fn with_default_conf(mut self, default_conf: Vec<(String, String)>) -> Self {
        self.default_conf = default_conf;
        self
    }
}

impl ServerSessionMutator for SparkSessionMutator {
    fn mutate_config(
        &self,
        config: SessionConfig,
        info: &ServerSessionInfo,
    ) -> Result<SessionConfig> {
        let plan_service = PlanService::new(
            Box::new(DefaultCatalogDisplay::<SparkCatalogObjectDisplay>::default()),
            Box::new(SparkPlanFormatter),
        );
        let spark = Arc::new(
            SparkSession::try_new(
                info.session_id.clone(),
                info.user_id.clone(),
                SparkSessionOptions {
                    execution_heartbeat_interval: Duration::from_secs(
                        self.config.spark.execution_heartbeat_interval_secs,
                    ),
                },
            )
            .map_err(|e| internal_datafusion_err!("{e}"))?,
        );
        spark
            .set_config(
                self.default_conf
                    .iter()
                    .map(|(key, value)| ConfigKeyValue {
                        key: key.clone(),
                        value: Some(value.clone()),
                    })
                    .collect(),
            )
            .map_err(|e| internal_datafusion_err!("{e}"))?;
        let config_service = SessionConfigService::new(spark.clone());
        // Seed the DataFusion `execution.time_zone` from the resolved Spark session timezone at
        // session creation. `SparkSession::try_new` sets `spark.sql.session.timeZone` to the
        // system timezone, but nothing otherwise copies it into `SessionConfig`; without this
        // seed a session that never calls `conf.set` leaves `execution.time_zone = None`, so
        // Parquet schema inference treats a non-UTC server as UTC. `handle_config_set` and
        // `handle_config_unset` keep the two in sync thereafter.
        let session_timezone = spark
            .plan_config()
            .map_err(|e| internal_datafusion_err!("{e}"))?
            .session_timezone
            .to_string();
        let mut config = config;
        config.options_mut().execution.time_zone = Some(session_timezone);
        Ok(config
            .with_extension(Arc::new(plan_service))
            .with_extension(spark)
            .with_extension(Arc::new(config_service)))
    }

    fn mutate_state(
        &self,
        builder: SessionStateBuilder,
        _info: &ServerSessionInfo,
    ) -> Result<SessionStateBuilder> {
        Ok(builder)
    }

    fn mutate_runtime_env(
        &self,
        builder: RuntimeEnvBuilder,
        _info: &ServerSessionInfo,
    ) -> Result<RuntimeEnvBuilder> {
        Ok(builder)
    }
}

/// The session factory used by the Spark Connect server by default.
pub fn create_spark_session_factory(
    config: Arc<AppConfig>,
    runtime: RuntimeHandle,
) -> Box<dyn SessionFactory<ServerSessionInfo>> {
    let mutator = Box::new(SparkSessionMutator::new(config.clone()));
    Box::new(ServerSessionFactory::new(config, runtime, mutator))
}

pub async fn create_spark_session_manager(
    config: Arc<AppConfig>,
    runtime: RuntimeHandle,
    system: &mut ActorSystem,
) -> SparkResult<SessionManager> {
    create_spark_session_manager_with_factory(config, runtime, system, create_spark_session_factory)
        .await
}

/// Creates the session manager with a session factory chosen by the caller.
/// The factory is expected to build on [`SparkSessionMutator`].
pub async fn create_spark_session_manager_with_factory(
    config: Arc<AppConfig>,
    runtime: RuntimeHandle,
    system: &mut ActorSystem,
    session_factory_fn: ServerSessionFactoryFn,
) -> SparkResult<SessionManager> {
    Ok(create_session_manager(
        config.clone(),
        runtime,
        session_factory_fn,
        Duration::from_secs(config.spark.session_timeout_secs),
        system,
    )
    .await?)
}

/// Creates the session manager with the session factory and the job runner
/// factory both chosen by the caller, so Spark plans execute on Ballista.
pub async fn create_spark_session_manager_with_factories(
    config: Arc<AppConfig>,
    runtime: RuntimeHandle,
    system: &mut ActorSystem,
    session_factory_fn: ServerSessionFactoryFn,
    job_runner_factory: Box<dyn SessionJobRunnerFactory>,
) -> SparkResult<SessionManager> {
    Ok(create_session_manager_with_job_runner_factory(
        config.clone(),
        runtime,
        session_factory_fn,
        job_runner_factory,
        Duration::from_secs(config.spark.session_timeout_secs),
        system,
    )
    .await?)
}
