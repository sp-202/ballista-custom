//! Executes Spark plans on Ballista.
//!
//! Sail's Spark front end parses, resolves and plans each Spark Connect
//! request, then hands the physical plan to a `JobRunner`.
//! [`BallistaJobRunner`] runs it:
//!
//! - plans without driver-only operators (all queries) are submitted to the
//!   Ballista scheduler as one physical-plan job;
//! - plans with driver-only operators (catalog commands, commits, ...) run in
//!   the Spark Connect server, with their distributed parts submitted as
//!   separate Ballista jobs (see [`placement`]).
//!
//! [`BallistaJobRunnerFactory`] creates one runner per Spark session; the
//! Spark session id is also the Ballista session id.

pub mod placement;
pub mod remote;
pub mod submit;

use std::sync::Arc;

use datafusion::common::Result;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::{ExecutionPlan, displayable, execute_stream};
use datafusion::prelude::SessionContext;
use sail_common::actor::ActorSystem;
use sail_common_datafusion::session::job::JobRunner;
use sail_session::error::SessionResult;
use sail_session::session_factory::{
    SessionJobRunner, SessionJobRunnerFactory, SessionJobRunnerInfo,
};

pub use crate::remote::BallistaSubplanExec;
pub use crate::submit::{BallistaClusterOptions, BallistaSubmitter};

/// A `JobRunner` that executes Spark plans on a Ballista cluster.
pub struct BallistaJobRunner {
    submitter: Arc<BallistaSubmitter>,
}

impl BallistaJobRunner {
    pub fn new(options: BallistaClusterOptions, session_id: String) -> Self {
        Self {
            submitter: Arc::new(BallistaSubmitter::new(options, session_id)),
        }
    }
}

#[tonic::async_trait]
impl JobRunner for BallistaJobRunner {
    async fn execute(
        &self,
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream> {
        let session_config = ctx.copied_config();
        if placement::has_driver_stage(&plan) {
            let local = placement::place(plan, &self.submitter, &session_config)?;
            log::debug!(
                "running plan in the Spark Connect server (session {}):\n{}",
                self.submitter.session_id(),
                displayable(local.as_ref()).indent(true)
            );
            execute_stream(local, ctx.task_ctx())
        } else {
            self.submitter.submit(plan, session_config).await
        }
    }

    fn explain(&self, plan: Arc<dyn ExecutionPlan>) -> Result<String> {
        let target = if placement::has_driver_stage(&plan) {
            "Spark Connect server, with distributed parts as Ballista jobs"
        } else {
            "one Ballista job, split into stages by the scheduler"
        };
        Ok(format!(
            "Runs as: {target}\n{}",
            displayable(plan.as_ref()).indent(true)
        ))
    }

    async fn stop(&self) {
        // Jobs are owned by the scheduler; cancelling running jobs when a
        // session closes is follow-up work.
        log::debug!(
            "Ballista job runner stopped for session {}",
            self.submitter.session_id()
        );
    }
}

/// Creates a [`BallistaJobRunner`] for each Spark session.
pub struct BallistaJobRunnerFactory {
    options: BallistaClusterOptions,
}

impl BallistaJobRunnerFactory {
    pub fn new(options: BallistaClusterOptions) -> Self {
        Self { options }
    }
}

impl SessionJobRunnerFactory for BallistaJobRunnerFactory {
    fn create(
        &mut self,
        _system: &mut ActorSystem,
        info: SessionJobRunnerInfo,
    ) -> SessionResult<SessionJobRunner> {
        Ok(SessionJobRunner::new(Box::new(BallistaJobRunner::new(
            self.options.clone(),
            info.session_id,
        ))))
    }
}
