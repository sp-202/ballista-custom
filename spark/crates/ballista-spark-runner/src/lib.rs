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
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, Partitioning, displayable, execute_stream};
use datafusion::prelude::SessionContext;
use sail_common::actor::ActorSystem;
use sail_common_datafusion::session::job::JobRunner;
use sail_physical_plan::coalesce::CoalesceExec;
use sail_physical_plan::repartition::ExplicitRepartitionExec;
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
        let plan = round_robin_as_repartition(plan)?;
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

    /// Called when the Spark session is released: cancels its Ballista jobs
    /// that are still running (BS-012).
    async fn stop(&self) {
        self.submitter.cancel_all().await;
        log::debug!(
            "Ballista job runner stopped for session {}",
            self.submitter.session_id()
        );
    }
}

/// Rewrites Sail's operators that change the partition count inside a stage
/// to DataFusion's round-robin `RepartitionExec`, at which Ballista splits stages:
///
/// - round-robin `ExplicitRepartitionExec` (`df.repartition(n)`), which Sail
///   keeps for its own job graph (BS-010);
/// - `CoalesceExec` (`df.coalesce(n)`), which merges input partitions within a
///   stage. Ballista gives each task a slice of a stage's partitions and assumes
///   operators keep partition numbers, so a task would read the wrong inputs and
///   lose rows. Spark coalesces without a shuffle; here it costs one.
fn round_robin_as_repartition(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let round_robin = |input: &Arc<dyn ExecutionPlan>, n: usize| -> Result<_> {
        Ok(Transformed::yes(Arc::new(RepartitionExec::try_new(
            Arc::clone(input),
            Partitioning::RoundRobinBatch(n),
        )?) as Arc<dyn ExecutionPlan>))
    };
    Ok(plan
        .transform_up(|node| {
            if let Some(explicit) = node.downcast_ref::<ExplicitRepartitionExec>()
                && let Partitioning::RoundRobinBatch(n) =
                    explicit.properties().output_partitioning()
            {
                return round_robin(explicit.input(), *n);
            }
            if let Some(coalesce) = node.downcast_ref::<CoalesceExec>() {
                return round_robin(coalesce.input(), coalesce.output_partitions());
            }
            Ok(Transformed::no(node))
        })?
        .data)
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
