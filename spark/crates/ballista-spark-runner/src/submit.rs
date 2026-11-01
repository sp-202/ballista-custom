//! Submitting physical plans to a Ballista scheduler.

use std::sync::{Arc, Mutex};

use ballista_core::config::BallistaConfig;
use ballista_core::execution_plans::{JobCancelHandle, submit_physical_plan};
use ballista_spark_codec::SparkPhysicalCodec;
use datafusion::common::Result;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::{ExecutionPlan, displayable};
use datafusion::prelude::SessionConfig;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use datafusion_proto::protobuf::PhysicalPlanNode;

/// Where and how to reach the Ballista scheduler.
#[derive(Debug, Clone)]
pub struct BallistaClusterOptions {
    /// e.g. `http://ballista-scheduler:50050`
    pub scheduler_url: String,
    pub ballista_config: BallistaConfig,
}

impl BallistaClusterOptions {
    pub fn new(scheduler_url: impl Into<String>) -> Self {
        Self {
            scheduler_url: scheduler_url.into(),
            ballista_config: BallistaConfig::default(),
        }
    }
}

/// Submits plans of one Spark session as Ballista physical-plan jobs, and
/// keeps track of them so they can be cancelled when the session ends.
pub struct BallistaSubmitter {
    options: BallistaClusterOptions,
    session_id: String,
    codec: Arc<dyn PhysicalExtensionCodec>,
    /// Jobs submitted and not yet known to have finished.
    jobs: Mutex<Vec<JobCancelHandle>>,
}

impl std::fmt::Debug for BallistaSubmitter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BallistaSubmitter")
            .field("options", &self.options)
            .field("session_id", &self.session_id)
            .finish_non_exhaustive()
    }
}

impl BallistaSubmitter {
    pub fn new(options: BallistaClusterOptions, session_id: String) -> Self {
        Self {
            options,
            session_id,
            codec: SparkPhysicalCodec::shared(),
            jobs: Mutex::new(Vec::new()),
        }
    }

    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// Runs `plan` as one Ballista job and streams all final partitions.
    ///
    /// The planning session's DataFusion options (which already carry
    /// Ballista's restrictions) are sent as job settings; the scheduler ignores
    /// keys it does not know.
    pub async fn submit(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        session_config: SessionConfig,
    ) -> Result<SendableRecordBatchStream> {
        log::debug!(
            "submitting plan to Ballista (session {}):\n{}",
            self.session_id,
            displayable(plan.as_ref()).indent(true)
        );
        let (stream, job) = submit_physical_plan::<PhysicalPlanNode>(
            self.options.scheduler_url.clone(),
            &self.options.ballista_config,
            plan,
            self.codec.as_ref(),
            self.session_id.clone(),
            session_config,
        )
        .await?;
        let mut jobs = self.jobs.lock().unwrap_or_else(|e| e.into_inner());
        jobs.retain(|job| !job.is_finished());
        jobs.push(job);
        Ok(stream)
    }

    /// Cancels every job of this session that has not finished. A job whose
    /// result stream is dropped early is cancelled anyway; this also covers
    /// streams still held, e.g. by a client that went away mid-query.
    pub async fn cancel_all(&self) {
        let jobs: Vec<_> =
            std::mem::take(&mut *self.jobs.lock().unwrap_or_else(|e| e.into_inner()));
        for job in jobs {
            job.cancel().await;
        }
    }
}
