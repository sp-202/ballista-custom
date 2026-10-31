//! Submitting physical plans to a Ballista scheduler.

use std::sync::Arc;

use ballista_core::config::BallistaConfig;
use ballista_core::execution_plans::execute_physical_plan;
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

/// Submits plans of one Spark session as Ballista physical-plan jobs.
#[derive(Debug)]
pub struct BallistaSubmitter {
    options: BallistaClusterOptions,
    session_id: String,
    codec: Arc<dyn PhysicalExtensionCodec>,
}

impl BallistaSubmitter {
    pub fn new(options: BallistaClusterOptions, session_id: String) -> Self {
        Self {
            options,
            session_id,
            codec: SparkPhysicalCodec::shared(),
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
        execute_physical_plan::<PhysicalPlanNode>(
            self.options.scheduler_url.clone(),
            &self.options.ballista_config,
            plan,
            self.codec.as_ref(),
            self.session_id.clone(),
            session_config,
        )
        .await
    }
}
