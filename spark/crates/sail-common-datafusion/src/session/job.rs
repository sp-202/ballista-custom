use std::future::Future;
use std::sync::Arc;

use datafusion::common::Result;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;

use crate::extension::SessionExtension;

#[tonic::async_trait]
pub trait JobRunner: Send + Sync + 'static {
    /// Executes a plan.
    async fn execute(
        &self,
        ctx: &SessionContext,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<SendableRecordBatchStream>;

    /// Returns the distributed execution plan for EXPLAIN output.
    fn explain(&self, plan: Arc<dyn ExecutionPlan>) -> Result<String>;

    async fn stop(&self);
}

pub struct JobService {
    runner: Box<dyn JobRunner>,
}

impl JobService {
    pub fn new(runner: Box<dyn JobRunner>) -> Self {
        Self { runner }
    }

    pub fn runner(&self) -> &dyn JobRunner {
        self.runner.as_ref()
    }
}

impl SessionExtension for JobService {
    fn name() -> &'static str {
        "JobService"
    }
}

/// What a [`JobRunner`] should record about the query it is about to submit.
#[derive(Debug, Clone, Default)]
pub struct JobMeta {
    /// The query: SQL text, or the kind of DataFrame operation.
    pub description: String,
    /// The client user (for Spark Connect, the session's user id).
    pub user: String,
    /// The client's name for its application (for Spark Connect, `spark.app.name`).
    pub app_name: String,
}

tokio::task_local! {
    static JOB_META: JobMeta;
}

/// Runs `future` with a description of the query being executed and who submitted it. A
/// [`JobRunner`] reads it with [`current_job_meta`] to name the job it submits, so a cluster's
/// UI can show what a job is. The metadata reaches only the code running inside `future`.
pub async fn with_job_meta<F: Future>(meta: JobMeta, future: F) -> F::Output {
    JOB_META.scope(meta, future).await
}

/// The metadata of the query this code is executing, if set with [`with_job_meta`].
pub fn current_job_meta() -> Option<JobMeta> {
    JOB_META.try_with(|m| m.clone()).ok()
}
