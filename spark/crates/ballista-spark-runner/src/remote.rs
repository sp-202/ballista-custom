//! An operator whose input runs as a separate Ballista job.

use std::sync::Arc;

use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Result, internal_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, displayable,
};
use datafusion::prelude::SessionConfig;
use futures::{StreamExt, TryStreamExt};

use crate::submit::BallistaSubmitter;

/// Runs `plan` on the Ballista cluster when executed, and returns its result
/// as a single partition.
///
/// Used in the Spark Connect server for plans that mix driver-only operators
/// (e.g. a table-format commit) with distributed work: the driver-only part
/// runs locally and reads the distributed part through this operator.
///
/// The remote plan is not exposed as a child, so local optimizers and
/// executors never touch it.
#[derive(Debug)]
pub struct BallistaSubplanExec {
    plan: Arc<dyn ExecutionPlan>,
    submitter: Arc<BallistaSubmitter>,
    session_config: SessionConfig,
    properties: Arc<PlanProperties>,
}

impl BallistaSubplanExec {
    pub fn new(
        plan: Arc<dyn ExecutionPlan>,
        submitter: Arc<BallistaSubmitter>,
        session_config: SessionConfig,
    ) -> Self {
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(plan.schema()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Self {
            plan,
            submitter,
            session_config,
            properties,
        }
    }

    pub fn remote_plan(&self) -> &Arc<dyn ExecutionPlan> {
        &self.plan
    }
}

impl DisplayAs for BallistaSubplanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                f,
                "BallistaSubplanExec: session={}, remote plan:\n{}",
                self.submitter.session_id(),
                displayable(self.plan.as_ref()).indent(true)
            ),
            DisplayFormatType::TreeRender => {
                writeln!(f, "session={}", self.submitter.session_id())
            }
        }
    }
}

impl ExecutionPlan for BallistaSubplanExec {
    fn name(&self) -> &'static str {
        "BallistaSubplanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return internal_err!("BallistaSubplanExec has no children");
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!("BallistaSubplanExec has one partition, got {partition}");
        }
        let submitter = self.submitter.clone();
        let plan = self.plan.clone();
        let session_config = self.session_config.clone();
        let stream =
            futures::stream::once(async move { submitter.submit(plan, session_config).await })
                .try_flatten()
                .boxed();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}
