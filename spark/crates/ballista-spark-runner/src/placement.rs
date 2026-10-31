//! Deciding where each part of a Spark plan runs.
//!
//! Some operators must run where the Spark session lives — in Ballista's case
//! the Spark Connect server — because they use session state that executors
//! do not have: catalog commands, system tables, table-format commits and
//! similar (Sail's list: `sail_execution::is_driver_stage_plan`). Spark has
//! the same split between driver and executors.
//!
//! [`place`] keeps the operators on the path from the root to every such
//! operator in the server, and turns each maximal subtree without one into a
//! [`BallistaSubplanExec`], i.e. a separate Ballista job.

use std::sync::Arc;

use datafusion::common::Result;
use datafusion::physical_plan::{ExecutionPlan, replace_children_if_necessary};
use datafusion::prelude::SessionConfig;
use sail_execution::is_driver_stage_plan;

use crate::remote::BallistaSubplanExec;
use crate::submit::BallistaSubmitter;

/// Whether any operator in `plan` must run in the Spark Connect server.
pub fn has_driver_stage(plan: &Arc<dyn ExecutionPlan>) -> bool {
    is_driver_stage_plan(plan) || plan.children().into_iter().any(has_driver_stage)
}

/// Rewrites `plan` so that it can be executed in the Spark Connect server:
/// driver-only operators (and their ancestors) stay local, everything else
/// runs on Ballista through [`BallistaSubplanExec`].
///
/// Call only when [`has_driver_stage`] is true; otherwise submit the whole
/// plan as one job.
pub fn place(
    plan: Arc<dyn ExecutionPlan>,
    submitter: &Arc<BallistaSubmitter>,
    session_config: &SessionConfig,
) -> Result<Arc<dyn ExecutionPlan>> {
    if !has_driver_stage(&plan) {
        return Ok(Arc::new(BallistaSubplanExec::new(
            plan,
            submitter.clone(),
            session_config.clone(),
        )));
    }
    let children = plan
        .children()
        .into_iter()
        .map(|child| place(child.clone(), submitter, session_config))
        .collect::<Result<Vec<_>>>()?;
    if children.is_empty() {
        return Ok(plan);
    }
    replace_children_if_necessary(plan, children)
}
