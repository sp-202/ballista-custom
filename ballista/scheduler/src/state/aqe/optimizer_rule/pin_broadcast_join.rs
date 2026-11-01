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

//! Keeps DataFusion's `join_selection` from swapping broadcast joins AQE chose.
//!
//! Every `CollectLeft` hash join in an adaptive plan was chosen by
//! `SelectJoinRule` from runtime sizes: its build side is a broadcast exchange
//! or a single-partition scan. `join_selection` re-runs on every replan and, for
//! a `CollectLeft` join, swaps the inputs whenever its estimates say the probe
//! side is smaller. When the probe side is still an estimate through joins that
//! have not run (a broadcast decided before the probe side ran, BS-025) that is
//! wrong: it would collect the large side. It also fails outright once
//! `FilterPushdown` has attached a dynamic filter to the join.
//!
//! [`PinBroadcastJoins`] wraps `join_selection`: it hides each `CollectLeft`
//! hash join behind a [`PinnedJoinExec`] the rule does not recognise, runs the
//! rule (which still visits the joins' inputs), and unwraps them again.

use std::sync::Arc;

use datafusion::{
    arrow::datatypes::SchemaRef,
    common::{
        Result, Statistics, internal_err,
        tree_node::{Transformed, TreeNode, TreeNodeRecursion},
    },
    config::ConfigOptions,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::PhysicalExpr,
    physical_optimizer::PhysicalOptimizerRule,
    physical_plan::{
        ChildStats, ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan,
        PlanProperties, ReplaceChildrenOptions, StatisticsArgs,
        joins::{HashJoinExec, PartitionMode},
    },
};

/// Runs `inner` (DataFusion's `join_selection`) with every `CollectLeft` hash
/// join pinned. See the module documentation.
#[derive(Debug)]
pub struct PinBroadcastJoins {
    inner: Arc<dyn PhysicalOptimizerRule + Send + Sync>,
}

impl PinBroadcastJoins {
    pub fn new(inner: Arc<dyn PhysicalOptimizerRule + Send + Sync>) -> Self {
        Self { inner }
    }
}

impl PhysicalOptimizerRule for PinBroadcastJoins {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let pinned = plan
            .transform_up(|node| {
                let is_collect_left =
                    node.downcast_ref::<HashJoinExec>().is_some_and(|join| {
                        join.partition_mode() == &PartitionMode::CollectLeft
                    });
                if is_collect_left {
                    Ok(Transformed::yes(
                        Arc::new(PinnedJoinExec { join: node }) as Arc<dyn ExecutionPlan>
                    ))
                } else {
                    Ok(Transformed::no(node))
                }
            })?
            .data;
        let optimized = self.inner.optimize(pinned, config)?;
        Ok(optimized
            .transform_up(|node| match node.downcast_ref::<PinnedJoinExec>() {
                Some(pinned) => Ok(Transformed::yes(Arc::clone(&pinned.join))),
                None => Ok(Transformed::no(node)),
            })?
            .data)
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn schema_check(&self) -> bool {
        self.inner.schema_check()
    }
}

/// A join hidden from `join_selection` for the duration of that rule. It has
/// the join's inputs, properties and statistics, and is never executed.
#[derive(Debug)]
struct PinnedJoinExec {
    join: Arc<dyn ExecutionPlan>,
}

impl DisplayAs for PinnedJoinExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "PinnedJoinExec: ")?;
        self.join.fmt_as(t, f)
    }
}

impl ExecutionPlan for PinnedJoinExec {
    fn name(&self) -> &str {
        "PinnedJoinExec"
    }

    fn schema(&self) -> SchemaRef {
        self.join.schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.join.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.join.children()
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        self.join.apply_expressions(f)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(PinnedJoinExec {
            join: Arc::clone(&self.join).replace_children(
                children,
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )?,
        }))
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        self.join.child_stats_requests(partition)
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        self.join.statistics_from_inputs(input_stats, args)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        internal_err!("PinnedJoinExec exists only while join_selection runs")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::JoinType;
    use datafusion::common::{ColumnStatistics, stats::Precision};
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_optimizer::join_selection::JoinSelection;
    use datafusion::physical_plan::test::exec::StatisticsExec;

    fn sized(rows: usize) -> Arc<dyn ExecutionPlan> {
        let schema = Schema::new(vec![Field::new("k", DataType::Int32, false)]);
        Arc::new(StatisticsExec::new(
            Statistics {
                num_rows: Precision::Exact(rows),
                total_byte_size: Precision::Exact(rows * 4),
                column_statistics: vec![ColumnStatistics::new_unknown()],
            },
            schema,
        ))
    }

    /// A `CollectLeft` join whose build side DataFusion believes is the larger
    /// one: plain `join_selection` swaps it, the pinned rule keeps it.
    #[test]
    fn collect_left_join_is_not_swapped() -> Result<()> {
        let build = sized(1_000);
        let probe = sized(10);
        let on = vec![(
            Arc::new(Column::new("k", 0)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("k", 0)) as Arc<dyn PhysicalExpr>,
        )];
        let join: Arc<dyn ExecutionPlan> = Arc::new(
            datafusion::physical_plan::joins::HashJoinExecBuilder::new(
                build,
                probe,
                on,
                JoinType::Inner,
            )
            .with_partition_mode(PartitionMode::CollectLeft)
            .build()?,
        );
        let config = ConfigOptions::new();

        let left_rows = |plan: &Arc<dyn ExecutionPlan>| {
            datafusion::physical_plan::StatisticsContext::new()
                .compute(find_join(plan).left().as_ref(), &StatisticsArgs::new())
                .unwrap()
                .num_rows
        };

        let swapped = JoinSelection::new().optimize(Arc::clone(&join), &config)?;
        assert_eq!(left_rows(&swapped), Precision::Exact(10));

        let pinned = PinBroadcastJoins::new(Arc::new(JoinSelection::new()))
            .optimize(join, &config)?;
        assert_eq!(left_rows(&pinned), Precision::Exact(1_000));
        assert!(pinned.is::<HashJoinExec>());
        Ok(())
    }

    fn find_join(plan: &Arc<dyn ExecutionPlan>) -> &HashJoinExec {
        if let Some(join) = plan.downcast_ref::<HashJoinExec>() {
            return join;
        }
        find_join(plan.children()[0])
    }
}
