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

//! Dynamic partition pruning for broadcast joins, inside one task.
//!
//! Spark prunes the fact-side scan of a broadcast join with the build side's
//! keys (`PartitionPruning` + `PlanDynamicPruningFilters`, broadcast reuse only).
//! DataFusion does the same with a hash join's dynamic filter: once the build
//! side is collected, its key bounds and key set become a predicate on the
//! probe-side scan, which skips files by partition value and statistics, row
//! groups, pages and rows.
//!
//! The scheduler plans with dynamic filters off, because a filter cannot cross
//! a stage boundary and DataFusion's protobuf does not carry the link between a
//! join and its filter. Here, after the task plan is decoded, the filters of
//! `CollectLeft` joins are pushed down again inside the task.
//!
//! A `CollectLeft` join publishes its filter only after every probe partition
//! has reported (a barrier), and a partition dropped unpolled never reports.
//! So a join is rewritten only where every one of its partitions is polled:
//! reached from the writer (which drives all its partitions) through operators
//! that drive all their inputs, entering joins only through the build side, and
//! with no partitioned or sort-merge join below it. `Partitioned` joins are
//! never given filters: theirs route rows by local partition index, which a
//! task's restricted plan does not keep.
//!
//! Spark's pruning skips whole partitions at file listing. DataFusion's
//! equivalent, the per-file pruner, folds partition values into the filter and
//! drops a file before opening it, but only for files that carry a statistics
//! struct; files listed without statistics get an all-unknown one here.

use std::sync::Arc;

use datafusion::common::Statistics;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfig};
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::Result;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::filter_pushdown::FilterPushdown;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::{ExecutionPlan, replace_children_if_necessary};
use datafusion::prelude::SessionConfig;

/// Operators that, when all their output partitions run to completion, poll
/// every input partition (consistently across partitions for joins, whose
/// build side is collected once).
const DRIVES_ALL_INPUTS: &[&str] = &[
    "ShuffleWriterExec",
    "SortShuffleWriterExec",
    "ProjectionExec",
    "FilterExec",
    "CoalesceBatchesExec",
    "CoalescePartitionsExec",
    "RepartitionExec",
    "SortExec",
    "SortPreservingMergeExec",
    "AggregateExec",
    "UnionExec",
    "BoundedWindowAggExec",
    "WindowAggExec",
    "CrossJoinExec",
    "NestedLoopJoinExec",
    "HashJoinExec",
];

/// Lets executed joins publish dynamic filters (the scheduler's session turns
/// them off; see `ballista_core::extension`).
pub fn enable_join_filters(config: &mut SessionConfig) {
    let optimizer = &mut config.options_mut().optimizer;
    optimizer.enable_join_dynamic_filter_pushdown = true;
}

/// Pushes the dynamic filters of every safely rewritable `CollectLeft` hash
/// join in a decoded task plan down to its probe-side scans.
pub fn push_join_filters(
    plan: Arc<dyn ExecutionPlan>,
    config: &SessionConfig,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut options: ConfigOptions = config.options().as_ref().clone();
    options.optimizer.enable_join_dynamic_filter_pushdown = true;
    options.optimizer.enable_topk_dynamic_filter_pushdown = false;
    options.optimizer.enable_aggregate_dynamic_filter_pushdown = false;
    rewrite(plan, &options)
}

fn rewrite(
    plan: Arc<dyn ExecutionPlan>,
    options: &ConfigOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    if is_collect_left(&plan) && every_partition_polled(&plan) {
        let plan = FilterPushdown::new_post_optimization().optimize(plan, options)?;
        return with_file_statistics(plan);
    }
    if !DRIVES_ALL_INPUTS.contains(&plan.name()) {
        return Ok(plan);
    }
    // A hash join may skip polling its probe side (an empty build side), so
    // only its build side is entered.
    let is_join = plan.downcast_ref::<HashJoinExec>().is_some();
    let children = plan
        .children()
        .into_iter()
        .enumerate()
        .map(|(i, child)| {
            if is_join && i > 0 {
                Ok(Arc::clone(child))
            } else {
                rewrite(Arc::clone(child), options)
            }
        })
        .collect::<Result<Vec<_>>>()?;
    replace_children_if_necessary(plan, children)
}

/// Gives every listed file without statistics an all-unknown statistics
/// struct, so the parquet opener builds its per-file pruner.
fn with_file_statistics(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    plan.transform_up(|node| {
        let Some(exec) = node.downcast_ref::<DataSourceExec>() else {
            return Ok(Transformed::no(node));
        };
        let Some(config) = exec.data_source().downcast_ref::<FileScanConfig>() else {
            return Ok(Transformed::no(node));
        };
        if config
            .file_groups
            .iter()
            .all(|group| group.iter().all(|file| file.statistics.is_some()))
        {
            return Ok(Transformed::no(node));
        }
        let unknown = Arc::new(Statistics::new_unknown(config.file_schema()));
        let mut config = config.clone();
        config.file_groups = std::mem::take(&mut config.file_groups)
            .into_iter()
            .map(|group| {
                let group_statistics = group.file_statistics(None).cloned();
                let files = group
                    .into_inner()
                    .into_iter()
                    .map(|mut file| {
                        file.statistics.get_or_insert_with(|| Arc::clone(&unknown));
                        file
                    })
                    .collect();
                let group = FileGroup::new(files);
                match group_statistics {
                    Some(statistics) => group.with_statistics(Arc::new(statistics)),
                    None => group,
                }
            })
            .collect();
        let exec = exec.clone().with_data_source(Arc::new(config));
        Ok(Transformed::yes(Arc::new(exec) as Arc<dyn ExecutionPlan>))
    })
    .map(|t| t.data)
}

fn is_collect_left(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.downcast_ref::<HashJoinExec>()
        .is_some_and(|join| *join.partition_mode() == PartitionMode::CollectLeft)
}

/// True when, once the root's partitions all run, every partition of every
/// node below is polled: only operators that drive all their inputs, and no
/// joins other than `CollectLeft` hash joins (whose build side is shared, so
/// all their partitions skip or poll the probe side alike).
fn every_partition_polled(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let children = plan.children();
    if children.is_empty() {
        return true;
    }
    let joins_ok = plan
        .downcast_ref::<HashJoinExec>()
        .is_none_or(|join| *join.partition_mode() == PartitionMode::CollectLeft);
    joins_ok
        && DRIVES_ALL_INPUTS.contains(&plan.name())
        && children.into_iter().all(every_partition_polled)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{JoinType, NullEquality};
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::physical_plan::{
        FileGroup, FileScanConfigBuilder, ParquetSource,
    };
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::physical_expr::expressions::col;
    use datafusion::physical_plan::displayable;

    fn scan(name: &str) -> Arc<dyn ExecutionPlan> {
        let schema =
            Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, false)]));
        let file =
            |i| FileGroup::new(vec![PartitionedFile::new(format!("{name}{i}"), 1)]);
        let config = FileScanConfigBuilder::new(
            ObjectStoreUrl::local_filesystem(),
            Arc::new(ParquetSource::new(schema)),
        )
        .with_file_groups(vec![file(0), file(1)])
        .build();
        DataSourceExec::from_data_source(config)
    }

    fn join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        mode: PartitionMode,
    ) -> Arc<dyn ExecutionPlan> {
        let on = vec![(
            col(&left.schema().field(0).name().clone(), &left.schema()).unwrap(),
            col(&right.schema().field(0).name().clone(), &right.schema()).unwrap(),
        )];
        Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                on,
                None,
                &JoinType::Inner,
                None,
                mode,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap(),
        )
    }

    fn pushed(plan: Arc<dyn ExecutionPlan>) -> String {
        let plan = push_join_filters(plan, &SessionConfig::new()).unwrap();
        displayable(plan.as_ref()).indent(true).to_string()
    }

    #[test]
    fn broadcast_join_filters_its_probe_scan() {
        let plan = join(scan("a"), scan("b"), PartitionMode::CollectLeft);
        assert!(pushed(plan).contains("DynamicFilter"));
    }

    #[test]
    fn probe_files_get_statistics() {
        let plan = join(scan("a"), scan("b"), PartitionMode::CollectLeft);
        let plan = push_join_filters(plan, &SessionConfig::new()).unwrap();
        let probe = plan.children()[1];
        let exec = probe.downcast_ref::<DataSourceExec>().unwrap();
        let config = exec.data_source().downcast_ref::<FileScanConfig>().unwrap();
        assert!(
            config
                .file_groups
                .iter()
                .all(|g| g.iter().all(|f| f.statistics.is_some()))
        );
    }

    #[test]
    fn partitioned_join_gets_no_filter() {
        let plan = join(scan("a"), scan("b"), PartitionMode::Partitioned);
        assert!(!pushed(plan).contains("DynamicFilter"));
    }

    #[test]
    fn broadcast_join_on_a_probe_side_is_left_alone() {
        // The outer partitioned join may skip probing a partition whose build
        // side is empty; the inner join's barrier would then never complete.
        let inner = join(scan("b"), scan("c"), PartitionMode::CollectLeft);
        let plan = join(scan("a"), inner, PartitionMode::Partitioned);
        assert!(!pushed(plan).contains("DynamicFilter"));
    }

    #[test]
    fn broadcast_join_on_a_build_side_is_rewritten() {
        let inner = join(scan("b"), scan("c"), PartitionMode::CollectLeft);
        let plan = join(inner, scan("a"), PartitionMode::Partitioned);
        assert!(pushed(plan).contains("DynamicFilter"));
    }
}
