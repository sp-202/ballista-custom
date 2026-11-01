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

//! Skew join splitting, after Spark's `OptimizeSkewedJoin`.
//!
//! A shuffled join runs one task per shuffle partition, so a partition much
//! larger than the rest makes one task run long after the others finish. When
//! both inputs of a shuffled join have been written, their per-partition sizes
//! are known. A partition of a side is *skewed* when it is larger than both
//! `factor` × the median partition size of that side and `threshold` bytes.
//! A skewed partition is read in several pieces, each a contiguous range of
//! the map tasks that wrote it, sized near the average non-skewed partition
//! (at least the advisory size). The other side's matching partition is read
//! whole once per piece. Every pair of pieces then joins exactly the rows the
//! original partition pair would have, so the answer is unchanged.
//!
//! As in Spark, only the side whose rows each appear in one output piece may
//! be split: the left side of inner, left outer, left semi and left anti
//! joins; the right side of inner, right outer, right semi and right anti
//! joins. A full outer join is never split.
//!
//! The join's output stops being hash partitioned on its keys, so, like
//! Spark's `ValidateRequirements` check, a join is split only when nothing
//! between it and the stage's shuffle writer requires a distribution, and the
//! writer repartitions its input.

use std::sync::Arc;

use ballista_core::config::BallistaConfig;
use ballista_core::execution_plans::ShuffleReaderExec;
use ballista_core::serde::scheduler::PartitionLocation;
use datafusion::common::JoinType;
use datafusion::error::Result;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode, SortMergeJoinExec};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{
    Distribution, ExecutionPlan, Partitioning, replace_children_if_necessary,
};
use log::info;

use crate::state::aqe::coalesce::split_size_list_by_target_size;

/// Spark's `ShufflePartitionsUtil.SMALL_PARTITION_FACTOR`.
const SMALL_PARTITION_FACTOR: f64 = 0.2;
/// Spark's `ShufflePartitionsUtil.MERGED_PARTITION_FACTOR`.
const MERGED_PARTITION_FACTOR: f64 = 1.2;

/// Splits skewed partitions of the shuffled joins in a stage body (the plan
/// below the stage's shuffle writer). `writer_repartitions` says whether the
/// writer hash- or round-robin-partitions its input; without that the stage's
/// output partitions must stay as the join produces them.
pub fn split_skewed_joins(
    stage_body: Arc<dyn ExecutionPlan>,
    writer_repartitions: bool,
    config: &BallistaConfig,
) -> Result<Arc<dyn ExecutionPlan>> {
    if !config.skew_join_enabled() || !writer_repartitions {
        return Ok(stage_body);
    }
    let settings = Settings {
        factor: config.skew_join_partition_factor(),
        threshold: config.skew_join_partition_threshold_bytes() as u64,
        advisory: config.coalesce_target_partition_bytes() as u64,
    };
    rewrite(stage_body, &settings)
}

struct Settings {
    factor: f64,
    threshold: u64,
    advisory: u64,
}

/// Walks down from the stage root through operators that place no
/// distribution requirement on their inputs; joins reached that way may change
/// their partitioning.
fn rewrite(plan: Arc<dyn ExecutionPlan>, settings: &Settings) -> Result<Arc<dyn ExecutionPlan>> {
    if let Some(split) = try_split_join(&plan, settings)? {
        return Ok(split);
    }
    let children = plan.children();
    if children.is_empty() {
        return Ok(plan);
    }
    let required = plan.input_distribution_requirements();
    let free = required
        .per_child_distributions()
        .all(|d| matches!(d, Distribution::UnspecifiedDistribution));
    // An operator that pairs its inputs' partitions (or needs them in one
    // place) depends on the join's partitioning; stop there.
    if !free || children.len() > 1 && !is_union(&plan) {
        return Ok(plan);
    }
    let new_children = children
        .into_iter()
        .map(|child| rewrite(Arc::clone(child), settings))
        .collect::<Result<Vec<_>>>()?;
    replace_children_if_necessary(plan, new_children)
}

fn is_union(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.name() == "UnionExec"
}

/// Which sides of a join may be split (left, right). Spark's
/// `canSplitLeftSide` / `canSplitRightSide`, plus DataFusion's right semi and
/// anti joins, the mirror images of left semi and anti.
fn splittable_sides(join_type: JoinType) -> (bool, bool) {
    use JoinType::*;
    let left = matches!(join_type, Inner | Left | LeftSemi | LeftAnti);
    let right = matches!(join_type, Inner | Right | RightSemi | RightAnti);
    (left, right)
}

/// A join input: a shuffle reader, possibly under a sort (sort-merge join).
struct Input<'a> {
    reader: &'a ShuffleReaderExec,
    sort: Option<&'a SortExec>,
}

impl<'a> Input<'a> {
    fn of(plan: &'a Arc<dyn ExecutionPlan>) -> Option<Self> {
        let (sort, inner) = match plan.downcast_ref::<SortExec>() {
            Some(sort) if sort.preserve_partitioning() => (Some(sort), sort.input()),
            Some(_) => return None,
            None => (None, plan),
        };
        let reader = inner.downcast_ref::<ShuffleReaderExec>()?;
        let plain = !reader.broadcast
            && reader.coalesce.is_none()
            && matches!(reader.properties().partitioning, Partitioning::Hash(_, _));
        plain.then_some(Input { reader, sort })
    }

    /// The input with its reader reading `partitions` instead.
    fn with_partitions(
        &self,
        partitions: Vec<Vec<PartitionLocation>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let count = partitions.len();
        let reader: Arc<dyn ExecutionPlan> = Arc::new(ShuffleReaderExec::try_new(
            self.reader.stage_id,
            partitions,
            self.reader.schema(),
            Partitioning::UnknownPartitioning(count),
        )?);
        Ok(match self.sort {
            Some(sort) => Arc::new(
                SortExec::new(sort.expr().clone(), reader)
                    .with_preserve_partitioning(true)
                    .with_fetch(sort.fetch()),
            ),
            None => reader,
        })
    }
}

fn try_split_join(
    plan: &Arc<dyn ExecutionPlan>,
    settings: &Settings,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    let (join_type, left, right) = if let Some(join) = plan.downcast_ref::<HashJoinExec>() {
        if join.partition_mode() != &PartitionMode::Partitioned || join.null_aware {
            return Ok(None);
        }
        (*join.join_type(), join.left(), join.right())
    } else if let Some(join) = plan.downcast_ref::<SortMergeJoinExec>() {
        (join.join_type(), join.left(), join.right())
    } else {
        return Ok(None);
    };
    let (can_left, can_right) = splittable_sides(join_type);
    if !can_left && !can_right {
        return Ok(None);
    }
    let (Some(left_input), Some(right_input)) = (Input::of(left), Input::of(right)) else {
        return Ok(None);
    };
    let left_parts = &left_input.reader.partition;
    let right_parts = &right_input.reader.partition;
    if left_parts.len() != right_parts.len() || left_parts.is_empty() {
        return Ok(None);
    }

    let left_sizes: Vec<u64> = left_parts.iter().map(|p| bytes(p)).collect();
    let right_sizes: Vec<u64> = right_parts.iter().map(|p| bytes(p)).collect();
    let left_skew = skew_threshold(&left_sizes, settings);
    let right_skew = skew_threshold(&right_sizes, settings);
    let left_target = target_size(&left_sizes, left_skew, settings.advisory);
    let right_target = target_size(&right_sizes, right_skew, settings.advisory);

    let mut new_left = Vec::new();
    let mut new_right = Vec::new();
    let (mut skewed_left, mut skewed_right) = (0, 0);
    for p in 0..left_parts.len() {
        let left_pieces = if can_left && left_sizes[p] > left_skew {
            split_by_map(&left_parts[p], left_target)
        } else {
            None
        };
        let right_pieces = if can_right && right_sizes[p] > right_skew {
            split_by_map(&right_parts[p], right_target)
        } else {
            None
        };
        skewed_left += usize::from(left_pieces.is_some());
        skewed_right += usize::from(right_pieces.is_some());
        let left_pieces = left_pieces.unwrap_or_else(|| vec![left_parts[p].clone()]);
        let right_pieces = right_pieces.unwrap_or_else(|| vec![right_parts[p].clone()]);
        for l in &left_pieces {
            for r in &right_pieces {
                new_left.push(l.clone());
                new_right.push(r.clone());
            }
        }
    }
    if skewed_left == 0 && skewed_right == 0 {
        return Ok(None);
    }
    info!(
        "skew join: {} split {} left and {} right partition(s); {} -> {} partitions \
         (left median {} B, max {} B; right median {} B, max {} B)",
        plan.name(),
        skewed_left,
        skewed_right,
        left_parts.len(),
        new_left.len(),
        median(&left_sizes),
        left_sizes.iter().max().copied().unwrap_or(0),
        median(&right_sizes),
        right_sizes.iter().max().copied().unwrap_or(0),
    );
    let children = vec![
        left_input.with_partitions(new_left)?,
        right_input.with_partitions(new_right)?,
    ];
    Ok(Some(replace_children_if_necessary(Arc::clone(plan), children)?))
}

fn bytes(locations: &[PartitionLocation]) -> u64 {
    locations
        .iter()
        .map(|l| l.partition_stats.num_bytes().unwrap_or(0))
        .sum()
}

/// Spark's `Utils.median(sizes, alreadySorted = false)`: the upper median.
fn median(sizes: &[u64]) -> u64 {
    let mut sorted = sizes.to_vec();
    sorted.sort_unstable();
    sorted.get(sorted.len() / 2).copied().unwrap_or(0)
}

/// Spark's `getSkewThreshold`.
fn skew_threshold(sizes: &[u64], settings: &Settings) -> u64 {
    settings
        .threshold
        .max((median(sizes) as f64 * settings.factor) as u64)
}

/// Spark's `targetSize`: the average non-skewed partition size, at least the
/// advisory size.
fn target_size(sizes: &[u64], skew_threshold: u64, advisory: u64) -> u64 {
    let normal: Vec<u64> = sizes.iter().copied().filter(|&s| s <= skew_threshold).collect();
    if normal.is_empty() {
        advisory
    } else {
        advisory.max(normal.iter().sum::<u64>() / normal.len() as u64)
    }
}

/// Spark's `createSkewPartitionSpecs`: splits one reduce partition into
/// contiguous ranges of the map tasks that wrote it. `None` when it would be a
/// single piece.
fn split_by_map(
    locations: &[PartitionLocation],
    target: u64,
) -> Option<Vec<Vec<PartitionLocation>>> {
    let mut by_map: Vec<Vec<PartitionLocation>> = Vec::new();
    let mut sorted = locations.to_vec();
    sorted.sort_by_key(|l| l.map_partition_id);
    for location in sorted {
        match by_map.last_mut() {
            Some(group) if group[0].map_partition_id == location.map_partition_id => {
                group.push(location)
            }
            _ => by_map.push(vec![location]),
        }
    }
    let sizes: Vec<u64> = by_map.iter().map(|g| bytes(g)).collect();
    let starts = split_size_list_by_target_size(
        &sizes,
        target,
        SMALL_PARTITION_FACTOR,
        MERGED_PARTITION_FACTOR,
    );
    if starts.len() <= 1 {
        return None;
    }
    let mut pieces = Vec::with_capacity(starts.len());
    for (i, &start) in starts.iter().enumerate() {
        let end = starts.get(i + 1).copied().unwrap_or(by_map.len());
        pieces.push(by_map[start..end].concat());
    }
    Some(pieces)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ballista_core::JobId;
    use ballista_core::serde::scheduler::{ExecutorMetadata, PartitionId, PartitionStats};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::NullEquality;
    use datafusion::physical_expr::expressions::Column;

    fn location(map: usize, reducer: usize, bytes: u64) -> PartitionLocation {
        PartitionLocation {
            map_partition_id: map,
            partition_id: PartitionId {
                job_id: JobId::new("skew".to_string()),
                stage_id: 1,
                partition_id: reducer,
            },
            executor_meta: ExecutorMetadata {
                id: "1".to_string(),
                host: "localhost".to_string(),
                port: 0,
                grpc_port: 0,
                specification: Default::default(),
                os_info: Default::default(),
            },
            partition_stats: PartitionStats::new(Some(1), Some(1), Some(bytes)),
            file_id: None,
            is_sort_shuffle: false,
        }
    }

    /// 4 reduce partitions, each written by 8 map tasks; `big` gives the
    /// per-map size of partition 3, the rest write 10 bytes per map.
    fn reader(big: u64) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let partitions = (0..4)
            .map(|r| {
                (0..8)
                    .map(|m| location(m, r, if r == 3 { big } else { 10 }))
                    .collect()
            })
            .collect();
        let keys = vec![Arc::new(Column::new("k", 0)) as _];
        Arc::new(
            ShuffleReaderExec::try_new(1, partitions, schema, Partitioning::Hash(keys, 4))
                .unwrap(),
        )
    }

    fn join(join_type: JoinType, left: u64, right: u64) -> Arc<dyn ExecutionPlan> {
        let on = vec![(
            Arc::new(Column::new("k", 0)) as _,
            Arc::new(Column::new("k", 0)) as _,
        )];
        Arc::new(
            HashJoinExec::try_new(
                reader(left),
                reader(right),
                on,
                None,
                &join_type,
                None,
                PartitionMode::Partitioned,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap(),
        )
    }

    fn settings() -> Settings {
        Settings {
            factor: 5.0,
            threshold: 0,
            advisory: 80,
        }
    }

    fn reader_partitions(plan: &Arc<dyn ExecutionPlan>, side: usize) -> Vec<Vec<usize>> {
        let child = &plan.children()[side];
        let reader = child.downcast_ref::<ShuffleReaderExec>().unwrap();
        reader
            .partition
            .iter()
            .map(|p| p.iter().map(|l| l.partition_id.partition_id * 100 + l.map_partition_id).collect())
            .collect()
    }

    #[test]
    fn skewed_left_partition_is_split_by_map_ranges() {
        // Partition 3 is 8 x 100 = 800 bytes, the others 80; target is 80, so
        // partition 3 splits into map ranges of about 80 bytes each.
        let plan = rewrite(join(JoinType::Inner, 100, 10), &settings()).unwrap();
        let left = reader_partitions(&plan, 0);
        let right = reader_partitions(&plan, 1);
        assert_eq!(left.len(), right.len());
        assert!(left.len() > 4, "{left:?}");
        // The first three partitions are untouched.
        assert_eq!(&left[..3], &reader_partitions(&join(JoinType::Inner, 10, 10), 0)[..3]);
        // Every map output of partition 3 is read exactly once on the left...
        let mut maps: Vec<usize> = left[3..].concat();
        maps.sort();
        assert_eq!(maps, (300..308).collect::<Vec<_>>());
        // ...and each piece meets the whole right partition 3.
        for r in &right[3..] {
            assert_eq!(r, &(300..308).collect::<Vec<_>>());
        }
        assert_eq!(plan.properties().partitioning.partition_count(), left.len());
    }

    #[test]
    fn side_that_cannot_be_split_is_left_alone() {
        // A left join keeps every left row once; its right side may not split.
        let plan = rewrite(join(JoinType::Left, 10, 100), &settings()).unwrap();
        assert_eq!(reader_partitions(&plan, 0).len(), 4);
        let plan = rewrite(join(JoinType::Full, 100, 100), &settings()).unwrap();
        assert_eq!(reader_partitions(&plan, 0).len(), 4);
    }

    #[test]
    fn join_under_an_operator_needing_its_partitioning_is_left_alone() {
        use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
        let joined = join(JoinType::Inner, 100, 10);
        let schema = joined.schema();
        let group = PhysicalGroupBy::new_single(vec![(
            Arc::new(Column::new("k", 0)) as _,
            "k".to_string(),
        )]);
        let agg: Arc<dyn ExecutionPlan> = Arc::new(
            AggregateExec::try_new(
                AggregateMode::FinalPartitioned,
                group,
                vec![],
                vec![],
                joined,
                schema,
            )
            .unwrap(),
        );
        let plan = rewrite(agg, &settings()).unwrap();
        assert_eq!(plan.children()[0].properties().partitioning.partition_count(), 4);
    }

    #[test]
    fn spark_thresholds() {
        let settings = Settings {
            factor: 5.0,
            threshold: 256,
            advisory: 64,
        };
        // median 100 -> 5 x 100 = 500 beats the 256 threshold
        let sizes = [100, 100, 100, 2000];
        assert_eq!(median(&sizes), 100);
        assert_eq!(skew_threshold(&sizes, &settings), 500);
        // non-skewed average 100 beats the advisory 64
        assert_eq!(target_size(&sizes, 500, 64), 100);
        // only skewed partitions: the advisory size
        assert_eq!(target_size(&[1000], 500, 64), 64);
    }

    #[test]
    fn full_outer_join_is_never_split() {
        assert_eq!(splittable_sides(JoinType::Full), (false, false));
        assert_eq!(splittable_sides(JoinType::Left), (true, false));
        assert_eq!(splittable_sides(JoinType::RightAnti), (false, true));
        assert_eq!(splittable_sides(JoinType::Inner), (true, true));
    }
}
