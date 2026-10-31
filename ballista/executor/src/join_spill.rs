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

//! Disk-backed spilling for partitioned hash joins (grace hash join).
//!
//! Under memory pressure, a partitioned `HashJoinExec` moves whole hash buckets of
//! its build side, together with the probe rows for those buckets, to a
//! [`JoinSpillHook`], and later joins each spilled bucket in a pass of its own.
//! [`BallistaJoinSpillHook`] stores those buckets as Arrow IPC files on the
//! executor's disk, built for build sides far larger than memory:
//!
//! - **Disk, through the task's runtime.** Files are created by the task's
//!   `DiskManager` (the executor's spill directory under `--work-dir`) and every
//!   write counts against `datafusion.runtime.max_temp_directory_size`. Running out
//!   of that budget fails the task with DataFusion's "exceeded the allowable limit"
//!   error instead of filling the disk.
//! - **DataFusion's spill writer.** Batches go through `SpillManager`: the session's
//!   spill compression, compaction of sliced view arrays (which would otherwise drag
//!   whole buffers into the file) and spill metrics.
//! - **Streamed back, read once, then deleted.** Each bucket is read exactly once,
//!   as a stream of batches, never loaded whole. Reading removes the bucket from the
//!   hook, and its files are deleted (and their disk usage released) as soon as the
//!   join drops the stream.
//! - **No contention between buckets.** Each bucket has its own lock; the map lock
//!   is held only to find or remove a bucket.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{JoinSide, Result, internal_err};
use datafusion::execution::{SendableRecordBatchStream, SpillFile, TaskContext};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::physical_plan::joins::{HashJoinExec, JoinSpillHook, PartitionMode};
use datafusion::physical_plan::{ExecutionPlan, SpillManager};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, SpillMetrics};
use datafusion::physical_plan::spill::InProgressSpillFile;
use log::{debug, info};
use parking_lot::Mutex;

/// Gives every partitioned [`HashJoinExec`] in `plan` that has no spill hook its own
/// [`BallistaJoinSpillHook`]. Call once per task plan, before execution: the hook binds
/// the task's runtime when the join starts executing.
pub fn with_join_spill_hooks(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    with_join_spill_hooks_from(plan, || Arc::new(BallistaJoinSpillHook::default()))
}

fn with_join_spill_hooks_from(
    plan: Arc<dyn ExecutionPlan>,
    new_hook: impl Fn() -> Arc<dyn JoinSpillHook>,
) -> Result<Arc<dyn ExecutionPlan>> {
    Ok(plan
        .transform_up(|node| {
            let Some(join) = node.downcast_ref::<HashJoinExec>() else {
                return Ok(Transformed::no(node));
            };
            if *join.partition_mode() != PartitionMode::Partitioned
                || join.spill_hook.is_some()
            {
                return Ok(Transformed::no(node));
            }
            let join = join.builder().with_spill_hooks(Some(new_hook())).build()?;
            Ok(Transformed::yes(Arc::new(join) as Arc<dyn ExecutionPlan>))
        })?
        .data)
}

/// Spill files of one `(bucket id, side)`: the file being written, plus finished ones.
#[derive(Default)]
struct BucketFiles {
    open: Option<InProgressSpillFile>,
    finished: Vec<Arc<dyn SpillFile>>,
}

/// See the [module documentation](self).
pub struct BallistaJoinSpillHook {
    /// The task this hook spills for, bound when the join starts executing.
    context: OnceLock<Arc<TaskContext>>,
    metrics: ExecutionPlanMetricsSet,
    spill_metrics: SpillMetrics,
    /// One spill writer per join side (each side has a single schema).
    managers: Mutex<HashMap<bool, Arc<SpillManager>>>,
    /// Keyed by `(bucket id, is build side)`; ids are unique per partition, pass and
    /// bucket, so every key is written by one pass and read exactly once.
    buckets: Mutex<HashMap<(usize, bool), Arc<Mutex<BucketFiles>>>>,
}

impl Default for BallistaJoinSpillHook {
    fn default() -> Self {
        let metrics = ExecutionPlanMetricsSet::new();
        let spill_metrics = SpillMetrics::new(&metrics, 0);
        Self {
            context: OnceLock::new(),
            metrics,
            spill_metrics,
            managers: Mutex::new(HashMap::new()),
            buckets: Mutex::new(HashMap::new()),
        }
    }
}

impl std::fmt::Debug for BallistaJoinSpillHook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BallistaJoinSpillHook")
            .field("spilled_rows", &self.spill_metrics.spilled_rows.value())
            .field("spilled_bytes", &self.spill_metrics.spilled_bytes.value())
            .finish_non_exhaustive()
    }
}

impl BallistaJoinSpillHook {
    /// Rows, bytes and files spilled through this hook so far.
    pub fn spilled(&self) -> (usize, usize, usize) {
        (
            self.spill_metrics.spilled_rows.value(),
            self.spill_metrics.spilled_bytes.value(),
            self.spill_metrics.spill_file_count.value(),
        )
    }

    /// The metrics set holding this hook's spill metrics.
    pub fn metrics(&self) -> &ExecutionPlanMetricsSet {
        &self.metrics
    }

    fn manager(&self, build_side: bool, schema: &SchemaRef) -> Result<Arc<SpillManager>> {
        let mut managers = self.managers.lock();
        if let Some(manager) = managers.get(&build_side) {
            return Ok(Arc::clone(manager));
        }
        let Some(context) = self.context.get() else {
            return internal_err!(
                "hash join spill hook used before the task context was bound"
            );
        };
        let compression = context.session_config().options().execution.spill_compression;
        let manager = Arc::new(
            SpillManager::new(
                context.runtime_env(),
                self.spill_metrics.clone(),
                Arc::clone(schema),
            )
            .with_compression_type(compression),
        );
        managers.insert(build_side, Arc::clone(&manager));
        Ok(manager)
    }
}

impl JoinSpillHook for BallistaJoinSpillHook {
    fn bind_task_context(&self, context: &Arc<TaskContext>) {
        // One hook serves one task; every partition it executes has the same context.
        let _ = self.context.set(Arc::clone(context));
    }

    fn spill_partition_batches(
        &self,
        bucket: usize,
        side: JoinSide,
        batches: &[RecordBatch],
    ) -> Result<()> {
        let Some(first) = batches.first() else {
            return Ok(());
        };
        let build_side = matches!(side, JoinSide::Left);
        let manager = self.manager(build_side, &first.schema())?;
        let slot = Arc::clone(
            self.buckets
                .lock()
                .entry((bucket, build_side))
                .or_default(),
        );
        let mut files = slot.lock();
        if files.open.is_none() {
            files.open = Some(manager.create_in_progress_file("Ballista hash join spill")?);
        }
        if let Some(open) = files.open.as_mut() {
            for batch in batches {
                open.append_batch(batch)?;
            }
        }
        debug!(
            "hash join spilled {} batches to bucket {bucket:#x} ({})",
            batches.len(),
            if build_side { "build" } else { "probe" }
        );
        Ok(())
    }

    fn read_partition_spills(
        &self,
        bucket: usize,
        side: JoinSide,
    ) -> Result<Vec<SendableRecordBatchStream>> {
        let build_side = matches!(side, JoinSide::Left);
        let Some(slot) = self.buckets.lock().remove(&(bucket, build_side)) else {
            return Ok(vec![]);
        };
        let files = {
            let mut files = slot.lock();
            if let Some(mut open) = files.open.take()
                && let Some(file) = open.finish()?
            {
                files.finished.push(file);
            }
            std::mem::take(&mut files.finished)
        };
        if files.is_empty() {
            return Ok(vec![]);
        }
        let Some(manager) = self.managers.lock().get(&build_side).cloned() else {
            return internal_err!("hash join spill files without a spill writer");
        };
        // The reader stream owns its file: dropping the stream deletes the file.
        files
            .into_iter()
            .map(|file| manager.read_spill_as_stream(file, None))
            .collect()
    }

    // The hash join uses only the bucket methods above. Returning `None` here makes
    // any other operator fall back to DataFusion's own spill files.
    fn spill_batches(&self, _batches: &[RecordBatch]) -> Result<Option<Arc<dyn SpillFile>>> {
        Ok(None)
    }

    fn read_spills(&self) -> Result<Vec<SendableRecordBatchStream>> {
        Ok(vec![])
    }

    fn read_spilled_batch(&self, _file: &std::path::Path) -> Result<Vec<RecordBatch>> {
        internal_err!("BallistaJoinSpillHook does not write single spill files")
    }
}

impl Drop for BallistaJoinSpillHook {
    fn drop(&mut self) {
        let (rows, bytes, files) = self.spilled();
        if rows > 0 {
            info!(
                "hash join spilled {rows} rows, {:.1} MiB in {files} files",
                bytes as f64 / (1024.0 * 1024.0)
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, Int64Array};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::physical_plan::common::collect;
    use datafusion::prelude::SessionConfig;

    fn batch(start: i32, rows: i32) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, false),
            Field::new("v", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from_iter_values(start..start + rows)),
                Arc::new(Int64Array::from_iter_values(
                    (start..start + rows).map(i64::from),
                )),
            ],
        )
        .unwrap()
    }

    fn context(dir: &std::path::Path, max_disk: u64) -> Arc<TaskContext> {
        let disk = DiskManagerBuilder::default()
            .with_mode(DiskManagerMode::Directories(vec![dir.to_path_buf()]))
            .with_max_temp_directory_size(max_disk);
        let runtime = RuntimeEnvBuilder::new()
            .with_disk_manager_builder(disk)
            .build_arc()
            .unwrap();
        Arc::new(
            TaskContext::default()
                .with_session_config(SessionConfig::new())
                .with_runtime(runtime),
        )
    }

    async fn rows(streams: Vec<SendableRecordBatchStream>) -> Vec<i32> {
        let mut out = vec![];
        for stream in streams {
            for b in collect(stream).await.unwrap() {
                let k = b.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
                out.extend(k.values().iter().copied());
            }
        }
        out.sort_unstable();
        out
    }

    #[tokio::test]
    async fn round_trips_buckets_and_sides_separately() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let ctx = context(dir.path(), 1 << 30);
        let hook = BallistaJoinSpillHook::default();
        hook.bind_task_context(&ctx);

        hook.spill_partition_batches(1, JoinSide::Left, &[batch(0, 10), batch(10, 5)])?;
        hook.spill_partition_batches(1, JoinSide::Left, &[batch(100, 3)])?;
        hook.spill_partition_batches(1, JoinSide::Right, &[batch(500, 4)])?;
        hook.spill_partition_batches(2, JoinSide::Left, &[batch(900, 2)])?;

        let build = rows(hook.read_partition_spills(1, JoinSide::Left)?).await;
        assert_eq!(build, (0..15).chain(100..103).collect::<Vec<_>>());
        assert_eq!(rows(hook.read_partition_spills(1, JoinSide::Right)?).await, (500..504).collect::<Vec<_>>());
        assert_eq!(rows(hook.read_partition_spills(2, JoinSide::Left)?).await, vec![900, 901]);
        // Read once: a bucket is gone after it has been read.
        assert!(hook.read_partition_spills(1, JoinSide::Left)?.is_empty());
        assert!(hook.read_partition_spills(3, JoinSide::Right)?.is_empty());
        assert_eq!(hook.spilled().0, 24);
        Ok(())
    }

    #[tokio::test]
    async fn deletes_files_once_read() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let ctx = context(dir.path(), 1 << 30);
        let disk = Arc::clone(&ctx.runtime_env().disk_manager);
        let hook = BallistaJoinSpillHook::default();
        hook.bind_task_context(&ctx);

        hook.spill_partition_batches(7, JoinSide::Left, &[batch(0, 10_000)])?;
        hook.spill_partition_batches(7, JoinSide::Right, &[batch(0, 10_000)])?;
        assert!(disk.used_disk_space() > 0);

        let left = hook.read_partition_spills(7, JoinSide::Left)?;
        let right = hook.read_partition_spills(7, JoinSide::Right)?;
        assert_eq!(rows(left).await.len() + rows(right).await.len(), 20_000);
        assert_eq!(disk.used_disk_space(), 0, "spill files must be released once read");
        Ok(())
    }

    #[tokio::test]
    async fn fails_cleanly_beyond_the_disk_limit() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let ctx = context(dir.path(), 64 * 1024);
        let hook = BallistaJoinSpillHook::default();
        hook.bind_task_context(&ctx);

        let err = hook
            .spill_partition_batches(1, JoinSide::Left, &[batch(0, 100_000)])
            .expect_err("100k rows exceed a 64 KiB spill budget");
        assert!(
            err.to_string().contains("exceeded the allowable limit"),
            "{err}"
        );
        Ok(())
    }

    /// A SQL-planned partitioned join, with every reservation counted as memory
    /// pressure so its build side spills bucket after bucket (and restored buckets spill
    /// again), returns the same answer as without spilling and leaves no files behind.
    #[tokio::test]
    async fn partitioned_join_spills_to_disk_and_returns_every_row() -> Result<()> {
        use datafusion::datasource::MemTable;
        use datafusion::physical_plan::{collect, displayable};
        use datafusion::prelude::SessionContext;

        let dir = tempfile::tempdir()?;
        let disk = DiskManagerBuilder::default()
            .with_mode(DiskManagerMode::Directories(vec![dir.path().to_path_buf()]))
            .with_max_temp_directory_size(1 << 30);
        let runtime = RuntimeEnvBuilder::new()
            .with_disk_manager_builder(disk)
            .with_memory_limit(512 * 1024 * 1024, 1.0)
            .build_arc()?;
        let mut config = SessionConfig::new()
            .with_target_partitions(4)
            .with_batch_size(256)
            .set_bool("datafusion.optimizer.prefer_hash_join", true)
            .set_usize("datafusion.optimizer.hash_join_single_partition_threshold", 0)
            .set_usize("datafusion.optimizer.hash_join_single_partition_threshold_rows", 0);
        config.options_mut().execution.hash_join_spill_pressure_threshold = 1e-12;
        let ctx = SessionContext::new_with_config_rt(config, Arc::clone(&runtime));

        // l: 200k rows, key = id % 50_000 (4 rows per key); r: one row per key.
        let l: Vec<Vec<RecordBatch>> = (0..4)
            .map(|p| (0..50).map(|b| batch(p * 50_000 + b * 1_000, 1_000)).collect())
            .collect();
        let r: Vec<Vec<RecordBatch>> = (0..2)
            .map(|p| (0..25).map(|b| batch(p * 25_000 + b * 1_000, 1_000)).collect())
            .collect();
        let schema = l[0][0].schema();
        ctx.register_table("l", Arc::new(MemTable::try_new(Arc::clone(&schema), l)?))?;
        ctx.register_table("r", Arc::new(MemTable::try_new(schema, r)?))?;
        let sql = "select count(*), sum(l.v), sum(r.v) from l join r on l.k % 50000 = r.k";
        let plan = ctx.sql(sql).await?.create_physical_plan().await?;
        let shown = displayable(plan.as_ref()).indent(true).to_string();
        assert!(shown.contains("HashJoinExec: mode=Partitioned"), "{shown}");

        let expected = collect(plan, ctx.task_ctx()).await?;

        // Operators such as RepartitionExec execute each partition once per instance,
        // so the spilling run gets a freshly planned copy (as each Ballista task does).
        let plan = ctx.sql(sql).await?.create_physical_plan().await?;
        let hook = Arc::new(BallistaJoinSpillHook::default());
        let hooked = {
            let hook = Arc::clone(&hook);
            with_join_spill_hooks_from(plan, move || Arc::clone(&hook) as _)?
        };
        let actual = collect(hooked, ctx.task_ctx()).await?;

        assert_eq!(
            datafusion::arrow::util::pretty::pretty_format_batches(&actual)?.to_string(),
            datafusion::arrow::util::pretty::pretty_format_batches(&expected)?.to_string()
        );
        let (rows, bytes, files) = hook.spilled();
        assert!(rows > 0 && bytes > 0 && files > 0, "must spill: {rows} rows");
        assert_eq!(
            runtime.disk_manager.used_disk_space(),
            0,
            "every spill file must be released"
        );
        Ok(())
    }

    #[tokio::test]
    async fn requires_a_bound_context() {
        let hook = BallistaJoinSpillHook::default();
        let err = hook
            .spill_partition_batches(1, JoinSide::Left, &[batch(0, 1)])
            .expect_err("no task context");
        assert!(err.to_string().contains("task context"), "{err}");
    }
}
