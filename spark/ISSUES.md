# Spark on Ballista — issue tracker

Every error or incompatibility found while building Spark support. Fixed issues
stay here as history (what broke, why, how it was fixed), so regressions are
recognisable. Roadmap steps refer to `~/Documents/ballista/roadmap/ROADMAP.md`.

**Status:** 🔴 open · 🟡 in progress · 🟢 fixed · ⚪ accepted difference (documented)
**Severity:** S1 wrong answer / data loss · S2 feature broken · S3 performance · S4 cosmetic

| ID | Status | Sev | Title | Step |
|---|---|---|---|---|
| [BS-001](#bs-001) | 🟢 | S2 | Sail build breaks under Ballista's DataFusion `sql` feature (error enum) | 00 |
| [BS-002](#bs-002) | 🟢 | S2 | Sail wildcard options don't compile with `sql` feature types | 00 |
| [BS-003](#bs-003) | 🟢 | S1 | Planning session ignored Ballista's distributed-safety restrictions | 00 |
| [BS-004](#bs-004) | 🟢 | S2 | Python interpreter not initialized in standalone binaries | 00 |
| [BS-005](#bs-005) | 🟢 | S2 | Catalog commands executed on executors | 00 |
| [BS-006](#bs-006) | 🟢 | S2 | Sail crate `data/` dirs ignored by git | 00 |
| [BS-007](#bs-007) | 🟢 | S3 | Spark Connect jobs run without AQE | 03 |
| [BS-008](#bs-008) | 🔴 | S4 | DDL returns a `value: true` row instead of no columns | 04 |
| [BS-009](#bs-009) | ⚪ | S4 | Decimal averages truncated instead of HALF_UP | 04 |
| [BS-010](#bs-010) | 🔴 | S2 | `df.repartition(n)` is not a Ballista stage boundary | 03 |
| [BS-011](#bs-011) | 🔴 | S2 | Executors don't register object stores from Sail settings | 05 |
| [BS-012](#bs-012) | 🟢 | S3 | Closing a Spark session doesn't cancel its Ballista jobs | 02 |
| [BS-013](#bs-013) | 🔴 | S2 | `BallistaSubplanExec` is single-partition; untested under driver-side writes | 05 |
| [BS-014](#bs-014) | 🔴 | S3 | Redundant shuffles from the front end's repartitions | 03 |
| [BS-015](#bs-015) | 🔴 | S3 | Uneven file-partition assignment makes one scan task a straggler | 07 |
| [BS-016](#bs-016) | 🟢 | S2 | Hash join build side cannot spill (DataFusion 55) | 06 |
| [BS-017](#bs-017) | 🟢 | S1 | Table path missing on the scheduler returns empty results, no error | 05 |
| [BS-018](#bs-018) | 🔴 | S2 | Hash join: one key larger than memory still fails after 4 spill levels | 06 |
| [BS-019](#bs-019) | 🟢 | S4 | DataFusion fork's hash-join code is not `cargo fmt` clean | 06 |
| [BS-020](#bs-020) | 🟢 | S1 | Managed tables written on executors are invisible to queries (node-local warehouse) | 05 |
| [BS-021](#bs-021) | 🟢 | S3 | `DROP TABLE` leaves a managed table's files in the warehouse | 05 |
| [BS-022](#bs-022) | 🟢 | S1 | Job hangs when a union branch is empty at runtime (task metrics mismatch) | 05 |
| [BS-023](#bs-023) | 🟢 | S1 | Adaptive jobs never free intermediate shuffle data early; TPC-DS filled the disk | 05 |
| [BS-024](#bs-024) | 🟢 | S1 | `UNION ALL` with a branch empty at runtime duplicates or loses rows (TPC-DS q23) | 03 |
| [BS-025](#bs-025) | 🟢 | S2 | AQE waits for both join inputs before broadcasting; TPC-DS q72 shuffles ~80 GB and fills the disk | 07 |
| [BS-026](#bs-026) | 🔴 | S3 | Every task rebuilds a broadcast join's hash table from scratch (TPC-DS q23 ~40% slower after BS-025) | 07 |
| [BS-027](#bs-027) | 🟢 | S1 | `spark.range(..., numPartitions)` results multiplied: every task ran all of the range's partitions | 02 |
| [BS-028](#bs-028) | 🟡 | S3 | TPC-DS q72 takes 124 s (vanilla Spark 17 s, Spark + Comet 2.2 s) | 07 |

---

## BS-001
**Sail build breaks under Ballista's DataFusion `sql` feature (error enum)** · 🟢 fixed
- **Error:** `error[E0004]: non-exhaustive patterns: &DataFusionError::SQL(_, _) not covered` in `sail-common-datafusion/src/error.rs`
- **Cause:** Ballista depends on DataFusion with default features (incl. `sql`); Sail
  builds without it. Cargo unifies features, so the enum gains a variant.
- **Fix:** map `DataFusionError::SQL` to Sail's `Plan` error; declare `sql` in the
  workspace DataFusion features so the dependency is explicit.

## BS-002
**Sail wildcard options don't compile with `sql` feature types** · 🟢 fixed
- **Error:** `unresolved import datafusion_expr::sql` and type mismatches in
  `sail-plan/src/resolver/expression/wildcard.rs`
- **Cause:** with `sql`, `datafusion_expr` uses `sqlparser` AST types for
  `SELECT * EXCLUDE/EXCEPT/REPLACE/RENAME` instead of its stand-in module.
- **Fix:** build the options with `sqlparser` types. Verified in DataFusion 55.2 that
  `ReplaceSelectElement.expr` and `RenameSelectItem` are display-only
  (`replace_columns` uses `column_name` + `planned_expressions`), so results are unchanged.

## BS-003
**Planning session ignored Ballista's distributed-safety restrictions** · 🟢 fixed (prevented)
- **Risk:** Sail plans with physical scalar subqueries (shared in-process container
  can't cross stages → executor decode failure), dynamic filter pushdown (scan
  waits forever for a filter produced in another stage), round-robin repartition and
  Utf8View (oversized Flight frames).
- **Fix:** `BallistaSparkSessionMutator` applies `ballista_restricted_configuration()`
  after Sail's settings (Sail calls the mutator last, verified in `ServerSessionFactory`).

## BS-004
**Python interpreter not initialized in standalone binaries** · 🟢 fixed
- **Error:** `panicked at pyo3-0.29.2/src/interpreter_lifecycle.rs:134: The Python interpreter is not initialized and the auto-initialize feature is not enabled`; client saw `internal error: failed to send message to actor`
- **Cause:** Sail normally runs inside Python (`pysail`) or its CLI initializes Python;
  session creation discovers Python data sources.
- **Fix:** `ballista-spark-python::bootstrap()` (ported from Sail's CLI) first thing in
  all three binaries, before any threads; Tokio runtime built afterwards.

## BS-005
**Catalog commands executed on executors** · 🟢 fixed
- **Error:** `Task failed due to runtime execution error: ... Internal("session extension not found: catalog manager")` on `createOrReplaceTempView`
- **Cause:** Sail runs catalog commands as `CatalogCommandExec` plans through the job
  runner; they need the Spark session, which only exists in the Spark Connect server.
- **Fix:** `ballista-spark-runner::placement` keeps driver-only operators (Sail's
  `is_driver_stage_plan`, now public) in the server; other subtrees run as Ballista jobs
  via `BallistaSubplanExec`.

## BS-006
**Sail crate `data/` dirs ignored by git** · 🟢 fixed
- **Cause:** repository `.gitignore` has `data/`; Sail keeps build inputs and test
  fixtures in `crates/*/data/` (read by `sail-sql-parser` and `sail-spark-connect` build scripts).
- **Fix:** `spark/.gitignore` re-includes `crates/*/data/`.

## BS-007
**Spark Connect jobs run without AQE** · 🟢 fixed · step 03
- **Evidence:** 0 of 28 jobs from a TPC-H SF10 run had an `AdaptiveDatafusionExec`
  root; TPC-H total 30.7 s via Spark Connect vs 12.6 s through Ballista's SQL path.
- **Cause:** `scheduler/src/state/task_manager.rs` routes `SubmitPlan::Physical` to the
  static planner ("AQE plans from the logical plan").
- **Fix (Ballista core):** `AdaptivePlanner::try_new_from_physical` applies the
  plan-preparation rules to the submitted plan, then the normal adaptive chain; routed
  when `ballista.planner.adaptive.physical_plans.enabled` (default **false**, so
  existing behaviour is unchanged). Spark planning sessions enable it. Existing
  repartitions and sorts are kept, so global ordering is preserved.
- **Tests:** 4 scheduler unit tests (`aqe/test/physical_submission.rs`) + 1 end-to-end
  test comparing ordered results with local execution; full scheduler suite 537/537.
- **Result:** 24/24 Spark Connect jobs use AQE; TPC-H SF10 30.7 s → 24.2 s; answers still
  22/22 vs vanilla Spark. Remaining gap: BS-014.

## BS-008
**DDL returns a `value: true` row instead of no columns** · 🔴 open · step 04
- **Repro:** `spark.sql("drop view v").columns` → Spark: `[]`; ours: `['value']`, row `True`.
- **Impact:** code that inspects DDL results; our TPC-H runner overwrote Q15's answer
  (runner fixed to skip DDL).
- **Plan:** match Spark's empty result for DDL in the Spark front end.

## BS-009
**Decimal averages truncated instead of HALF_UP** · ⚪ documented · step 04
- **Repro:** TPC-H Q1 `avg_price` 38237.151008 vs Spark 38237.151009.
- **Cause:** DataFusion decimal division truncates at the result scale.
- **Plan:** HALF_UP in the Spark function layer; within comparator tolerance meanwhile.

## BS-010
**`df.repartition(n)` is not a Ballista stage boundary** · 🔴 open · step 03
- **Cause:** Sail's optimizer leaves round-robin `ExplicitRepartitionExec` for its own job
  graph; Ballista's planner splits stages at DataFusion repartition operators only.
- **Plan:** rewrite to a Ballista-supported repartition when submitting to Ballista.

## BS-011
**Executors don't register object stores from Sail settings** · 🔴 open · step 05
- **Cause:** executors use Ballista's default runtime environment; Sail registers S3,
  GCS, Azure, HDFS stores through its own `RuntimeEnvFactory`.
- **Plan:** executor runtime producer that adds Sail's object store registry while keeping
  Ballista's memory pool and spill settings.

## BS-012
**Closing a Spark session doesn't cancel its Ballista jobs** · 🟢 fixed · step 02
- **Cause:** `BallistaJobRunner::stop` only logged. Interrupts did nothing either:
  `execute_physical_plan` followed the job to its end before returning a stream, so Sail
  registered the operation only after the job finished (`interruptAll` returned `[]`); a
  dropped result stream never cancelled its job; and the push client learnt the job id
  only from status updates, of which none came while a job ran.
- **Fix:** `submit_physical_plan` returns the stream at once (the job is submitted on first
  poll) with a `JobCancelHandle`; the stream cancels its job if dropped before its end; the
  scheduler returns the job id in the `ballista-job-id` response header; the runner keeps
  its session's handles and `stop()` cancels the unfinished ones. Sail now answers a
  reattach to an interrupted operation with `[OPERATION_CANCELED]` (see `VENDORED.md`).
- **Verified:** `spark/tests/connect_operations.py`: `interruptAll`, `interruptTag` and
  `stop()` stop the Ballista job within a second; the client gets `SparkException` with
  condition `OPERATION_CANCELED`; reattach after cut connections returns the answer.

## BS-013
**`BallistaSubplanExec` is single-partition; untested under driver-side writes** · 🔴 open · step 05
- **Risk:** driver-only parents (e.g. `DeltaCommitExec`, `FileDeleteExec`) expecting
  N input partitions may misbehave.
- **Plan:** Delta/Iceberg write tests through Spark Connect; preserve partitioning if needed.

## BS-014
**Redundant shuffles from the front end's repartitions** · 🔴 open · step 03
- **Evidence:** TPC-H Q9 (SF10). Ballista SQL path: 8 stages, 5 sort-shuffle writes,
  0.95–1.4 s. Spark Connect + AQE: 13 stages, 11 sort-shuffle writes, 3.1 s. Both end
  with the same 5 hash joins. TPC-H total still 24.2 s vs 12.6 s.
- **Cause:** the front end's physical plan already contains hash `RepartitionExec`s
  placed for its own join choices; `try_new_from_physical` keeps them (by design, for
  ordering safety), and AQE turns each into a shuffle.
- **Refined cause (Q9 stage dump):** every join in the front end's plan is a partitioned
  hash join with both inputs repartitioned, so the 60 M-row `lineitem` is shuffled in
  full; Ballista's SQL path broadcasts `part`/`supplier`/`nation` (`CollectLeft`) after
  measuring them and shuffles only 3.3 M rows. Q9: 13 stages vs 8; 2.4 s vs 1.1 s.
- **Attempt 1 (2026-11-01, rolled back):** `StripRequiredDistributionRule` removed
  repartitions that only satisfy a parent's required distribution before AQE. Unit and
  end-to-end tests passed and answers stayed 22/22, but TPC-H got slower
  (24.2 s → 27.9 s; Q8 1.93 → 3.71 s) and stage counts did not drop, so the joins
  were still not broadcast. Patch kept at `roadmap/attempts/bs-014-strip-required-distribution.patch`.
- **Next:** trace one join through `DelayJoinSelectionRule` / `SelectJoinRule` on the
  Spark path to see why the measured build side does not lead to `CollectLeft`
  (candidates: join type or Sail-specific operators between join and scan blocking
  build-side staging; statistics not reported for Sail scans).

## BS-015
**Uneven file-partition assignment makes one scan task a straggler** · 🔴 open · step 07
- **Evidence (web UI, stage page):** TPC-H Q9 via Spark Connect, stage 5 (`lineitem`
  scan, 12 tasks): task 0 got partitions 0–5 and 12.9 M rows (770 ms); median task
  4.3 M rows (559 ms), min 2.1 M. Stage time is set by task 0 (1.2 s of a 2.3 s job).
- **Likely cause:** partitions are grouped into tasks without balancing by size.
- **Plan:** confirm on Ballista's SQL path; balance task groups by bytes/rows, or split
  oversized groups (relates to skew handling in step 07).

## BS-016
**Hash join build side cannot spill (DataFusion 55)** · 🟢 fixed · step 06
- **Cause:** upstream `HashJoinExec` only `try_grow`s its reservation; a build side
  larger than the pool fails with `ResourcesExhausted`. Ballista mitigates via AQE's
  switch to sort-merge join above `hash_join_max_build_partition_bytes` (SMJ, sort and
  aggregates do spill).
- **Candidate:** the DataFusion 55.2.0 spill fork (`~/Documents/datafusion`,
  `df-55.2.0-comet`): grace hash join with a `JoinSpillHook`. Review:
  `roadmap/notes/datafusion-spill-fork.md` — needs a Ballista hook, and a fix for a
  panic when no hook is set and the pool is above the proactive threshold.
- **Progress 2026-11-01:** panic fixed in the fork (`df-55.2.0-ballista`, `bd36d3c56`,
  reproduced by a test first; 1697 physical-plan tests pass). Aggregates reviewed:
  partial phase emits early, final phase spills when temp files are enabled (they are
  on Ballista executors) — no change needed. Next: Ballista `JoinSpillHook`, build
  against the fork, memory-pressure proof.
- **Progress 2026-11-01 (2):** testing the hook contract found 8 pre-existing
  correctness bugs in the fork's grace hash join (lost/duplicated rows once 2+ buckets
  spill; e.g. 3356 of 4000 rows). Fixed in `443183b7c` with tests; table in
  `roadmap/notes/datafusion-spill-fork.md`. They also affect the Comet branch.
- **Progress 2026-11-01 (3):** Ballista hook `BallistaJoinSpillHook` (`f6f6fd3`):
  Arrow IPC spill files through the executor's `DiskManager` (counted against
  `--max-temp-directory-size`, removed when read back), attached to every
  partitioned hash join of a task plan; `ballista.join.spill.enabled` (default true).
- **Progress 2026-11-01 (4):** cluster proof first failed: executors at 512 MB pools
  (8 tasks each) ran a 60 M × 15 M-row join and died with `ResourcesExhausted` in
  `HashJoinInput[n]` (`can spill: false`) although the hook had spilled 2.2 M rows.
  Causes in the fork: the build reservation was unspillable, so the first joins took
  the whole fair pool; the post-build hash-table check reserved and *released* the
  memory, so another task claimed it, and it omitted the concat copy and bitmap. Fixed
  in `bd2b42f8b` (spillable reservation with a hook; post-build memory reserved and
  held). Reproduced first: 8–16 concurrent partitions on a multi-threaded runtime,
  14/15 runs failed → 42/42 correct.
- **Result:** on 512 MB executors, the join spills 1.7 GB (51.6 M build rows, 352
  files) and matches vanilla Spark; spill files removed. TPC-H SF10 with hash join
  forced everywhere (`hash_join_max_build_partition_bytes=0`): 22/22 match Spark.

## BS-017
**Table path missing on the scheduler returns empty results, no error** · 🟢 fixed · step 05
- **Evidence:** the Spark compose scheduler lacked the `~/Documents/tpch` mount
  (executors had it). Ballista SQL-path queries over those tables completed with
  0 rows instead of failing.
- **Cause (likely):** the scheduler plans the scan by listing files; a missing
  directory lists as an empty table.
- **Plan:** fail planning when a registered table's location does not exist, as Spark
  does (`Path does not exist`). Mount fixed in `cluster/spark/compose.yml`.
- **Reproduced 2026-11-01:** SF1 copied where only the client container sees it; TPC-H
  Q6 returned one row with an empty `revenue` (correct: 123141078.23), no error.
- **Fix (Ballista SQL path):** before planning a logical-plan job the scheduler checks
  every listing table's location (`state/table_paths.rs`, including subqueries): a local
  path must exist (an existing empty directory is an empty table), an object-store
  location must be an object or a prefix of one. Otherwise `[PATH_NOT_FOUND] Path does
  not exist`. Setting `ballista.planner.require_table_paths` (default true).
- **Same bug on the Spark Connect path:** `spark.read.parquet("/missing").count()`
  returned 0. Sail treated a missing path as an empty directory. Fixed in the copied
  `sail-data-source` like Spark's `DataSource.checkAndGlobPathIfNecessary`: direct reads
  (`spark.read`, ``parquet.`path` ``) fail on a missing path or a glob matching nothing;
  catalog-table reads skip the check (a new managed table has no directory yet).
- **Verified:** repro now fails with `PATH_NOT_FOUND`; TPC-H SF10 22/22 vs Spark on both
  the Ballista SQL path and Spark Connect.

## BS-018
**Hash join: one key larger than memory still fails after 4 spill levels** · 🔴 open · step 06
- **Cause:** grace hash join splits a spilled bucket by re-hashing its rows into new
  buckets. Rows with the same key always land in one bucket, so one very hot key cannot
  be split. The fork stops at `MAX_SPILL_DEPTH = 4` (so recursion ends) and builds that
  bucket in memory; if it does not fit, the query fails with `ResourcesExhausted`.
- **Evidence:** fork test `spilling_join_with_one_hot_key_terminates` (4000 rows, one
  key) returns the right answer only because the bucket fits in memory.
- **Spark:** skewed keys are handled by AQE's skew-join split (`OptimizeSkewedJoin`)
  and by sort-merge join, which streams equal-key groups and can spill them.
- **Plan:** when a bucket at max depth still does not fit, fall back to sort-merge join
  for that bucket, or rely on step 07 skew-join splitting. AQE's switch to sort-merge
  join above `hash_join_max_build_partition_bytes` (default 64 MiB) already avoids
  most cases.

## BS-019
**DataFusion fork's hash-join code is not `cargo fmt` clean** · 🟢 fixed · step 06
- **Evidence:** `cargo fmt -p datafusion-physical-plan -- --check` on
  `~/Documents/datafusion` (`df-55.2.0-ballista`) reports diffs in
  `hash_join/exec.rs` (including the fixes in `443183b7c`, `bd2b42f8b`) and in
  `aggregates/grouped_hash_stream.rs`, code that was already in the fork.
- **Plan:** run `cargo fmt` as its own commit on both fork branches, so later fix diffs
  stay readable and the fork could be offered upstream.
- **Fixed:** formatting-only commits `ad47fc6ee`, `05ec5fe64` (`df-55.2.0-ballista`) and
  `a7b9ce9c8`, `6c2df7367` (`df-55.2.0-comet`); 1703 physical-plan tests pass.

## BS-020
**Managed tables written on executors are invisible to queries (node-local warehouse)** · 🟢 fixed · step 05
- **Evidence:** via Spark Connect, `CREATE TABLE t (a int) USING parquet`, `INSERT` 2
  rows, `SELECT count(*)` → **0**. The default warehouse is `spark-warehouse` relative to
  each process; executor-2 wrote `/spark-warehouse/t-…` on its own disk, while the Spark
  Connect server lists its own disk when planning the read.
- **Spark:** the same requirement holds (the warehouse must be shared storage, e.g. HDFS
  or S3); `spark.sql.warehouse.dir` is a static setting given to the server.
- **Fix:** `ballista-connect --conf key=value` (repeatable) sets Spark configuration for
  every session, like `start-connect-server.sh --conf`; the server warns at startup when
  `spark.sql.warehouse.dir` is unset and notes that a local path must be the same shared
  mount everywhere. The cluster mounts `cluster/spark/warehouse` at `/warehouse` in the
  server and both executors.
- **Verified:** insert 1000 + 2 rows → `count 1002, sum 499503`; CTAS → correct.

## BS-021
**`DROP TABLE` leaves a managed table's files in the warehouse** · 🟢 fixed · step 05
- **Evidence:** after `DROP TABLE` of two managed tables, both directories stayed in
  `cluster/spark/warehouse`. Spark deletes a managed table's data on drop (external
  tables keep theirs).
- **Impact:** disk use only: a re-created table gets a new `name-<uuid>` directory, so
  old files are not read back.
- **Plan:** delete the directory of a managed (non-external) table on drop, in the
  catalog command, after the catalog entry is removed.
- **Fix:** `DropTable` reads the table first; for a managed table with a location it
  deletes the local directory (`remove_dir_all`, missing is fine, never `/`) or every
  object under the prefix (never a bucket root) after the catalog entry is removed.
- **Verified:** on the cluster, managed table (1000 rows) directory removed on drop,
  external table's files kept, re-created table reads 0 rows; unit test covers local
  and object-store deletion.

## BS-022
**Job hangs when a union branch is empty at runtime (task metrics mismatch)** · 🟢 fixed · step 05
- **Evidence:** TPC-DS q23 (second SELECT) hung on Spark Connect: stage 38 showed all
  tasks succeeded but stayed Running; the scheduler logged `task metrics array size 8
  does not equal with the stage metrics array size 9` for each rejected task. Minimal
  repro: `UNION ALL` of two aggregates where one branch's filter matches nothing
  (`ws_quantity > 1000`), so AQE turns that branch into `EmptyExec`.
- **Cause:** a task whose partitions skip that branch got its `EmptyExec` (no metrics)
  replaced by a 0-partition memory scan (has metrics), so its metrics list was one
  longer than its siblings'. The scheduler turned the mismatch into an error and dropped
  the whole status update, task completion included, so the stage never finished.
- **Fix:** the stand-in is a new `EmptyPartitionsExec` (Ballista codec keeps its partition
  count; no metrics, like `EmptyExec`), so task metrics match the stage plan; and a
  metrics mismatch now drops only that task's metrics with a warning.
- **Verified:** repro returns the same rows as Spark; scheduler/core tests pass.

## BS-023
**Adaptive jobs never free intermediate shuffle data early; TPC-DS filled the disk** · 🟢 fixed · step 05
- **Evidence:** a TPC-DS SF10 run on Spark Connect filled `/home` (executor `/work` in the
  container layer): about 49 GB in 74–76 job directories per executor; from q39 on queries
  failed with `No space left on device`, from q61 tasks were rejected. After q1–q5 alone each
  executor held 3.6–4.2 GB; no `Remove intermediate data` log line ever appeared.
- **Cause:** on success the scheduler deletes intermediate stages at once and the whole job
  after `finished_job_data_clean_up_interval_seconds` (300 s). Intermediate stages are the
  ones with output links, but the adaptive graph creates every stage without them, so the
  list was always empty. A fast run produced data faster than the 300 s cleanup removed it.
- **Fix:** `AdaptiveExecutionGraph::intermediate_stage_ids` returns every stage (and every
  stage retired by a replan) except the one the job's output locations point at.
- **Verified:** after q1–q5 each executor keeps only the final stage (116K / 44K); whole-job
  cleanup still removes the rest after 300 s; no deleted-but-open files in the executor.

## BS-024
**`UNION ALL` with a branch empty at runtime duplicates or loses rows (TPC-DS q23)** · 🟢 fixed · step 03
- **Evidence:** TPC-DS SF10 q23's second SELECT returns `Thompson, Fidel, 5834.01` only;
  Spark also returns `Barnes, Lowell, 202.14`. The first SELECT matches (6036.15). This is
  the query that hung before BS-022; its plan has a `UNION ALL` branch that AQE turns empty.
- **Second symptom:** `SELECT count(*), sum(c)` over `store_sales` agg `UNION ALL` an
  aggregate of `web_sales WHERE ws_quantity > 1000` (empty) returned 174,857 / 130,867 rows
  on two runs; Spark returns 102,000. Each branch alone was right.
- **Cause:** AQE turned the empty branch into a 28-partition `EmptyExec`. Because DataFusion's
  protobuf drops `EmptyExec`'s partition count, `make_empty_exec_serde_safe` rewrote it as
  `RepartitionExec(RoundRobinBatch(28))` over a 1-partition `EmptyExec`. Per-task
  restriction cannot slice a repartition, so each task's union had 28 partitions on top of
  its share (36 instead of 8). The pass-through shuffle writer drains every local
  partition and maps those past the task's slice to their own index, so a task wrote two
  partitions to the same shuffle file: the reader read the file twice (duplicates: exactly
  20 of 28 partitions here) or the empty write won (q23's lost row).
- **Fix:** the rewrite produces `EmptyPartitionsExec(N)`, a leaf that keeps its partition
  count on the wire; restriction slices `EmptyExec(N)` / `EmptyPartitionsExec` to the task's
  share; a restricted union child with more partitions than its share is an error; the
  pass-through writer fails a task whose plan has a different partition count than its
  slice instead of writing over other partitions' files.
- **Verified:** union repro 102,000 rows twice; q23 second SELECT returns both rows.
- **Not covered:** the pipelined (streaming shuffle) writer has no such check yet.

## BS-025
**AQE waits for both join inputs before broadcasting; TPC-DS q72 shuffles ~80 GB and fills the disk** · 🟢 fixed · step 07
- **Evidence:** TPC-DS SF10 q72 on Spark Connect: after 5 minutes the job held 78 GB of
  shuffle data (sort-shuffle writers spilled ~6 GB per input partition) and `/home` was down
  to 12 GB; stopped by hand. The other 98 queries match Spark.
- **Plan shape:** catalog_sales ⋈ inventory on `item_sk` (with `inv_quantity_on_hand <
  cs_quantity`) runs first, as in Spark, and produces a very large intermediate. Spark joins
  it to warehouse, item, demographics and three date_dim copies with broadcast hash joins in
  the same stage, so it is never written out. Here every one of those joins waits for the
  large side to be shuffled to disk first.
- **Cause:** `DynamicJoinSelectionExec` decides a join only when `upstream_resolved()`: neither
  input may contain an unresolved exchange or an undecided join. The small sides finish at
  once (warehouse is 10 rows, stage 8) but the large side is the join below, so each join is
  planned as shuffled.
- **Plan:** decide a broadcast as soon as one side is resolved, under the broadcast threshold
  and safe to build from (join type), without waiting for the other side; the other side then
  needs no exchange and streams through in the same stage, as in Spark.
- **Also seen:** stopping the client did not stop the job (BS-012); only stopping the cluster
  freed the disk.
- **Second cause:** the planner's sort-merge and partitioned joins arrive with a hash
  repartition (and sort) on the join keys under each input. Kept under the dynamic join, they
  made both inputs look already shuffled, so a broadcast was never considered.
- **Fix:** (1) the dynamic join drops the planned repartition/sort on each input's join keys;
  it adds exchanges itself if it picks a shuffled join. (2) `early_collect_left`: when exactly
  one input is resolved, its size is measured (not estimated through a join), under the
  broadcast threshold, and the join type can build from it, the join is planned as a
  broadcast at once and the other input streams through it in its own stage.
  (3) `PinBroadcastJoins` hides `CollectLeft` joins from DataFusion's `join_selection`, which
  otherwise swapped them on estimates (collecting the large side) and failed on joins that
  already carry a dynamic filter.
- **Verified:** q72 117 s, exact match with Spark, `/work` stays under 30 KB; the job runs the
  big join and all nine broadcast joins in one stage, as Spark does. TPC-H 22/22 and TPC-DS
  99/99 match Spark; the 78 TPC-DS queries timed in both runs take 60.5 s instead of 121.3 s
  (q14 18.7 → 5.8 s, q21 5.6 → 0.5 s). Slower: q23 (BS-026).

## BS-026
**Every task rebuilds a broadcast join's hash table from scratch (TPC-DS q23 ~40% slower after BS-025)** · 🔴 open · step 07
- **Evidence:** TPC-DS SF10 q23 takes 13–17 s (three runs) instead of 10.3 s before BS-025.
  Its two slowest stages join store_sales (28.8M rows) to customer (500K rows), now broadcast,
  where both sides were shuffled before. All other queries got faster or stayed the same.
- **Cause:** a `CollectLeft` join's build side is read and hashed by every task: a scan build
  side is read as one partition by each of the stage's 28 tasks, a broadcast exchange is
  fetched by each. Spark builds the hash relation once and ships it to executors.
- **Plan:** build each broadcast hash table once per executor and share it across that
  executor's tasks of the stage (keyed by job, stage and build plan); consider a lower
  threshold for scan build sides until then.

## BS-027
**`spark.range(..., numPartitions)` results multiplied: every task ran all of the range's partitions** · 🟢 fixed · step 02
- **Evidence:** `spark.range(0, n, 1, 56).select(sum(id % 7))` failed with "shuffle writer
  input has 56 partition(s) but the task owns 8" once BS-024 added that guard; before it,
  each of the stage's 7 tasks ran all 56 partitions and wrote them, multiplying the result.
- **Cause:** per-task restriction (`restrict_plan_to_partitions`) only knows shuffle
  readers, file and memory scans and empty leaves. Any other leaf (Sail's `RangeExec`, an
  unrecognised data source) was left whole in every task.
- **Fix:** such a leaf is wrapped in `SelectPartitionsExec`, which exposes only the task's
  partitions (keeping those the leaf has, as file scans do). It is serialized by Ballista's
  codec and reports no metrics, so task and stage plans keep the same metrics slots.
  `UNION ALL` branches over such leaves can now stay in the union's stage (one stage fewer).
- **Verified:** sums and counts exact for 3, 8, 56 and 100 partitions and 2 billion rows.

## BS-028
**TPC-DS q72 takes 124 s (vanilla Spark 17 s, Spark + Comet 2.2 s)** · 🟡 in progress (now 71 s) · step 07
- **Evidence:** SF10, 16 cores each, same machine (runs of 2026-11-01; Comet's of 2026-10-12
  used the `datafusion-benchmarks` copy of the query). With q72 excluded Ballista's TPC-DS
  total is 76.8 s against Comet's 123.8 s and Spark's 305.1 s; q72 alone decides the total.
- **Shape (after BS-025):** stage 5 joins the shuffled catalog_sales (14.4M rows) and inventory
  (133M rows) with a partitioned hash join on `item_sk` with filter
  `inv_quantity_on_hand < cs_quantity`, then streams through nine broadcast joins.
- **Found:** Spark runs the same join order, with a sort-merge join for catalog_sales ×
  inventory and no runtime filters. The join emits 1.12 billion rows; about 140 catalog_sales
  rows share each item key. Timed alone in DataFusion 55 (16 partitions): hash join 104.2 s,
  sort-merge join 35.6 s. The hash join evaluates the filter on every pair it gathers from
  its hash chains; the sort-merge join walks the pairs key group by key group.
- **Fix 1:** AQE runs a shuffled inner or outer join with a non-equi filter as a sort-merge
  join (`ballista.optimizer.sort_merge_filtered_joins`, default on), as Spark does for every
  shuffled join. Semi and anti joins keep the hash join: they stop at the first match, and
  sort-merge made TPC-H q21 1.94 → 2.76 s and TPC-DS q16 0.31 → 0.66 s.
- **Result:** q72 125 → 71 s, q23 19.3 → 12.5 s; TPC-DS 210 → 145 s; TPC-H unchanged (15.3 s);
  22/22 and 99/99 match Spark, no query more than 1.5× slower.
- **Left:** 71 s against Spark's 17 s. DataFusion's sort-merge join alone takes 36 s here, then
  1.1 billion rows pass through nine broadcast joins. Next: per-operator time in that stage;
  rerun Comet with this repository's query text.

