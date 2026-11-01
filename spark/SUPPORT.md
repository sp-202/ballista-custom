# Spark on Ballista — what is supported

What a Spark user can rely on today, feature by feature, and what is left. Every
✅ names its evidence; anything not tested end to end is ❔, never assumed to work.
Issues are in [ISSUES.md](ISSUES.md); the plan is `~/Documents/ballista/roadmap/ROADMAP.md`.

**Status:** ✅ verified (matches Spark) · ⚠️ works with a known gap · ❌ not supported or broken · ❔ not tested yet

**Front ends.** *Connect* = PySpark / Spark SQL through `ballista-connect` (Sail planner,
Ballista execution). *SQL* = Ballista's own SQL path (DataFusion planner on the scheduler).

Last updated 2026-11-01.

## 1. Benchmarks (answers compared with vanilla Spark 3.5.8, Comet off)

| Suite | Front end | Result | Evidence |
|---|---|---|---|
| TPC-H SF10, 22 queries | SQL | ✅ 22/22 | `tpcbench/compare_answers.py`, answers in `~/Documents/tpch/answers/` |
| TPC-H SF10, 22 queries | Connect | ✅ 22/22 | same; re-verified after BS-017 |
| TPC-DS SF10, 99 queries | Connect | ✅ 99/99 | runs of 2026-11-01 after BS-022/023/024/025; `compare_answers.py` |
| TPC-DS SF10 | SQL | ❔ not run | |
| Nightly TPC-H + TPC-DS comparison | Connect | ✅ `conformance/nightly.py`, dated reports | first run 2026-11-01: 22/22, 99/99 |
| Spark `sql-tests` suite | both | ❔ not run | step 01 |

TPC-DS queries are the spec's, with two rewrites Spark itself needs: `+ 30 days` →
`+ interval 30 days`, `"quoted"` aliases → backticks (Spark rejected 18 queries otherwise).
Numbers match within `compare_answers.py` tolerance (relative 1e-9 or absolute 1e-4);
decimal averages differ in the last digit (BS-009).

## 2. Spark Connect session and DataFrame API

| Feature | Status | Evidence / gap |
|---|---|---|
| Connect `sc://host:15002`, `spark.sql(...).collect()` | ✅ | TPC-H/TPC-DS runs |
| `spark.range`, `select`, `groupBy().count()`, `orderBy`, aggregate functions | ✅ | `tests/connect_operations.py`; `range` with many partitions fixed in BS-027 |
| `spark.read.parquet(path)`, temp views | ✅ | TPC-DS runner registers 24 tables this way |
| `spark.read` of a missing path raises `PATH_NOT_FOUND` | ✅ | BS-017 |
| Server-side Spark conf (`ballista-connect --conf k=v`) | ✅ | BS-020 |
| `df.repartition(n)`, `repartition(n, cols)`, `coalesce(n)` | ✅ | BS-010, BS-030; `tests/connect_operations.py`. `coalesce(n)` costs a shuffle (Spark's does not) |
| `spark_partition_id()`, `monotonically_increasing_id()` | ✅ | BS-029; `tests/connect_operations.py` |
| Closing a session cancels its jobs | ✅ | BS-012; `tests/connect_operations.py` |
| `df.explain()` (all modes) | ✅ | `tests/connect_operations.py` |
| `spark.interruptAll` / `interruptTag`: job stopped, client gets `OPERATION_CANCELED` | ✅ | BS-012; `tests/connect_operations.py` |
| Reattach after a dropped connection | ✅ | `tests/connect_operations.py` (proxy cuts the connection mid-query) |
| `df.write` to a path (parquet) | ❔ | managed-table writes tested, path writes not |
| Other `spark.read` formats (CSV, JSON, ORC, text) | ❔ | |
| `df.cache()` / `persist()` | ❔ | scheduler has a cache linker; untested from Connect |
| PySpark UDFs (Python, pandas/Arrow, UDTF) | ❔ | only interpreter start-up fixed (BS-004); step 04 |
| Structured Streaming, RDD API, MLlib | ❌ | out of scope |

## 3. SQL

| Feature | Status | Evidence / gap |
|---|---|---|
| SELECT, joins (inner, left outer, full outer), CTEs | ✅ | TPC-H; TPC-DS q1, q5, q40, q49, q51 |
| Subqueries: scalar, `IN (SELECT …)`, `EXISTS` | ✅ | TPC-H q2/q4/q17/q20–22; TPC-DS q10, q14, q16, q33, q35, q45 |
| `UNION ALL`, `INTERSECT` | ✅ | TPC-DS q2, q4, q5, q8, q11, q14, q33, q38 |
| `UNION ALL` with a branch empty at runtime | ✅ | BS-022 (hang), BS-024 (duplicated or lost rows) |
| `EXCEPT` | ❔ | in q87, not reached yet |
| `ROLLUP`, `GROUPING()` | ✅ | TPC-DS q5, q14, q18, q22, q27, q36 |
| Window functions (`rank()`, aggregates `OVER`) | ✅ | TPC-DS q36, q44, q49, q51 |
| `CASE`, `stddev_samp`, date + interval arithmetic | ✅ | TPC-DS q17, q25, q29, q35; interval q5, q12, q16, q20, q21, q32, q37, q40 |
| Decimal arithmetic | ⚠️ | averages truncate instead of HALF_UP in the last digit (BS-009) |
| `CREATE TABLE … USING parquet`, `INSERT INTO`, CTAS (managed) | ✅ | BS-020: 1002 rows read back correctly |
| `DROP TABLE` (managed deletes data, external keeps it) | ✅ | BS-021 |
| `TRUNCATE TABLE` | ❔ | implemented in Sail, not tested here |
| DDL result shape (no columns, as in Spark) | ❌ | returns one `value: true` row (BS-008) |
| Catalogs: Hive Metastore, Glue, Iceberg REST, Unity | ❔ | available in Sail; step 05 |
| Delta write, append, overwrite, partitioned write, `UPDATE`, `DELETE` | ✅ | tested 2026-11-01 (BS-013) |
| Delta `MERGE` | ❌ | intermittently loses rows (BS-031) |
| Iceberg path tables: write, append, overwrite, partitioned write, read, partition filter, `UPDATE`, `DELETE`, `MERGE` | ✅ | BS-032, BS-029; `tests/connect_operations.py` |
| Iceberg catalogs (REST, Glue, HMS), time travel, metadata tables, schema evolution | ❔ | available in Sail; step 05 |

## 4. Data sources and writes

| Feature | Status | Evidence / gap |
|---|---|---|
| Parquet on a local or shared path | ✅ | TPC-H, TPC-DS |
| Projection pushdown, partition filters, row-group and page pruning | ✅ | 2026-11-01, 10M-row partitioned table: one column reads 18 MB of 372 MB; `d = 7` reads 1 partition; `id < 100000` keeps 1.6M of 10M rows after page pruning; answers equal Spark |
| Partitioned parquet write (`partitionBy`) | ⚠️ | works, but in one task; many partition values run out of memory (BS-034) |
| `bucketBy` | ❌ | BS-035 |
| Table path invisible to the scheduler fails instead of returning empty | ✅ | BS-017 |
| Object stores (S3, GCS, Azure, HDFS) | ✅ S3 tested | BS-011; configured from `AWS_*` / `GOOGLE_*` / `AZURE_*` environment on every node; `tests/connect_operations.py --s3-path` |
| Shared warehouse (`spark.sql.warehouse.dir`) | ✅ | BS-020; must be the same mount on server and executors |
| Atomic writes (commit protocol, no partial files on failure) | ❌ | tasks write final files directly; step 05 |
| Driver-side writes (`BallistaSubplanExec`) | ⚠️ | single partition, untested under load (BS-013) |

## 5. Execution engine

| Feature | Spark | Status | Evidence / gap |
|---|---|---|---|
| Spilling sort, aggregate, sort-merge join, sort shuffle | yes | ✅ | memory-pressure runs (`compose.lowmem.yml`) |
| Hash join build side spills | yes | ✅ | BS-016 (DataFusion fork) |
| Hash join with one key larger than memory | sort-merge fallback | ❌ | fails after 4 spill levels (BS-018) |
| Shuffle data cleanup after a job | yes | ✅ | BS-023: intermediate stages at once, the rest after 300 s |
| Lineage recovery after executor loss | yes | ❔ | exists in Ballista; not tested from Connect (step 06) |
| Durable / remote shuffle | external shuffle service | ❌ | executor-local files only (step 06) |
| Speculation, fair pools, dynamic allocation | yes | ❌ | step 08 |
| Scheduler HA | driver per app | ❌ | in-memory state only (step 09) |

## 6. Adaptive query execution (Connect jobs run with AQE, BS-007)

| Spark AQE rule | Spark default | Status | Evidence / gap |
|---|---|---|---|
| Runtime join selection (shuffle join → broadcast) | on | ✅ | `SelectJoinRule`, `DelayJoinSelectionRule`, `DemoteUnsafeBroadcastJoinRule`; broadcasts as soon as the small side is measured (BS-025) |
| Empty relation propagation | on | ✅ | `PropagateEmptyExecRule`; BS-022 fixed the hang it exposed |
| Coalesce small shuffle partitions | on (`parallelismFirst`, 1 MB min) | ⚪ off on purpose | `CoalescePartitionsRule` exists (`ballista.planner.coalesce.enabled`). Measured 2026-11-01: on, TPC-DS 144.6 → 148.2 s, q51 1.16 → 2.62 s, no query faster. Spark coalesces to avoid many tiny tasks; Ballista already packs several partitions into one task, so coalescing only removes parallelism |
| Initial shuffle partition count from `spark.sql.shuffle.partitions` | 200 | ❌ | every exchange uses 28 (looks like DataFusion `target_partitions` = CPU count) |
| Split skewed join partitions | on (5× median and > 256 MB) | ✅ | `optimizer_rule/skew_join.rs`, Spark's `OptimizeSkewedJoin` (same thresholds, map-range splits, split sides per join type). Applies when the join's stage hash-shuffles its output; a join feeding a global aggregate or the result stays unsplit. `tests/connect_operations.py` (776 MB partition split, answer exact). Not triggered by TPC-H/TPC-DS SF10 |
| Local shuffle reader after switching to broadcast | on | ❌ | step 07 |
| Rebalance skew (`REBALANCE` hint) | on | ❌ | step 07 |
| Dynamic partition pruning | on (broadcast reuse only) | ✅ | BS-033: broadcast joins prune the probe scan by partition value, statistics, row group and page; per-task, executor-side. `tests/connect_operations.py` |

## 7. Operations and observability

| Feature | Status | Evidence / gap |
|---|---|---|
| Web UI (jobs, stages, executors) on :4040 | ✅ | step 11 |
| Per-stage input/output row counts | ⚠️ | stages whose task metrics don't line up show 0 rows (seen on TPC-DS q5 union stages) |
| History server UI, SQL operator metrics, auth | ❌ | step 11 |
| Authentication, TLS on Connect | ❌ | step 10 |

## 8. Next, in order
0. BS-034 — distributed file writes (in progress).
1. Iceberg beyond path tables: REST catalog, time travel, metadata tables, schema evolution.
2. AQE skew join: also split joins whose stage does not repartition (Spark validates the whole plan).
3. BS-031 — Delta `MERGE` loses rows intermittently (wrong answer).
4. BS-026 — build a broadcast hash table once per executor, not once per task (q23).
5. BS-028 — q72 at 71 s (Spark 17 s), deferred.
