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
| [BS-012](#bs-012) | 🔴 | S3 | Closing a Spark session doesn't cancel its Ballista jobs | 02 |
| [BS-013](#bs-013) | 🔴 | S2 | `BallistaSubplanExec` is single-partition; untested under driver-side writes | 05 |
| [BS-014](#bs-014) | 🔴 | S3 | Redundant shuffles from the front end's repartitions | 03 |
| [BS-015](#bs-015) | 🔴 | S3 | Uneven file-partition assignment makes one scan task a straggler | 07 |
| [BS-016](#bs-016) | 🔴 | S2 | Hash join build side cannot spill (DataFusion 55) | 06 |

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
**Closing a Spark session doesn't cancel its Ballista jobs** · 🔴 open · step 02
- **Cause:** `BallistaJobRunner::stop` only logs.
- **Plan:** track job ids per session; `CancelJob` on stop and on `spark.interruptAll()`.

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
**Hash join build side cannot spill (DataFusion 55)** · 🔴 open · step 06
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
