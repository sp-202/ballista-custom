# Code copied from LakeSail Sail

The `crates/sail-*` crates in this workspace are copied from
[LakeSail Sail](https://github.com/lakehq/sail), licensed under the Apache
License 2.0 (see `LICENSE-SAIL`). They provide the Spark-facing half of Spark
support for Ballista: the Spark Connect protocol, the Spark SQL parser and
analyzer, plan resolution, Spark functions, catalogs, Delta Lake and Iceberg,
and PySpark UDFs. Ballista's own crates (`../ballista/*`) execute the plans.

- **Source:** `https://github.com/lakehq/sail`, commit
  `35d4d2f2d58890567c2a7b56372b12c9a4092c57` (2026-10-31, version 0.7.3)
- **Copied:** the 34 crates needed by `sail-spark-connect` and
  `sail-execution` (the plan codec), unmodified except as listed below.
- **Not copied:** `sail-cli`, `sail-flight`, `sail-gold-test`, `sail-mimalloc`,
  `sail-python`.
- `Cargo.lock` started from Sail's lockfile at the same commit, so the copied
  code builds against the dependency versions it was tested with.

## Local modifications

Every change to copied code is listed here. Keep this list current; it is what
has to be re-applied when updating from upstream.

| File | Change | Why |
|---|---|---|
| `sail-execution/src/lib.rs` | Re-export `RemoteExecutionCodec` | Ballista's scheduler and executors decode Spark plans with it (through `ballista-spark-codec`) |
| `sail-execution/src/lib.rs`, `job_graph/mod.rs`, `job_graph/planner.rs` | Make `is_driver_stage_plan` public and re-export it | `ballista-spark-runner` keeps driver-only operators (catalog commands, commits, ...) in the Spark Connect server, using Sail's own list |
| `sail-session/src/session_factory/job_runner.rs` | Public `SessionJobRunner::new(Box<dyn JobRunner>)` | Lets a Ballista job runner be plugged in |
| `sail-session/src/session_manager/mod.rs` | `create_session_manager_with_job_runner_factory`; `create_session_manager` delegates to it | Same |
| `sail-spark-connect/src/session_manager.rs` | `create_spark_session_manager_with_factories` | Same |
| `sail-spark-connect/src/session.rs`, `service/plan_executor.rs`, `error.rs` | Remember interrupted operation ids; reattaching to one fails with `[OPERATION_CANCELED]` (`SparkError::OperationCanceled`); errors whose message starts with `[ERROR_CLASS]` carry it as `errorClass` | PySpark then raises `SparkException` with `getCondition() == "OPERATION_CANCELED"` after an interrupt, as with Spark, instead of "operation not found" |
| `sail-execution/src/proto/codec.rs` (+ `driver/job_scheduler/mod.rs`, `task_runner/preparation.rs`) | `RemoteExecutionCodec` is a struct with an optional weak outer codec (`with_nested_codec`, `new()`); every nested plan inside a Sail node is encoded and decoded with it | Sail nodes can hold Ballista nodes (shuffle readers, `SelectPartitionsExec`), which only Ballista's composite codec can encode (BS-030) |
| `sail-spark-connect/src/entrypoint.rs` | `serve_with_factories`; shared `serve_session_manager` used by both serve functions | Same |
| `sail-common-datafusion/src/error.rs` | Handle `DataFusionError::SQL` | Ballista enables DataFusion's `sql` feature, which adds this variant |
| `sail-plan/src/resolver/expression/wildcard.rs` | Build wildcard options with `sqlparser` AST types | Under the `sql` feature DataFusion uses `sqlparser` types instead of `datafusion_expr::sql`; the rename/replace items are display-only (DataFusion evaluates `planned_expressions`), so results are unchanged |

| `sail-data-source/src/url.rs`, `listing/source.rs`, `Cargo.toml` | `resolve_listing_urls(.., check_exists)`: direct reads fail with `[PATH_NOT_FOUND]` when a path does not exist or a glob matches nothing; catalog-table reads skip the check (test dev-dependency `tempfile`) | Upstream treats a missing path as an empty directory, so `spark.read.parquet("/missing")` returned 0 rows instead of Spark's error (BS-017) |
| `sail-spark-connect/src/session_manager.rs` | `SparkSessionMutator::with_default_conf` | Server-side Spark settings for every session (`ballista-connect --conf`), e.g. a shared `spark.sql.warehouse.dir` (BS-020) |
| `sail-catalog/src/command.rs`, `Cargo.toml` | `DropTable` deletes a managed table's data (local directory or object-store prefix) after removing the catalog entry; external tables keep theirs (deps `futures`, `object_store`) | Spark's `DropTableCommand` deletes managed table data; upstream left the files behind (BS-021) |
Workspace-level: `Cargo.toml` is derived from Sail's root manifest (same
dependency versions, lints and release profile) with an explicit member list,
the Ballista integration crates, DataFusion's `sql` feature declared, and a
`dev-opt` profile for local builds.

## Known gaps (not yet handled)

- DDL such as `DROP VIEW` returns one row `value: true`; Spark returns an
  empty result with no columns.
