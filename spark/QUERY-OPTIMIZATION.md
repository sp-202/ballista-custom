# Query optimization: Spark vs Ballista, and scan pushdown

Status as of 2026-11-11. This covers which optimizer rules run in Spark and in Ballista (Sail front end),
whether projection and filter pushdown reach the Parquet scan, what was measured, and what to change next.
Issue IDs refer to [ISSUES.md](ISSUES.md).

## How a query is optimized in Ballista

1. **Sail logical optimizer:** DataFusion's default rules plus Sail's own, minus `push_down_leaf_projections`
   (`crates/sail-session/src/optimizer.rs`).
2. **Sail physical optimizer** (`crates/sail-physical-optimizer`):
   - DataFusion's rules
   - `OffsetSafeRequirements` in place of EnsureRequirements
   - `ContextSafeProjectionPushdown`
   - `FilterPushdown` and `PostFilterPushdown`
3. **Submission:** `BallistaJobRunner` sends the physical plan to the scheduler. Scans are wrapped in `CodecScanExec`
   and encoded by the Sail codec. The Parquet options and the scan predicate survive encoding.
4. **Ballista scheduler, once per job** (`plan_preparation_optimizers`):
   - `FilterPushdown`: Ballista's own copy in `ballista/scheduler/src/physical_optimizer/filter_pushdown.rs`
   - `BalanceAndPruneFileGroups`
   - `DelayJoinSelectionRule`
5. **Ballista scheduler, per stage (AQE):** in order:
   - `NormalizeInterleave`, `PropagateEmpty` and `SelectJoinRule` (broadcast or partitioned, from runtime sizes).
   - DataFusion's default physical rules without `FilterPushdown`. Join selection is pinned
     (`PinBroadcastJoins`) and followed by `DemoteUnsafeBroadcastJoinRule`.
   - The window rules, then `DistributedExchangeRule`.
6. **Executor:** after decoding a task plan, `dynamic_pruning::push_join_filters` (BS-033) reruns DataFusion's
   post-optimization filter pushdown for broadcast (`CollectLeft`) joins. The probe-side scan then prunes with the
   build side's keys.

## Rule-by-rule comparison (Spark 3.5.8 defaults)

| Area | Spark 3.5.8 | Ballista today | Verdict |
|---|---|---|---|
| Column pruning to the scan | `ColumnPruning`, on | DataFusion projection pushdown, works | Same |
| Nested field pruning | `spark.sql.optimizer.nestedSchemaPruning.enabled`, on | **Off**: Sail removes `push_down_leaf_projections` | Gap |
| Filter pushdown to Parquet (row groups, column index, bloom) | `spark.sql.parquet.filterPushdown`, on | Pruning predicate: row-group stats, page index, bloom filters, works | Same |
| Row-level filtering inside the reader | Off (`recordLevelFilter.enabled=false`) | Off (`parquet.pushdown_filters=false`) | Same; see measurements |
| Filters inferred through join keys | `InferFiltersFromConstraints`, on | Works (`o_orderkey < 10000` also prunes `l_orderkey`) | Same |
| Hive partition pruning | on | Works | Same |
| Dynamic partition pruning | `dynamicPartitionPruning`, on, reuses broadcasts only | BS-033: broadcast joins only, inside each task | Same scope |
| Runtime bloom filters for shuffle joins | `InjectRuntimeFilter` (`runtime.bloomFilter.enabled`), on | **None** | Gap |
| TopK filters (`ORDER BY .. LIMIT`) | None | DataFusion has them, but Ballista turns them off | Beyond Spark, not used yet |
| Cost-based join reorder | `cbo` and `joinReorder`, off | Sail `optimizer.enable_join_reorder`, off | Same |
| AQE: broadcast switch, coalescing | on | `SelectJoinRule`, coalescing | Same |
| AQE: skew join, local shuffle read | on | None (roadmap step 07) | Gap |

## What was verified on kind (SF1, stage plans with metrics from `/api/job/{id}/stages?plan_format=metrics`)

Query texts are in "Reproducing" below.

- **Projection pushdown works.**
  - q6 reads only its 4 columns: 59 MB of lineitem.
- **Filter pushdown into the scan works for pruning:**
  - row-group statistics, e.g. `joinkey` reads 1 of 64 row groups
  - page index
  - bloom filters
  - Hive partition values
- **Transitive filters work.** A filter on `o_orderkey` reaches the `lineitem` scan through the join key.
- **AQE picks broadcast joins at runtime.**
- **Gaps:**
  - **Rows are not filtered inside the reader.** With `pushdown_filters=false`, q6 decodes all 6.0 M rows and a
    `FilterExec` drops them afterwards.
  - **The scan predicate is duplicated** (`predicate=X AND X`). Sail's physical `FilterPushdown` and Ballista's
    preparation `FilterPushdown` both push the same filter. It is used only for pruning, so the cost is small.
  - **BS-033 broadcast-join filters prune nothing when the join key is not sorted or clustered.** They act only
    through statistics.
  - **TopK has no filter.** `ORDER BY l_extendedprice DESC LIMIT 10` scans all 6.0 M rows.
  - **No nested pruning.** `sum(s.price)` over a struct column reads the whole struct, including the long
    `comment` field: 97 MB.

## Measured: Parquet row-level filtering (`SAIL_PARQUET__PUSHDOWN_FILTERS=true`)

**Effect on rows (SF1, kind):**

| Query | Rows out of the scan, off → on |
|---|---|
| q6 | 6.00 M → 114 K (the `FilterExec` and the duplicated predicate disappear) |
| `dim` (selective `part`, broadcast join) | 6.00 M → 6.09 K |
| `joinkey` | 20.1 K → 9.96 K |

**Effect on time (SF100, podman cluster, 200 partitions, 2026-11-09):** one run per setting, in sequence, not ABBA.

| Query | off (s) | on (s) |
|---|---|---|
| q6 | 3.2 | 5.2 |
| q19 | 7.0 | 7.5 |
| q7 | 19.0 | 19.9 |
| q18 | 28.7 | 29.3 |
| q21 | 40.4 | 43.5 |
| q1 | 7.3 | 7.4 |
| q17 | 15.9 | 18.6 |
| q9 | 15.7 | 24.4 |
| **Total** | **137.2** | **155.9** |

In seven other runs that day with filtering off (at different partition counts), q6 took 3.0–3.8 s, so its
slowdown is not noise. q9 varied more: 13.8–19.0 s.

**Conclusion: keep `pushdown_filters` off by default, as Spark does.** Filtering inside the reader decodes fewer rows,
but it was slower end to end. Decoding whole columns and then filtering a batch at a time (SIMD) beats selecting
scattered rows during decoding when rows are 2–25% selective. DataFusion 55 has no adaptive switch between the two.
A default flip was tried on branch `perf/scan-pushdown` and reverted.

## DataFusion 55 facts that shape the plan

- **The Parquet opener rechecks dynamic filters before every row group** (`RowGroupPruner`), whether or not
  `pushdown_filters` is on. It also has a per-file `FilePruner` and an `EarlyStoppingStream` that ends a file's scan
  once the filter proves the rest can be skipped. Metric: `row_groups_pruned_dynamic_filter`.
  So TopK and join filters can skip whole row groups without row-level filtering.
- **TopK filters are shared by all partitions of one `SortExec` and are never waited on.** Only tests call
  `wait_complete`. A Ballista task that runs only some of the partitions therefore cannot hang on one. It cannot
  return wrong rows either: every partition's k-th value bounds the global top k.
- **`datasource-parquet/src/nested_schema_pruning.rs`** clips nested columns when the scan casts them to a
  narrower type. Leaf pruning for `get_field` needs the logical rule `push_down_leaf_projections`. Sail dropped
  that rule when it upgraded to DataFusion 53 (upstream Sail commit `2d7f1488c`) and gave no reason.

## Plan

| # | Change | Size | Expected gain | State |
|---|---|---|---|---|
| P1 | Turn row-level Parquet filtering on by default | config | Negative at SF100 | **Dropped** |
| P2 | TopK filters on executors: `push_topk_filters` in `ballista/executor/src/dynamic_pruning.rs` before the join rewrite | small | `ORDER BY .. LIMIT` on sorted or clustered data skips row groups and files (e.g. "latest N rows") | Code and 3 unit tests on branch `perf/scan-pushdown` (`436458a`), not compiled or run yet |
| P3 | Nested field pruning: re-enable `push_down_leaf_projections` | small code, high risk | Struct-heavy tables read only the fields they use | Needs Sail's gold tests and a differential test against vanilla Spark |
| P4 | Remove the duplicated scan predicate | small | Small: pruning-time CPU only | Low priority |
| P5 | Runtime filters for shuffle joins, Spark's `InjectRuntimeFilter`: build a bloom filter and min/max of the join keys from the build-side stage, then apply it to the probe-side scan stage before its shuffle | large: AQE plus shuffle writer | TPC-H q18, q21 and q7 are shuffle-bound at SF100. This is where most of the remaining time is | Design not started (roadmap step 07, item "cross-stage runtime filters") |
| P6 | Adaptive late materialization: row-level filtering only when a sample shows the filter is selective | large: DataFusion fork | Gets P1's row savings without its slowdown | Idea |

## Next steps

1. **Test P2.** Build and run `cargo test -p ballista-executor --lib dynamic_pruning`. Then deploy to kind and time
   SF10 with P2 off and on, in ABBA order:
   - TPC-H q1, q3, q6, q12, q14, q17 and q19
   - three TopK probes on `lineitem`: `order by l_orderkey limit 10` (sorted), `order by l_orderkey desc limit 10`
     and `order by l_extendedprice desc limit 10` (unsorted)
2. **Run the conformance suite** with P2 (TPC-H 22 + TPC-DS 99 against vanilla Spark).
3. **Design P5** against Ballista's AQE stage ordering. The probe-side map stage has to wait for the build side.
   Spark accepts the same trade.

## Reproducing

```
SAIL_PARQUET__PUSHDOWN_FILTERS=true|false   # connect server env, A/B toggle
SAIL_PARQUET__REORDER_FILTERS=true|false
GET /api/job/{id}/stages?plan_format=metrics # scheduler REST: stage plans with scan metrics
```

Queries used on SF1:

| Name | Query |
|---|---|
| `joinkey` | `select count(*) from lineitem join orders on l_orderkey = o_orderkey where o_orderkey < 10000` |
| `dim` | `select sum(l_extendedprice) from lineitem join part on l_partkey = p_partkey where p_brand = 'Brand#23' and p_container = 'MED BOX'` |
| `topk` | `select l_orderkey, l_extendedprice from lineitem order by l_extendedprice desc limit 10` |
| `struct` | `select sum(s.price) from pd_nested where s.k < 1000`, where `pd_nested` holds `named_struct('k', l_orderkey, 'price', l_extendedprice, 'comment', l_comment)` |
