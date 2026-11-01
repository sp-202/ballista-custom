# Conformance against Spark

Checks that Spark jobs run on Ballista (through `ballista-connect`) return the same
answers as vanilla Spark. Roadmap step 01.

| File | Purpose |
|---|---|
| `spark_tpcbench.py` | Runs TPC-H or TPC-DS through any Spark session (Spark itself, or Spark Connect with `SPARK_REMOTE`) and saves each query's answer as CSV |
| `compare_answers.py` | Compares answer CSVs with a reference engine's |
| `nightly.py` | Runs both suites on Ballista, compares with Spark, writes a dated report |
| `queries/tpcds/` | TPC-DS queries 1–99, with two rewrites Spark needs: `+ 30 days` → `+ interval 30 days`, `"quoted"` aliases → backticks |

## Reference answers

Produced once with vanilla Spark 3.5.8, Comet and other plugins off:

```sh
spark-submit --conf spark.plugins= --conf spark.comet.enabled=false \
  --conf spark.sql.extensions= spark_tpcbench.py --engine spark --benchmark tpcds \
  --data /mnt/data/tpcds/sf10-parquet --queries queries/tpcds \
  --output /tmp/spark-timings --results-dir ~/Documents/tpch/answers/tpcds/spark
```

TPC-H answers go to `~/Documents/tpch/answers/spark` the same way, with
`--benchmark tpch` and the TPC-H queries from `datafusion-benchmarks`.

## Nightly run

With the cluster up (`~/Documents/ballista/cluster/spark`):

```sh
uv run --no-project --python 3.13 --with 'pyspark-client==4.0.1' \
  python spark/conformance/nightly.py
```

Reports go to `~/Documents/ballista/conformance-reports/` (`latest.md` is the newest).
Each lists the pass count per suite, every difference from Spark, what changed since
the previous run (queries now wrong, now right, or more than 1.5× slower), and the
slowest queries. The exit code is 1 if any query fails or differs.

Answers match when every cell is equal, or numerically within relative 1e-9 or
absolute 1e-4; rows may come back in another order only where `ORDER BY` has ties.
