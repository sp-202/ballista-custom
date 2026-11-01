# tpcbench.py (datafusion-benchmarks/runners/datafusion-comet) plus
# --results-dir: write each query's final result to <dir>/q<N>.csv so answers
# can be compared across engines. Timing logic is unchanged.

import argparse
import csv
import json
import os
import time
from datetime import datetime

from pyspark.sql import SparkSession


def cell(v):
    if v is None:
        return ""
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return str(v)


def main(args):
    spark = SparkSession.builder.appName(f"{args.engine} {args.benchmark}").getOrCreate()

    if args.benchmark == "tpch":
        num_queries = 22
        table_names = ["customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier"]
    else:
        num_queries = 99
        table_names = [
            "call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer",
            "customer_address", "customer_demographics", "date_dim", "household_demographics",
            "income_band", "inventory", "item", "promotion", "reason", "ship_mode", "store",
            "store_returns", "store_sales", "time_dim", "warehouse", "web_page", "web_returns",
            "web_sales", "web_site",
        ]
    for table in table_names:
        path = f"{args.data}/{table}.parquet"
        print(f"Registering table {table} using path {path}")
        spark.read.parquet(path).createOrReplaceTempView(table)

    # Spark Connect sessions (SPARK_REMOTE / --remote) have no SparkContext.
    is_connect = type(spark).__module__.startswith("pyspark.sql.connect")
    conf = {} if is_connect else {k: v for k, v in spark.sparkContext.getConf().getAll()}
    if is_connect:
        conf["spark.remote"] = os.environ.get("SPARK_REMOTE", "")
    results = {
        "engine": args.engine,
        "benchmark": args.benchmark,
        "data_path": args.data,
        "query_path": args.queries,
        "spark_conf": conf,
    }
    row_counts = {}

    if args.query_list:
        queries = [int(q) for q in args.query_list.split(",")]
    else:
        queries = [args.query] if args.query else range(1, num_queries + 1)
    for iteration in range(args.iterations):
        print(f"Starting iteration {iteration} of {args.iterations}")
        for query in queries:
            if not is_connect:
                spark.sparkContext.setJobDescription(f"{args.benchmark} q{query}")
            with open(f"{args.queries}/q{query}.sql") as f:
                text = f.read()
            start = time.time()
            written = False
            try:
                for sql in text.split(";"):
                    sql = sql.strip().replace("create view", "create temp view")
                    if not sql:
                        continue
                    df = spark.sql(sql)
                    rows = df.collect()
                    print(f"Query {query} returned {len(rows)} rows")
                    # Keep the query's answer: skip DDL and empty results. Engines
                    # differ in what DDL returns (Spark: no columns; Sail-based
                    # Spark Connect: one boolean `value` column).
                    # Leading `--` comment lines (TPC-H file headers) are not the statement.
                    code = "\n".join(l for l in sql.splitlines() if not l.lstrip().startswith("--"))
                    if not df.columns or code.lstrip().lower().startswith(("create ", "drop ")):
                        continue
                    row_counts[str(query)] = (row_counts.get(str(query), 0) if written else 0) + len(rows)
                    if args.results_dir:
                        # Queries with several SELECTs (TPC-DS 14, 23, 24, 39) append each
                        # result after the first, without another header.
                        os.makedirs(args.results_dir, exist_ok=True)
                        with open(f"{args.results_dir}/q{query}.csv", "a" if written else "w", newline="") as out:
                            w = csv.writer(out)
                            if not written:
                                w.writerow(df.columns)
                            for r in rows:
                                w.writerow([cell(v) for v in r])
                    written = True
            except Exception as e:
                # Record the failure and go on, so one query does not end the run.
                print(f"Query {query} failed: {str(e).splitlines()[0] if str(e) else type(e).__name__}")
                results.setdefault("failed", {})[str(query)] = str(e)[:2000]
                if args.results_dir and os.path.exists(f"{args.results_dir}/q{query}.csv"):
                    os.remove(f"{args.results_dir}/q{query}.csv")
                continue
            elapsed = time.time() - start
            print(f"Query {query} took {elapsed} seconds")
            results.setdefault(str(query), []).append(elapsed)

    results["row_counts"] = row_counts
    millis = int(datetime.now().timestamp() * 1000)
    path = f"{args.output}/{args.engine}-{args.benchmark}-{millis}.json"
    print(f"Writing results to {path}")
    with open(path, "w") as f:
        json.dump(results, f, indent=4)
    spark.stop()


if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--engine", required=True, help="label for the output, e.g. spark or spark-comet")
    p.add_argument("--benchmark", choices=["tpch", "tpcds"], default="tpch")
    p.add_argument("--data", required=True)
    p.add_argument("--queries", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--iterations", type=int, default=1)
    p.add_argument("--query", type=int)
    p.add_argument("--query-list", help="comma-separated query numbers, e.g. 5,12,16")
    p.add_argument("--results-dir")
    main(p.parse_args())
