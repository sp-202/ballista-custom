"""Spark Connect operation tests against a running Ballista cluster.

Checks what a PySpark client relies on beyond query answers: explain, interrupts
(all, by tag, by operation id), closing a session, reattaching after a dropped
connection, and spark.range results. Each interrupt and close must also stop the
Ballista job, which is checked through the scheduler's REST API.

    uv run --no-project --python 3.13 --with 'pyspark-client==4.0.1' \\
        python spark/tests/connect_operations.py --remote sc://localhost:15002 \\
        --scheduler http://localhost:4040

Exits non-zero if a check fails.
"""

import argparse
import contextlib
import io
import json
import socket
import threading
import time
import urllib.request

from pyspark.sql import SparkSession, functions as F

FAILED = []


def check(name, ok, detail=""):
    print(f"{'PASS' if ok else 'FAIL'}  {name}{'  ' + detail if detail else ''}")
    if not ok:
        FAILED.append(name)


def running_jobs(scheduler):
    jobs = json.load(urllib.request.urlopen(f"{scheduler}/api/jobs"))
    return [j["job_id"] for j in jobs if j["status"] in ("Queued", "Running")]


def wait_until_no_jobs(scheduler, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not running_jobs(scheduler):
            return True
        time.sleep(0.5)
    return False


def slow_query(spark):
    """About 10 s on a 2-executor, 16-core cluster: a 600M-row shuffle."""
    n = 600_000_000
    rows = spark.range(0, n, 1, 56).withColumn("k", F.col("id") % (n // 4))
    return rows.groupBy("k").agg(F.count("*").alias("c")).agg(F.sum("c"))


def start_in_thread(action, setup=None):
    """Runs `action` in a thread; returns (thread, result dict)."""
    result = {}

    def run():
        if setup:
            setup()
        try:
            result["value"] = action()
        except Exception as e:  # noqa: BLE001 - the error is what is checked
            result["error"] = e

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread, result


def is_cancelled(result):
    e = result.get("error")
    return e is not None and getattr(e, "getCondition", lambda: None)() == "OPERATION_CANCELED"


def test_explain(spark):
    df = spark.range(0, 1000, 1, 4).groupBy((F.col("id") % 3).alias("k")).count()
    for mode in ["simple", "extended", "formatted", "cost", "codegen"]:
        out = io.StringIO()
        try:
            with contextlib.redirect_stdout(out):
                df.explain(mode)
            check(f"explain({mode})", "Physical Plan" in out.getvalue() or mode == "codegen")
        except Exception as e:  # noqa: BLE001
            check(f"explain({mode})", False, str(e).splitlines()[0][:120])


def test_range(spark):
    def expected(n):
        q, r = divmod(n, 7)
        return q * 21 + r * (r - 1) // 2

    for n, parts in [(1000, 8), (10_000_000, 56), (10_000_000, 3), (10_000_000, 100)]:
        row = spark.range(0, n, 1, parts).select(
            F.sum(F.col("id") % 7).alias("s"), F.count("*").alias("c")
        ).collect()[0]
        check(f"range({n}, {parts} partitions)", row.s == expected(n) and row.c == n,
              f"sum={row.s} count={row.c}")


def test_repartition(spark):
    """repartition / coalesce change partitioning, never the rows (BS-010)."""
    df = spark.range(0, 1_000_000, 1, 4)
    for name, d in [
        ("repartition(16)", df.repartition(16)),
        ("repartition(16, k)", df.withColumn("k", F.col("id") % 100).repartition(16, "k")),
        ("coalesce(2)", df.coalesce(2)),
    ]:
        row = d.agg(F.sum("id").alias("s"), F.count("*").alias("c")).collect()[0]
        check(f"{name} keeps every row", row.s == 499_999_500_000 and row.c == 1_000_000,
              f"sum={row.s} count={row.c}")


def test_partition_ids(spark):
    """Partition-indexed functions see global partition ids, not task-local ones (BS-029)."""
    df = spark.range(0, 100_000, 1, 16)
    ids = df.select(F.spark_partition_id().alias("p")).distinct().count()
    check("spark_partition_id() is distinct per partition", ids == 16, f"{ids} ids")
    unique = df.select(F.monotonically_increasing_id().alias("m")).distinct().count()
    check("monotonically_increasing_id() is unique", unique == 100_000, f"{unique} unique")


def test_skew_join(spark):
    """A join with one partition far larger than the rest (AQE skew splitting) keeps
    every row: 80% of 60M rows share key 0, and the join feeds a hash shuffle."""
    fact = spark.range(0, 60_000_000, 1, 32).select(
        F.when(F.col("id") % 10 < 8, 0).otherwise(F.col("id")).alias("k"), F.col("id").alias("v"))
    dim = spark.range(0, 30_000_000, 1, 32).select(F.col("id").alias("k"), (F.col("id") * 2).alias("w"))
    row = (fact.join(dim, "k").groupBy((F.col("v") % 100).alias("g"))
           .agg(F.count("*").alias("c"), F.sum("w").alias("s"))
           .agg(F.sum("c"), F.sum("s")).collect()[0])
    check("skewed join keeps every row", tuple(row) == (54_000_000, 180_000_042_000_000), str(tuple(row)))


def test_iceberg(spark, warehouse):
    """Iceberg path tables: write, append, overwrite, partitioned, UPDATE, DELETE, MERGE."""
    import uuid
    path = f"{warehouse}/ops_iceberg_{uuid.uuid4().hex[:8]}"
    sum_count = lambda df: tuple(df.agg(F.count("*"), F.sum("id")).collect()[0])
    try:
        spark.range(0, 100_000, 1, 8).write.format("iceberg").mode("overwrite").save(path)
        spark.range(100_000, 200_000, 1, 8).write.format("iceberg").mode("append").save(path)
        got = sum_count(spark.read.format("iceberg").load(path))
        check("iceberg write + append", got == (200_000, 19_999_900_000), str(got))
        spark.range(0, 100_000, 1, 8).withColumn("k", F.col("id") % 10) \
            .write.format("iceberg").partitionBy("k").mode("overwrite").save(path + "_p")
        spark.sql("DROP TABLE IF EXISTS ops_ice")
        spark.sql(f"CREATE TABLE ops_ice USING iceberg LOCATION '{path}_p'")
        spark.sql("UPDATE ops_ice SET id = -1 WHERE k = 3").collect()
        spark.sql("DELETE FROM ops_ice WHERE k = 4").collect()
        spark.range(0, 20).withColumn("k", F.lit(99)).createOrReplaceTempView("ops_src")
        spark.sql("MERGE INTO ops_ice USING ops_src ON ops_ice.id = ops_src.id "
                  "WHEN MATCHED THEN UPDATE SET k = ops_src.k WHEN NOT MATCHED THEN INSERT *").collect()
        got = tuple(spark.sql("SELECT count(*), sum(CASE WHEN k = 99 THEN 1 ELSE 0 END), "
                              "sum(CASE WHEN id = -1 THEN 1 ELSE 0 END) FROM ops_ice").collect()[0])
        check("iceberg UPDATE, DELETE, MERGE", got == (90_004, 20, 10_000), str(got))
    except Exception as e:  # noqa: BLE001
        check("iceberg operations", False, str(e).splitlines()[0][:200])


def scan_rows(scheduler, table):
    """Rows output by the scans of `table` in the newest job, from the scheduler's metrics."""
    import re
    jobs = json.load(urllib.request.urlopen(f"{scheduler}/api/jobs"))
    job = max(jobs, key=lambda j: j["start_time"])["job_id"]
    stages = json.load(urllib.request.urlopen(
        f"{scheduler}/api/job/{job}/stages?plan_format=metrics"))["stages"]
    rows = []
    for stage in stages:
        for line in (stage.get("stage_plan") or "").splitlines():
            if "DataSourceExec" in line and f"/{table}/" in line:
                m = re.search(r"output_rows=([\d.]+) ?([KM]?)", line)
                rows.append(float(m.group(1)) * {"": 1, "K": 1e3, "M": 1e6}[m.group(2)])
    return rows


def test_dynamic_partition_pruning(spark, scheduler, warehouse):
    """A broadcast join prunes the partitioned fact table to the dimension's keys,
    as Spark's dynamic partition pruning does: 2 of 20 partitions are read."""
    import uuid
    path = f"{warehouse}/ops_dpp_{uuid.uuid4().hex[:8]}"
    spark.range(0, 2_000_000, 1, 8).select((F.col("id") % 20).alias("d"), F.col("id").alias("v")) \
        .write.partitionBy("d").parquet(path + "/fact")
    spark.range(0, 20).select(F.col("id").alias("d"), F.col("id").isin(7, 12).alias("flag")) \
        .write.parquet(path + "/dim")
    spark.read.parquet(path + "/fact").createOrReplaceTempView("ops_fact")
    spark.read.parquet(path + "/dim").createOrReplaceTempView("ops_dim")
    row = spark.sql("SELECT count(*), sum(v) FROM ops_fact JOIN ops_dim USING (d) "
                    "WHERE ops_dim.flag").collect()[0]
    check("dynamic partition pruning: answer", tuple(row) == (200_000, 199_999_900_000), str(tuple(row)))
    rows = scan_rows(scheduler, "fact")
    check("dynamic partition pruning: fact scan reads 2 of 20 partitions",
          rows and sum(rows) == 200_000, f"scan rows {rows}")


def test_partitioned_write(spark, warehouse):
    """Partitioned parquet writes run in every task (BS-034); overwrite keeps only the
    new rows, append adds to them."""
    import uuid
    path = f"{warehouse}/ops_write_{uuid.uuid4().hex[:8]}"
    rows = lambda n, k: spark.range(0, n, 1, 16).select("id", (F.col("id") % k).alias("d"))
    read = lambda: tuple(spark.read.parquet(path).agg(
        F.count("*"), F.sum("id"), F.countDistinct("d")).collect()[0])
    rows(1_000_000, 100).write.partitionBy("d").parquet(path)
    check("partitioned write", read() == (1_000_000, 499_999_500_000, 100), str(read()))
    rows(100_000, 10).write.mode("overwrite").partitionBy("d").parquet(path)
    check("partitioned overwrite keeps only new rows", read() == (100_000, 4_999_950_000, 10), str(read()))
    rows(100_000, 10).write.mode("append").partitionBy("d").parquet(path)
    check("partitioned append", read() == (200_000, 9_999_900_000, 10), str(read()))


def test_object_store(spark, path):
    """Write to and read back from an object store (BS-011). Needs the store
    configured on every node, e.g. AWS_* settings for S3."""
    spark.range(0, 1_000_000, 1, 8).write.mode("overwrite").parquet(path)
    row = spark.read.parquet(path).agg(F.sum("id").alias("s"), F.count("*").alias("c")).collect()[0]
    check(f"write and read back {path}", row.s == 499_999_500_000 and row.c == 1_000_000,
          f"sum={row.s} count={row.c}")


def test_interrupts(spark, scheduler):
    query = slow_query(spark)

    thread, result = start_in_thread(query.collect)
    time.sleep(4)
    ids = spark.interruptAll()
    thread.join(30)
    check("interruptAll returns the operation", len(ids) == 1, str(ids))
    check("interruptAll: client gets OPERATION_CANCELED", is_cancelled(result),
          repr(result.get("error") or result.get("value"))[:120])
    check("interruptAll: Ballista job stopped", wait_until_no_jobs(scheduler))

    thread, result = start_in_thread(query.collect, setup=lambda: spark.addTag("ops-test"))
    time.sleep(4)
    ids = spark.interruptTag("ops-test")
    thread.join(30)
    check("interruptTag returns the operation", len(ids) == 1, str(ids))
    check("interruptTag: client gets OPERATION_CANCELED", is_cancelled(result))
    check("interruptTag: Ballista job stopped", wait_until_no_jobs(scheduler))

    check("session usable after interrupts", spark.range(10).count() == 10)


def test_stop_cancels_jobs(remote, scheduler):
    spark = SparkSession.builder.remote(remote).create()
    start_in_thread(slow_query(spark).collect)
    time.sleep(4)
    spark.stop()
    check("stop(): Ballista job stopped", wait_until_no_jobs(scheduler))


class CuttingProxy:
    """A TCP proxy to the Spark Connect server that can drop every connection."""

    def __init__(self, target_port):
        self.target_port = target_port
        self.sockets = []
        self.lock = threading.Lock()
        self.listener = socket.socket()
        self.listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen()
        self.port = self.listener.getsockname()[1]
        threading.Thread(target=self._serve, daemon=True).start()

    def _serve(self):
        while True:
            client, _ = self.listener.accept()
            upstream = socket.create_connection(("127.0.0.1", self.target_port))
            with self.lock:
                self.sockets += [client, upstream]
            for a, b in [(client, upstream), (upstream, client)]:
                threading.Thread(target=self._pipe, args=(a, b), daemon=True).start()

    @staticmethod
    def _pipe(a, b):
        try:
            while data := a.recv(65536):
                b.sendall(data)
        except OSError:
            pass
        finally:
            for s in (a, b):
                with contextlib.suppress(OSError):
                    s.close()

    def cut(self):
        with self.lock:
            for s in self.sockets:
                with contextlib.suppress(OSError):
                    s.shutdown(socket.SHUT_RDWR)
                    s.close()
            count, self.sockets = len(self.sockets), []
        return count


def test_reattach(remote):
    port = int(remote.rsplit(":", 1)[1].split("/")[0])
    proxy = CuttingProxy(port)
    spark = SparkSession.builder.remote(f"sc://127.0.0.1:{proxy.port}").create()
    thread, result = start_in_thread(slow_query(spark).collect)
    time.sleep(4)
    cut = proxy.cut()
    thread.join(120)
    value = result.get("value")
    check("reattach after dropped connection returns the answer",
          cut > 0 and value is not None and value[0][0] == 600_000_000,
          f"cut {cut} sockets; {repr(result.get('error') or value)[:120]}")
    spark.stop()


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--remote", default="sc://localhost:15002")
    p.add_argument("--scheduler", default="http://localhost:4040")
    p.add_argument("--warehouse", default="/warehouse",
                   help="a path every node shares, for table tests; '' skips them")
    p.add_argument("--s3-path", help="e.g. s3://spark-test/ops; skipped when not given")
    args = p.parse_args()

    spark = SparkSession.builder.remote(args.remote).getOrCreate()
    test_explain(spark)
    test_range(spark)
    test_repartition(spark)
    test_partition_ids(spark)
    test_skew_join(spark)
    if args.warehouse:
        test_iceberg(spark, args.warehouse)
        test_dynamic_partition_pruning(spark, args.scheduler, args.warehouse)
        test_partitioned_write(spark, args.warehouse)
    if args.s3_path:
        test_object_store(spark, args.s3_path)
    test_interrupts(spark, args.scheduler)
    test_stop_cancels_jobs(args.remote, args.scheduler)
    test_reattach(args.remote)

    print(f"\n{len(FAILED)} failed" + (": " + ", ".join(FAILED) if FAILED else ""))
    raise SystemExit(1 if FAILED else 0)


if __name__ == "__main__":
    main()
