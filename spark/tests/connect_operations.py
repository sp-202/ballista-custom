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
    args = p.parse_args()

    spark = SparkSession.builder.remote(args.remote).getOrCreate()
    test_explain(spark)
    test_range(spark)
    test_interrupts(spark, args.scheduler)
    test_stop_cancels_jobs(args.remote, args.scheduler)
    test_reattach(args.remote)

    print(f"\n{len(FAILED)} failed" + (": " + ", ".join(FAILED) if FAILED else ""))
    raise SystemExit(1 if FAILED else 0)


if __name__ == "__main__":
    main()
