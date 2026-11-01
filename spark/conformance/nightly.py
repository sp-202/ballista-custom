"""Nightly conformance run: TPC-H and TPC-DS through Spark Connect, compared with Spark.

Runs each suite against a running Ballista cluster (`ballista-connect`), compares
every answer with vanilla Spark's (Comet off) and writes a dated report next to the
previous ones:

    <reports>/<date>.md      what a person reads: pass rates, every difference,
                             changes since the previous run, slowest queries
    <reports>/<date>.json    the same, for the next run to compare against
    <reports>/latest.md      a copy of the newest report

    uv run --no-project --python 3.13 --with 'pyspark-client==4.0.1' \\
        python spark/conformance/nightly.py

Exits 1 if a query fails or differs from Spark. Spark's answers are produced once
with `spark_tpcbench.py --engine spark` and kept under `--answers`.
"""

import argparse
import datetime
import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

from compare_answers import REL_TOL, ABS_TOL, compare_query

HERE = Path(__file__).resolve().parent
HOME = Path.home()
SLOWER = 1.5  # a query this many times slower than last run is reported...
SLOWER_MIN_SECONDS = 1.0  # ...when it also takes at least this long


def suites(args):
    return [
        {
            "name": "tpch",
            "title": "TPC-H SF10",
            "data": args.tpch_data,
            "queries": args.tpch_queries,
            "reference": Path(args.answers) / "spark",
            "answers": Path(args.answers),
        },
        {
            "name": "tpcds",
            "title": "TPC-DS SF10",
            "data": args.tpcds_data,
            "queries": str(HERE / "queries" / "tpcds"),
            "reference": Path(args.answers) / "tpcds" / "spark",
            "answers": Path(args.answers) / "tpcds",
        },
    ]


def run_suite(suite, label, args, log_path):
    """Runs one suite; returns {query: seconds} and {query: error} from the log."""
    results_dir = suite["answers"] / label
    shutil.rmtree(results_dir, ignore_errors=True)
    out_dir = Path(args.reports) / "timings"
    out_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable, "-u", str(HERE / "spark_tpcbench.py"),
        "--engine", label, "--benchmark", suite["name"],
        "--data", suite["data"], "--queries", suite["queries"],
        "--output", str(out_dir), "--results-dir", str(results_dir),
    ]
    env = dict(os.environ, SPARK_REMOTE=args.remote)
    with open(log_path, "w") as log:
        subprocess.run(cmd, stdout=log, stderr=subprocess.STDOUT, env=env,
                       timeout=args.timeout, check=False)
    text = Path(log_path).read_text()
    times = {int(q): float(t) for q, t in re.findall(r"^Query (\d+) took ([\d.]+)", text, re.M)}
    errors = {int(q): e for q, e in re.findall(r"^Query (\d+) failed: (.*)$", text, re.M)}
    return times, errors


def check_suite(suite, label, times, errors):
    queries = sorted(int(p.stem[1:]) for p in suite["reference"].glob("q*.csv"))
    rows = []
    for q in queries:
        if q in errors:
            ok, what = False, f"FAILED: {errors[q][:300]}"
        else:
            ok, what = compare_query(suite["reference"] / f"q{q}.csv",
                                     suite["answers"] / label / f"q{q}.csv")
        rows.append({"query": q, "ok": ok, "result": what, "seconds": times.get(q)})
    return rows


def previous_report(reports, today):
    older = sorted(p for p in Path(reports).glob("*.json") if p.stem < today)
    return json.loads(older[-1].read_text()) if older else None


def changes(suite_name, rows, previous):
    if not previous or suite_name not in previous["suites"]:
        return [], [], []
    before = {r["query"]: r for r in previous["suites"][suite_name]["queries"]}
    broke = [r for r in rows if not r["ok"] and before.get(r["query"], {}).get("ok")]
    fixed = [r for r in rows if r["ok"] and r["query"] in before and not before[r["query"]]["ok"]]
    slower = []
    for r in rows:
        old = before.get(r["query"], {}).get("seconds")
        if r["seconds"] and old and r["seconds"] >= SLOWER_MIN_SECONDS and r["seconds"] > SLOWER * old:
            slower.append((r, old))
    return broke, fixed, slower


def write_report(report, previous, path):
    lines = [f"# Conformance run {report['date']}", ""]
    lines.append(f"Ballista via Spark Connect (`{report['remote']}`) compared with vanilla "
                 f"Spark 3.5.8 (Comet off). Numbers match within relative {REL_TOL:g} or "
                 f"absolute {ABS_TOL:g}. Commit `{report['commit']}`."
                 + (f" Previous run: {previous['date']}." if previous else ""))
    lines += ["", "| Suite | Match | Failed or different | Total time |", "|---|---|---|---|"]
    for s in report["suites"].values():
        bad = [r for r in s["queries"] if not r["ok"]]
        total = sum(r["seconds"] or 0 for r in s["queries"])
        lines.append(f"| {s['title']} | {len(s['queries']) - len(bad)}/{len(s['queries'])} "
                     f"| {len(bad)} | {total:.1f} s |")
    for name, s in report["suites"].items():
        bad = [r for r in s["queries"] if not r["ok"]]
        broke, fixed, slower = changes(name, s["queries"], previous)
        lines += ["", f"## {s['title']}", ""]
        if bad:
            lines += ["Differences from Spark:", ""]
            lines += [f"- q{r['query']}: {r['result']}" for r in bad]
        else:
            lines.append("Every query matches Spark.")
        if previous:
            lines += ["", "Since the previous run:", ""]
            lines += [f"- **now wrong** q{r['query']}: {r['result']}" for r in broke]
            lines += [f"- now right: q{r['query']}" for r in fixed]
            lines += [f"- slower: q{r['query']} {old:.1f} s → {r['seconds']:.1f} s" for r, old in slower]
            if not (broke or fixed or slower):
                lines.append("- no change in answers; no query more than "
                             f"{SLOWER}× slower")
        timed = sorted((r for r in s["queries"] if r["seconds"]), key=lambda r: -r["seconds"])[:5]
        if timed:
            lines += ["", "Slowest: " + ", ".join(f"q{r['query']} {r['seconds']:.1f} s" for r in timed)]
    Path(path).write_text("\n".join(lines) + "\n")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--remote", default="sc://localhost:15002")
    p.add_argument("--answers", default=str(HOME / "Documents/tpch/answers"))
    p.add_argument("--tpch-data", default=str(HOME / "Documents/tpch/sf10"))
    p.add_argument("--tpch-queries", default=str(HOME / "Documents/datafusion-benchmarks/tpch/queries"))
    p.add_argument("--tpcds-data", default="/mnt/data/tpcds/sf10-parquet")
    p.add_argument("--reports", default=str(HOME / "Documents/ballista/conformance-reports"))
    p.add_argument("--timeout", type=int, default=7200, help="per suite, seconds")
    p.add_argument("--only", choices=["tpch", "tpcds"])
    args = p.parse_args()

    today = datetime.date.today().isoformat()
    label = f"nightly-{today}"
    reports = Path(args.reports)
    (reports / "logs").mkdir(parents=True, exist_ok=True)
    commit = subprocess.run(["git", "-C", str(HERE), "rev-parse", "--short", "HEAD"],
                            capture_output=True, text=True).stdout.strip() or "unknown"
    report = {"date": today, "remote": args.remote, "commit": commit, "suites": {}}
    for suite in suites(args):
        if args.only and suite["name"] != args.only:
            continue
        start = time.time()
        log = reports / "logs" / f"{today}-{suite['name']}.log"
        times, errors = run_suite(suite, label, args, log)
        rows = check_suite(suite, label, times, errors)
        report["suites"][suite["name"]] = {"title": suite["title"], "queries": rows,
                                           "wall_seconds": round(time.time() - start, 1)}
        ok = sum(r["ok"] for r in rows)
        print(f"{suite['title']}: {ok}/{len(rows)} match Spark")

    previous = previous_report(reports, today)
    (reports / f"{today}.json").write_text(json.dumps(report, indent=1))
    write_report(report, previous, reports / f"{today}.md")
    shutil.copyfile(reports / f"{today}.md", reports / "latest.md")
    print(f"report: {reports / (today + '.md')}")
    bad = sum(not r["ok"] for s in report["suites"].values() for r in s["queries"])
    raise SystemExit(1 if bad else 0)


if __name__ == "__main__":
    main()
