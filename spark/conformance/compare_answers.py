"""Compare per-query answer CSVs from several engines against a reference.

    python3 compare_answers.py <answers_dir> <reference> <engine> [<engine>...]

<answers_dir>/<engine>/q<N>.csv as written by tpcbench --results-dir /
spark_tpcbench.py --results-dir. Cells are compared positionally (column
names differ between engines for unaliased expressions). Numbers match within
REL_TOL relative or ABS_TOL absolute difference, which absorbs exact decimal
vs floating-point arithmetic.
"""

import csv
import math
import sys
from pathlib import Path

REL_TOL = 1e-9
ABS_TOL = 1e-4


def load(path):
    with open(path, newline="") as f:
        rows = list(csv.reader(f))
    return rows[0], rows[1:]


def num(s):
    try:
        v = float(s)
        return v if math.isfinite(v) else None
    except ValueError:
        return None


def cell_diff(a, b):
    """None if equal, else the numeric abs difference (or inf for non-numeric)."""
    if a == b:
        return None
    x, y = num(a), num(b)
    if x is None or y is None:
        # Spark writes booleans as True/False, arrow as true/false
        return None if a.lower() == b.lower() else math.inf
    d = abs(x - y)
    if d <= ABS_TOL or d <= REL_TOL * max(abs(x), abs(y)):
        return None
    return d


def compare_rows(ref, got):
    worst = 0.0
    for i, (r, g) in enumerate(zip(ref, got)):
        for j, (a, b) in enumerate(zip(r, g)):
            d = cell_diff(a, b)
            if d is not None:
                return f"row {i + 1} col {j + 1}: expected {a!r}, got {b!r}", None
            x, y = num(a), num(b)
            if x is not None and y is not None and x != y and max(abs(x), abs(y)) > 0:
                worst = max(worst, abs(x - y) / max(abs(x), abs(y)))
    return None, worst


def sort_key(row):
    return [(0, round(num(c), 4), "") if num(c) is not None else (1, 0, c) for c in row]


def compare_query(ref_path, got_path):
    """Compares one query's answer with the reference: (matches, description)."""
    _, ref = load(ref_path)
    if not Path(got_path).exists():
        return False, "MISSING"
    _, got = load(got_path)
    if len(got) != len(ref):
        return False, f"DIFF row count {len(got)} vs {len(ref)}"
    if ref and len(got[0]) != len(ref[0]):
        return False, f"DIFF column count {len(got[0])} vs {len(ref[0])}"
    err, worst = compare_rows(ref, got)
    note = ""
    if err:
        # ORDER BY ties may legitimately come back in another order
        err2, worst = compare_rows(sorted(ref, key=sort_key), sorted(got, key=sort_key))
        if err2:
            return False, f"DIFF {err}"
        note = " (tie order differs)"
    return True, (f"match, max rel diff {worst:.1e}{note}" if worst else f"exact match{note}")


def main():
    base, ref_engine, engines = Path(sys.argv[1]), sys.argv[2], sys.argv[3:]
    queries = sorted((int(p.stem[1:]) for p in (base / ref_engine).glob("q*.csv")))
    print(f"Reference: {ref_engine}   tolerance: rel {REL_TOL:g} / abs {ABS_TOL:g}\n")
    print(f"{'query':>5} {'rows':>6}  " + "  ".join(f"{e:<34}" for e in engines))
    summary = {e: [0, 0] for e in engines}
    for q in queries:
        ref_path = base / ref_engine / f"q{q}.csv"
        cols = []
        for e in engines:
            ok, description = compare_query(ref_path, base / e / f"q{q}.csv")
            summary[e][1] += 1
            summary[e][0] += ok
            cols.append(description)
        rows = len(load(ref_path)[1])
        print(f"{'q' + str(q):>5} {rows:>6}  " + "  ".join(f"{c:<34}" for c in cols))
    print()
    for e, (ok, total) in summary.items():
        print(f"{e}: {ok}/{total} queries match {ref_engine}")


if __name__ == "__main__":
    main()
