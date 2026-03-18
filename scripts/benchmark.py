"""
benchmark.py — Run all queries 5 times and report execution times.
Assignment 2: Big Data Storage & Retrieval

Output:
  - Per run: elapsed time only (no query output)
  - Per query/db: avg ± std
  - End: summary table
  - CSV: all raw timings saved to benchmark_results.csv
"""

import subprocess
import sys
import time
import statistics
import os
import csv
from tabulate import tabulate

SCRIPTS_DIR = os.path.dirname(os.path.abspath(__file__))
PYTHON      = sys.executable
RUNS        = 5

# (label, script_file)
QUERIES = [
    ("Q1a", "psql",       "q1a_psql.py"),
    ("Q1a", "psql_own",   "q1a_psql_own.py"),
    ("Q1a", "mongo",      "q1a_mongo.py"),
    ("Q1a", "memgraph",   "q1a_memgraph.py"),

    ("Q1b", "psql",       "q1b_psql.py"),
    ("Q1b", "psql_own",   "q1b_psql_own.py"),
    ("Q1b", "mongo",      "q1b_mongo.py"),
    ("Q1b", "memgraph",   "q1b_memgraph.py"),

    ("Q2",  "psql",       "q2_psql.py"),
    ("Q2",  "psql_own",   "q2_psql_own.py"),
    ("Q2",  "mongo",      "q2_mongo.py"),
    ("Q2",  "memgraph",   "q2_memgraph.py"),

    ("Q3",  "psql",       "q3_psql.py"),
    ("Q3",  "psql_own",   "q3_psql_own.py"),
    ("Q3",  "mongo",      "q3_mongo.py"),
    ("Q3",  "memgraph",   "q3_memgraph.py"),
]


def run_once(script: str) -> tuple[float, str, str]:
    """Run a script, return (elapsed_seconds, stdout, stderr)."""
    t0 = time.perf_counter()
    proc = subprocess.run(
        [PYTHON, script],
        capture_output=True,
        text=True,
        cwd=SCRIPTS_DIR,
    )
    elapsed = time.perf_counter() - t0
    return elapsed, proc.stdout, proc.stderr


def separator(label: str):
    width = 72
    print("\n" + "=" * width)
    print(f"  {label}")
    print("=" * width)


if __name__ == "__main__":
    records = []  # (query, db, run, elapsed)

    for query, db, script in QUERIES:
        label = f"{query} / {db}"
        separator(label)

        times = []

        run_once(script)  # warm-up: prime OS/DB caches, not recorded

        for run_idx in range(1, RUNS + 1):
            elapsed, stdout, stderr = run_once(script)
            times.append(elapsed)
            records.append((query, db, run_idx, round(elapsed, 4)))

            status = "ERROR" if stderr.strip() else "OK"
            print(f"  run {run_idx}/{RUNS}: {elapsed:.3f}s  [{status}]")
            if stderr.strip():
                print(f"    {stderr.strip()[:200]}")

        avg = statistics.mean(times)
        std = statistics.stdev(times) if len(times) > 1 else 0.0
        print(f"  → avg: {avg:.3f}s  std: {std:.3f}s")

    # ── Summary table ──────────────────────────────────────────────────────────
    separator("SUMMARY  (avg ± std over 5 runs, seconds)")

    # aggregate
    from collections import defaultdict
    grouped = defaultdict(list)
    for query, db, _, elapsed in records:
        grouped[(query, db)].append(elapsed)

    summary_rows = []
    for query, db, _ in [(q, d, s) for q, d, s in QUERIES]:
        key = (query, db)
        if key not in grouped:
            continue
        times = grouped.pop(key)   # pop so we don't duplicate
        avg = statistics.mean(times)
        std = statistics.stdev(times) if len(times) > 1 else 0.0
        summary_rows.append([query, db, f"{avg:.3f}", f"{std:.3f}",
                              f"{min(times):.3f}", f"{max(times):.3f}"])

    print(tabulate(
        summary_rows,
        headers=["Query", "Database", "Avg (s)", "Std (s)", "Min (s)", "Max (s)"],
        tablefmt="psql",
    ))

    # ── Save CSV ───────────────────────────────────────────────────────────────
    csv_path = os.path.join(SCRIPTS_DIR, "benchmark_results.csv")
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["query", "database", "run", "elapsed_s"])
        writer.writerows(records)
    print(f"\nRaw results saved to: {csv_path}")
