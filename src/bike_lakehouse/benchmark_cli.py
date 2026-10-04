"""Command-line reproducible local benchmark."""

from __future__ import annotations

import argparse
from datetime import date
import json
from pathlib import Path

from .benchmark import run_benchmark
from .spark import create_local_spark


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark the local batch pipeline")
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--work", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=10_000)
    parser.add_argument("--seed", type=int, default=2027)
    parser.add_argument("--date", type=date.fromisoformat, default=date(2026, 10, 1))
    parser.add_argument("--warmup-rounds", type=int, default=1)
    parser.add_argument("--measured-rounds", type=int, default=3)
    args = parser.parse_args()
    spark = create_local_spark("bike-trip-benchmark", Path("build/spark-warehouse"))
    try:
        report = run_benchmark(
            spark,
            repo_root=args.repo_root,
            work_path=args.work,
            report_path=args.report,
            input_rows=args.rows,
            seed=args.seed,
            batch_date=args.date,
            warmup_rounds=args.warmup_rounds,
            measured_rounds=args.measured_rounds,
        )
        print(json.dumps(report["summary"], ensure_ascii=False, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
