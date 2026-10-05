"""Command-line Spark physical-plan evidence generator."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .plan_analysis import build_plan_analysis, write_plan_evidence
from .spark import create_local_spark


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare local Spark physical plans")
    parser.add_argument("--rows", type=int, default=1_000)
    parser.add_argument("--json", type=Path, required=True)
    parser.add_argument("--markdown", type=Path, required=True)
    args = parser.parse_args()
    spark = create_local_spark("bike-trip-plan-analysis", Path("build/spark-warehouse"))
    try:
        report = build_plan_analysis(spark, fact_rows=args.rows)
        write_plan_evidence(
            report,
            json_path=args.json,
            markdown_path=args.markdown,
        )
        print(
            json.dumps(
                {
                    "status": "PASS",
                    "json": str(args.json),
                    "markdown": str(args.markdown),
                    "baseline_join": report["baseline_sort_merge"]["features"],
                    "broadcast_join": report["explicit_broadcast"]["features"],
                    "adaptive": report["adaptive_aggregation"]["features"],
                },
                sort_keys=True,
            )
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
