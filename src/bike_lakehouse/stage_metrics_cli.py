"""Command-line Spark UI stage metrics evidence generator."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .spark import create_local_spark
from .stage_metrics import collect_stage_metrics, write_stage_metrics


def main() -> None:
    parser = argparse.ArgumentParser(description="Capture local Spark UI REST metrics")
    parser.add_argument("--rows", type=int, default=50_000)
    parser.add_argument("--partitions", type=int, default=8)
    parser.add_argument("--json", type=Path, required=True)
    parser.add_argument("--markdown", type=Path, required=True)
    args = parser.parse_args()
    spark = create_local_spark(
        "bike-trip-stage-metrics",
        Path("build/spark-warehouse"),
        ui_enabled=True,
    )
    try:
        report = collect_stage_metrics(
            spark,
            row_count=args.rows,
            shuffle_partitions=args.partitions,
        )
        write_stage_metrics(report, json_path=args.json, markdown_path=args.markdown)
        print(
            json.dumps(
                {
                    "status": "PASS",
                    "json": str(args.json),
                    "markdown": str(args.markdown),
                    "job_count": len(report["jobs"]),
                    "stage_count": len(report["stages"]),
                    "stage_totals": report["stage_totals"],
                },
                sort_keys=True,
            )
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
