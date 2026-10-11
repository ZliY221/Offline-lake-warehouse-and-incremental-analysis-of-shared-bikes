"""Command-line cross-layer quality report."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .quality_report import build_quality_report
from .spark import create_local_spark


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a cross-layer lakehouse quality report")
    parser.add_argument("--bronze", type=Path, required=True)
    parser.add_argument("--silver-valid", type=Path, required=True)
    parser.add_argument("--silver-rejected", type=Path, required=True)
    parser.add_argument("--silver-duplicates", type=Path, required=True)
    parser.add_argument("--station-dimension", type=Path, required=True)
    parser.add_argument("--station-rejected", type=Path, required=True)
    parser.add_argument("--gold-daily-metrics", type=Path, required=True)
    parser.add_argument("--gold-popular-routes", type=Path, required=True)
    parser.add_argument("--gold-cohort-retention", type=Path, required=True)
    parser.add_argument("--bronze-manifest", type=Path, required=True)
    parser.add_argument("--backfill-manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--route-limit", type=int, default=3)
    parser.add_argument("--max-rejected-ratio", type=float, default=0.05)
    parser.add_argument("--max-duplicate-ratio", type=float, default=0.05)
    parser.add_argument("--fail-on-error", action="store_true")
    args = parser.parse_args()
    spark = create_local_spark("bike-trip-quality-report", Path("build/spark-warehouse"))
    try:
        report = build_quality_report(
            spark,
            bronze_path=args.bronze,
            silver_valid_path=args.silver_valid,
            silver_rejected_path=args.silver_rejected,
            silver_duplicate_path=args.silver_duplicates,
            station_dimension_path=args.station_dimension,
            station_rejected_path=args.station_rejected,
            gold_daily_metrics_path=args.gold_daily_metrics,
            gold_popular_routes_path=args.gold_popular_routes,
            gold_cohort_retention_path=args.gold_cohort_retention,
            bronze_manifest_path=args.bronze_manifest,
            backfill_manifest_path=args.backfill_manifest,
            output_path=args.output,
            route_limit=args.route_limit,
            max_rejected_ratio=args.max_rejected_ratio,
            max_duplicate_ratio=args.max_duplicate_ratio,
        )
        print(
            json.dumps(
                {
                    "overall_status": report["overall_status"],
                    "check_count": len(report["checks"]),
                    "output": str(args.output),
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        if args.fail_on_error and report["overall_status"] != "PASS":
            raise SystemExit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
