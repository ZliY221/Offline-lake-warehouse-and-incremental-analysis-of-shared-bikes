"""Command-line controlled date backfill."""

from __future__ import annotations

import argparse
from datetime import date
import json
from pathlib import Path

from .backfill import run_date_backfill
from .spark import create_local_spark


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill one trip date through Gold")
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--target-date", type=date.fromisoformat, required=True)
    parser.add_argument("--bronze", type=Path, required=True)
    parser.add_argument("--bronze-manifest", type=Path, required=True)
    parser.add_argument("--silver-valid", type=Path, required=True)
    parser.add_argument("--silver-rejected", type=Path, required=True)
    parser.add_argument("--silver-duplicates", type=Path, required=True)
    parser.add_argument("--station-dimension", type=Path, required=True)
    parser.add_argument("--gold-daily-metrics", type=Path, required=True)
    parser.add_argument("--gold-popular-routes", type=Path, required=True)
    parser.add_argument("--backfill-manifest", type=Path, required=True)
    args = parser.parse_args()
    spark = create_local_spark("bike-trip-date-backfill", Path("build/spark-warehouse"))
    try:
        result = run_date_backfill(
            spark,
            input_path=args.input,
            target_date=args.target_date,
            bronze_path=args.bronze,
            bronze_manifest_path=args.bronze_manifest,
            silver_valid_path=args.silver_valid,
            silver_rejected_path=args.silver_rejected,
            silver_duplicate_path=args.silver_duplicates,
            station_dimension_path=args.station_dimension,
            gold_daily_metrics_path=args.gold_daily_metrics,
            gold_popular_routes_path=args.gold_popular_routes,
            backfill_manifest_path=args.backfill_manifest,
        )
        print(json.dumps(result.to_dict(), ensure_ascii=False, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
