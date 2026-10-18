"""Command-line read-only date backfill planner."""

from __future__ import annotations

import argparse
from datetime import date
import json
from pathlib import Path

from .backfill_plan import plan_date_backfill
from .spark import create_local_spark


def main() -> None:
    parser = argparse.ArgumentParser(description="Preview the impact of one trip date backfill")
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--target-date", type=date.fromisoformat, required=True)
    parser.add_argument("--bronze", type=Path, required=True)
    parser.add_argument("--silver-valid", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    spark = create_local_spark("bike-trip-backfill-plan", Path("build/spark-warehouse"))
    try:
        payload = plan_date_backfill(
            spark,
            input_path=args.input,
            target_date=args.target_date,
            bronze_path=args.bronze,
            silver_valid_path=args.silver_valid,
        ).to_dict()
        encoded = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
        if args.output is not None:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(encoded + "\n", encoding="utf-8")
        print(encoded)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
