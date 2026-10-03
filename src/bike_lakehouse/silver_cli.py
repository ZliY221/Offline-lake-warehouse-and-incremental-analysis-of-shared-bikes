"""Command-line Silver trip build."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .silver import build_trip_silver
from .spark import create_local_spark


def main() -> None:
    parser = argparse.ArgumentParser(description="Build validated Silver trip datasets")
    parser.add_argument("--bronze", type=Path, required=True)
    parser.add_argument("--valid", type=Path, required=True)
    parser.add_argument("--rejected", type=Path, required=True)
    parser.add_argument("--duplicates", type=Path, required=True)
    args = parser.parse_args()
    spark = create_local_spark("bike-trip-silver", Path("build/spark-warehouse"))
    try:
        result = build_trip_silver(
            spark,
            bronze_path=args.bronze,
            valid_path=args.valid,
            rejected_path=args.rejected,
            duplicate_path=args.duplicates,
        )
        print(json.dumps(result.to_dict(), ensure_ascii=False, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
