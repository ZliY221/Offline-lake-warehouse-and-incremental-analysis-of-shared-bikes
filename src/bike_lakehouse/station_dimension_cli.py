"""Command-line station SCD2 dimension build."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .spark import create_local_spark
from .station_dimension import build_station_dimension


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the station SCD2 dimension")
    parser.add_argument("--input", type=Path, action="append", required=True)
    parser.add_argument("--dimension", type=Path, required=True)
    parser.add_argument("--rejected", type=Path, required=True)
    args = parser.parse_args()
    spark = create_local_spark("bike-station-dimension", Path("build/spark-warehouse"))
    try:
        result = build_station_dimension(
            spark,
            input_paths=args.input,
            dimension_path=args.dimension,
            rejected_path=args.rejected,
        )
        print(json.dumps(result.to_dict(), ensure_ascii=False, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
