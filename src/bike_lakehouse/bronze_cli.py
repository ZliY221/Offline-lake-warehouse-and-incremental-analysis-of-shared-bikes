"""Command-line Bronze trip ingestion."""

from __future__ import annotations

import argparse
from datetime import date
import json
from pathlib import Path

from .bronze import ingest_trip_bronze
from .spark import create_local_spark


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a Bronze trip partition")
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--ingestion-date", type=date.fromisoformat, required=True)
    parser.add_argument("--manifest", type=Path)
    args = parser.parse_args()
    spark = create_local_spark("bike-trip-bronze", Path("build/spark-warehouse"))
    try:
        result = ingest_trip_bronze(
            spark,
            input_path=args.input,
            output_path=args.output,
            ingestion_date=args.ingestion_date,
            manifest_path=args.manifest,
        )
        print(json.dumps(result.to_dict(), ensure_ascii=False, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
