from __future__ import annotations

import argparse
import json
from pathlib import Path

from bike_lakehouse.batch_manifest import write_json_atomic
from bike_lakehouse.compaction import (
    build_compaction_plan,
    execute_compaction_plan,
    spark_partition_writer,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Plan or execute auditable Parquet partition compaction"
    )
    parser.add_argument("--dataset", type=Path, required=True)
    parser.add_argument("--partition-column", required=True)
    parser.add_argument("--target-size-mb", type=int, default=128)
    parser.add_argument("--min-files", type=int, default=4)
    parser.add_argument(
        "--plan-output",
        type=Path,
        default=Path("build/compaction/plan.json"),
    )
    parser.add_argument("--execute", action="store_true")
    parser.add_argument(
        "--report-output",
        type=Path,
        default=Path("build/compaction/report.json"),
    )
    args = parser.parse_args()
    plan = build_compaction_plan(
        args.dataset,
        partition_column=args.partition_column,
        target_file_size_bytes=args.target_size_mb * 1024 * 1024,
        min_files=args.min_files,
    )
    write_json_atomic(args.plan_output, plan.to_dict())
    if not args.execute:
        print(json.dumps(plan.to_dict(), ensure_ascii=False, sort_keys=True))
        return
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.master("local[2]").appName(
        "bike-lakehouse-compaction"
    ).getOrCreate()
    try:
        report = execute_compaction_plan(
            args.dataset,
            plan,
            writer=spark_partition_writer(spark),
            report_path=args.report_output,
        )
        print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
