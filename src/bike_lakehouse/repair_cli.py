"""Build and audit controlled missing-partition repair plans."""

from __future__ import annotations

import argparse
from datetime import date
import json
from pathlib import Path

from .backfill import run_date_backfill
from .batch_manifest import write_json_atomic
from .repair_execution import execute_repair_plan
from .repair_plan import (
    audit_repair_plan,
    build_repair_plan,
    build_source_catalog,
    discover_partitions,
    load_manifest_directory,
)
from .spark import create_local_spark


def main() -> None:
    parser = argparse.ArgumentParser(description="Plan or audit missing Bronze partition repairs")
    subparsers = parser.add_subparsers(dest="command", required=True)
    catalog = subparsers.add_parser("catalog")
    catalog.add_argument("--source-root", type=Path, required=True)
    catalog.add_argument("--output", type=Path, required=True)
    plan = subparsers.add_parser("plan")
    plan.add_argument("--bronze", type=Path, required=True)
    plan.add_argument("--required-first-date", type=date.fromisoformat, required=True)
    plan.add_argument("--required-latest-date", type=date.fromisoformat, required=True)
    plan.add_argument("--source-catalog", type=Path, required=True)
    plan.add_argument("--output", type=Path, required=True)
    audit = subparsers.add_parser("audit")
    audit.add_argument("--plan", type=Path, required=True)
    audit.add_argument("--bronze", type=Path, required=True)
    audit.add_argument("--backfill-manifests", type=Path, required=True)
    audit.add_argument("--output", type=Path, required=True)
    execute = subparsers.add_parser("execute")
    execute.add_argument("--plan", type=Path, required=True)
    execute.add_argument("--source-root", type=Path, required=True)
    execute.add_argument("--bronze", type=Path, required=True)
    execute.add_argument("--bronze-manifests", type=Path, required=True)
    execute.add_argument("--silver-valid", type=Path, required=True)
    execute.add_argument("--silver-rejected", type=Path, required=True)
    execute.add_argument("--silver-duplicates", type=Path, required=True)
    execute.add_argument("--station-dimension", type=Path, required=True)
    execute.add_argument("--gold-daily-metrics", type=Path, required=True)
    execute.add_argument("--gold-popular-routes", type=Path, required=True)
    execute.add_argument("--gold-cohort-retention", type=Path, required=True)
    execute.add_argument("--backfill-manifests", type=Path, required=True)
    execute.add_argument("--output", type=Path, required=True)
    execute.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if args.command == "catalog":
        report = build_source_catalog(args.source_root)
    elif args.command == "plan":
        catalog = json.loads(args.source_catalog.read_text(encoding="utf-8"))
        report = build_repair_plan(
            discover_partitions(args.bronze), args.required_first_date, args.required_latest_date, catalog.get("sources", [])
        )
    elif args.command == "audit":
        report = audit_repair_plan(
            json.loads(args.plan.read_text(encoding="utf-8")),
            discover_partitions(args.bronze),
            load_manifest_directory(args.backfill_manifests),
        )
    else:
        plan_document = json.loads(args.plan.read_text(encoding="utf-8"))
        spark_holder: dict[str, object] = {}

        def run(target_date: date, source_path: Path, action_id: str) -> dict[str, object]:
            spark = spark_holder.get("spark")
            if spark is None:
                spark = create_local_spark("bike-trip-repair-plan", Path("build/spark-warehouse"))
                spark_holder["spark"] = spark
            return run_date_backfill(
                spark,
                input_path=source_path,
                target_date=target_date,
                bronze_path=args.bronze,
                bronze_manifest_path=args.bronze_manifests,
                silver_valid_path=args.silver_valid,
                silver_rejected_path=args.silver_rejected,
                silver_duplicate_path=args.silver_duplicates,
                station_dimension_path=args.station_dimension,
                gold_daily_metrics_path=args.gold_daily_metrics,
                gold_popular_routes_path=args.gold_popular_routes,
                gold_cohort_retention_path=args.gold_cohort_retention,
                backfill_manifest_path=args.backfill_manifests,
            ).to_dict()

        def verify(target_date: date, source_sha256: str) -> dict[str, bool]:
            target = target_date.isoformat()
            manifests = load_manifest_directory(args.backfill_manifests)
            return {
                "partition_present": target in discover_partitions(args.bronze),
                "matching_success_manifest": any(
                    row.get("pipeline") == "trip_date_backfill"
                    and row.get("status") == "SUCCEEDED"
                    and row.get("target_date") == target
                    and row.get("source_sha256") == source_sha256
                    for row in manifests
                ),
            }

        try:
            report = execute_repair_plan(
                plan_document,
                source_root=args.source_root,
                current_partitions=discover_partitions(args.bronze),
                backfill_manifests=load_manifest_directory(args.backfill_manifests),
                executor=run,
                verifier=verify,
                dry_run=args.dry_run,
            )
        finally:
            spark = spark_holder.get("spark")
            if spark is not None:
                spark.stop()
    write_json_atomic(args.output, report)
    print(json.dumps({"status": report.get("status", "PLANNED"), "output": str(args.output)}))
    if report.get("status") in {"BLOCKED", "FAILED", "INCOMPLETE"}:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
