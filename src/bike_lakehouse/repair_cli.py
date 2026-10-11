"""Build and audit controlled missing-partition repair plans."""

from __future__ import annotations

import argparse
from datetime import date
import json
from pathlib import Path

from .batch_manifest import write_json_atomic
from .repair_plan import audit_repair_plan, build_repair_plan, discover_partitions, load_manifest_directory


def main() -> None:
    parser = argparse.ArgumentParser(description="Plan or audit missing Bronze partition repairs")
    subparsers = parser.add_subparsers(dest="command", required=True)
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
    args = parser.parse_args()

    if args.command == "plan":
        catalog = json.loads(args.source_catalog.read_text(encoding="utf-8"))
        report = build_repair_plan(
            discover_partitions(args.bronze), args.required_first_date, args.required_latest_date, catalog.get("sources", [])
        )
    else:
        report = audit_repair_plan(
            json.loads(args.plan.read_text(encoding="utf-8")),
            discover_partitions(args.bronze),
            load_manifest_directory(args.backfill_manifests),
        )
    write_json_atomic(args.output, report)
    print(json.dumps({"status": report.get("status", "PLANNED"), "output": str(args.output)}))


if __name__ == "__main__":
    main()
