"""Deterministic missing-partition repair plans and completion audits."""

from __future__ import annotations

from datetime import date, timedelta
import hashlib
import json
import re
from pathlib import Path
from typing import Any

SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


def _required_dates(first: date, latest: date) -> list[date]:
    if first > latest:
        raise ValueError("required_first_date must not be later than required_latest_date")
    count = (latest - first).days + 1
    if count > 366:
        raise ValueError("repair range must not exceed 366 days")
    return [first + timedelta(days=offset) for offset in range(count)]


def build_repair_plan(
    available_partitions: set[str],
    required_first_date: date,
    required_latest_date: date,
    source_catalog: list[dict[str, Any]],
) -> dict[str, Any]:
    """Build one stable repair action per missing required partition."""
    sources: dict[str, dict[str, str]] = {}
    for entry in source_catalog:
        ingestion_date = str(entry.get("ingestion_date", ""))
        source_name = str(entry.get("source_name", ""))
        source_sha256 = str(entry.get("source_sha256", ""))
        date.fromisoformat(ingestion_date)
        if not source_name or Path(source_name).name != source_name:
            raise ValueError("source_name must be a file name without directory components")
        if not SHA256_PATTERN.fullmatch(source_sha256):
            raise ValueError("source_sha256 must contain 64 lowercase hexadecimal characters")
        if ingestion_date in sources:
            raise ValueError(f"duplicate source catalog date: {ingestion_date}")
        sources[ingestion_date] = {"source_name": source_name, "source_sha256": source_sha256}

    missing = [
        value.isoformat()
        for value in _required_dates(required_first_date, required_latest_date)
        if value.isoformat() not in available_partitions
    ]
    actions: list[dict[str, Any]] = []
    for ingestion_date in missing:
        source = sources.get(ingestion_date)
        material = f"{ingestion_date}:{source['source_sha256'] if source else 'missing'}"
        actions.append(
            {
                "action_id": f"repair-{hashlib.sha256(material.encode()).hexdigest()[:20]}",
                "ingestion_date": ingestion_date,
                "status": "READY" if source else "BLOCKED_SOURCE_MISSING",
                "source_name": source["source_name"] if source else None,
                "source_sha256": source["source_sha256"] if source else None,
                "command": (
                    ["backfill-date", "--target-date", ingestion_date, "--input", source["source_name"]]
                    if source
                    else None
                ),
            }
        )
    return {
        "plan_version": "1.0",
        "required_first_ingestion_date": required_first_date.isoformat(),
        "required_latest_ingestion_date": required_latest_date.isoformat(),
        "available_partition_count": len(available_partitions),
        "missing_partition_count": len(missing),
        "ready_action_count": sum(action["status"] == "READY" for action in actions),
        "blocked_action_count": sum(action["status"] == "BLOCKED_SOURCE_MISSING" for action in actions),
        "actions": actions,
    }


def audit_repair_plan(
    plan: dict[str, Any],
    current_partitions: set[str],
    backfill_manifests: list[dict[str, Any]],
) -> dict[str, Any]:
    successful = {
        (str(record.get("target_date")), str(record.get("source_sha256")))
        for record in backfill_manifests
        if record.get("pipeline") == "trip_date_backfill" and record.get("status") == "SUCCEEDED"
    }
    results: list[dict[str, Any]] = []
    for action in plan.get("actions", []):
        target = str(action["ingestion_date"])
        source_sha256 = action.get("source_sha256")
        partition_present = target in current_partitions
        manifest_succeeded = (target, str(source_sha256)) in successful if source_sha256 else False
        completed = partition_present and manifest_succeeded
        results.append(
            {
                "action_id": action["action_id"],
                "ingestion_date": target,
                "status": "COMPLETE" if completed else "INCOMPLETE",
                "partition_present": partition_present,
                "matching_success_manifest": manifest_succeeded,
            }
        )
    return {
        "audit_version": "1.0",
        "status": "COMPLETE" if all(row["status"] == "COMPLETE" for row in results) else "INCOMPLETE",
        "planned_action_count": len(results),
        "completed_action_count": sum(row["status"] == "COMPLETE" for row in results),
        "actions": results,
    }


def discover_partitions(root: Path) -> set[str]:
    if not root.is_dir():
        raise ValueError(f"Bronze dataset does not exist: {root}")
    values = set()
    for path in root.glob("ingestion_date=*"):
        if path.is_dir():
            value = path.name.split("=", 1)[1]
            date.fromisoformat(value)
            values.add(value)
    return values


def load_manifest_directory(root: Path) -> list[dict[str, Any]]:
    if not root.is_dir():
        return []
    return [json.loads(path.read_text(encoding="utf-8")) for path in sorted(root.glob("*.json"))]
