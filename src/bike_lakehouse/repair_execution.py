"""Preflight, resume, and execute deterministic partition-repair plans."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Callable

from .batch_manifest import fingerprint_file

RepairExecutor = Callable[[date, Path, str], dict[str, Any]]
RepairVerifier = Callable[[date, str], dict[str, Any]]


def _successful_repairs(manifests: list[dict[str, Any]]) -> set[tuple[str, str]]:
    return {
        (str(row.get("target_date")), str(row.get("source_sha256")))
        for row in manifests
        if row.get("pipeline") == "trip_date_backfill" and row.get("status") == "SUCCEEDED"
    }


def execute_repair_plan(
    plan: dict[str, Any],
    *,
    source_root: Path,
    current_partitions: set[str],
    backfill_manifests: list[dict[str, Any]],
    executor: RepairExecutor,
    verifier: RepairVerifier | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Execute a repair only when its source and persisted postconditions are valid."""
    if plan.get("plan_version") != "1.0":
        raise ValueError("unsupported repair plan version")
    actions = plan.get("actions")
    if not isinstance(actions, list):
        raise ValueError("repair plan actions must be a list")
    source_root = source_root.resolve()
    if not source_root.is_dir():
        raise ValueError(f"source root does not exist: {source_root}")
    successful = _successful_repairs(backfill_manifests)
    prepared: list[dict[str, Any]] = []
    action_ids: set[str] = set()
    for action in actions:
        action_id = str(action.get("action_id", ""))
        ingestion_date = str(action.get("ingestion_date", ""))
        source_name = action.get("source_name")
        expected_sha = action.get("source_sha256")
        if not action_id or action_id in action_ids:
            raise ValueError("repair action IDs must be non-empty and unique")
        action_ids.add(action_id)
        date.fromisoformat(ingestion_date)
        row: dict[str, Any] = {
            "action_id": action_id,
            "ingestion_date": ingestion_date,
            "source_name": source_name,
            "source_sha256": expected_sha,
        }
        if action.get("status") != "READY" or not source_name or not expected_sha:
            row["status"] = "BLOCKED_SOURCE_MISSING"
            prepared.append(row)
            continue
        source_path = (source_root / str(source_name)).resolve()
        if source_path.parent != source_root or not source_path.is_file():
            row["status"] = "BLOCKED_SOURCE_UNAVAILABLE"
            prepared.append(row)
            continue
        actual_sha, _ = fingerprint_file(source_path)
        if actual_sha != expected_sha:
            row["status"] = "BLOCKED_SOURCE_HASH_MISMATCH"
            row["actual_source_sha256"] = actual_sha
            prepared.append(row)
            continue
        if ingestion_date in current_partitions and (ingestion_date, expected_sha) in successful:
            row["status"] = "SKIPPED_ALREADY_COMPLETE"
        else:
            row["status"] = "WOULD_EXECUTE" if dry_run else "READY_TO_EXECUTE"
            row["source_path"] = source_path
        prepared.append(row)

    blocked = [row for row in prepared if str(row["status"]).startswith("BLOCKED_")]
    public_actions = [
        {key: value for key, value in row.items() if key != "source_path"} for row in prepared
    ]
    if blocked:
        return {
            "execution_version": "1.0",
            "mode": "DRY_RUN" if dry_run else "EXECUTE",
            "status": "BLOCKED",
            "actions": public_actions,
        }
    if dry_run:
        return {
            "execution_version": "1.0",
            "mode": "DRY_RUN",
            "status": "READY",
            "actions": public_actions,
        }
    if verifier is None and any(row["status"] == "READY_TO_EXECUTE" for row in prepared):
        raise ValueError("repair execution requires a postcondition verifier")

    results: list[dict[str, Any]] = []
    failed = False
    for row in prepared:
        public = {key: value for key, value in row.items() if key != "source_path"}
        if row["status"] == "SKIPPED_ALREADY_COMPLETE":
            results.append(public)
            continue
        try:
            result = executor(
                date.fromisoformat(str(row["ingestion_date"])),
                row["source_path"],
                str(row["action_id"]),
            )
            verification = verifier(
                date.fromisoformat(str(row["ingestion_date"])),
                str(row["source_sha256"]),
            )
            partition_present = verification.get("partition_present")
            matching_manifest = verification.get("matching_success_manifest")
            if not isinstance(partition_present, bool) or not isinstance(matching_manifest, bool):
                raise ValueError("postcondition verifier must return boolean evidence")
            evidence = {
                "partition_present": partition_present,
                "matching_success_manifest": matching_manifest,
            }
            if not partition_present or not matching_manifest:
                results.append(
                    {
                        **public,
                        "status": "POSTCONDITION_FAILED",
                        "result": result,
                        "verification": evidence,
                    }
                )
                failed = True
                break
            results.append(
                {
                    **public,
                    "status": "VERIFIED",
                    "result": result,
                    "verification": evidence,
                }
            )
        except Exception as error:
            results.append(
                {
                    **public,
                    "status": "FAILED",
                    "error_type": type(error).__name__,
                    "error_message": str(error)[:1000],
                }
            )
            failed = True
            break
    return {
        "execution_version": "1.0",
        "mode": "EXECUTE",
        "status": "FAILED" if failed else "COMPLETE",
        "actions": results,
    }
