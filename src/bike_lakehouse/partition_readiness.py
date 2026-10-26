"""Assess whether offline source partitions are safe to enter the pipeline."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class PartitionReadinessInput:
    partition: str
    expected_rows: int
    observed_rows: int
    upstream_complete: bool
    contract_valid: bool
    checksum_available: bool
    late_by_minutes: int = 0


def assess_partition_readiness(
    partitions: list[PartitionReadinessInput],
    *,
    minimum_completeness: float = 0.98,
    maximum_lateness_minutes: int = 30,
) -> dict[str, Any]:
    """Return a release-safe readiness decision for each source partition."""
    if not partitions:
        raise ValueError("at least one partition is required")
    if not 0 < minimum_completeness <= 1:
        raise ValueError("minimum completeness must be in (0, 1]")
    if maximum_lateness_minutes < 0:
        raise ValueError("maximum lateness must be non-negative")
    names = [item.partition.strip() for item in partitions]
    if any(not name for name in names) or len(names) != len(set(names)):
        raise ValueError("partition names must be non-empty and unique")

    results = []
    for item in partitions:
        if item.expected_rows <= 0 or item.observed_rows < 0 or item.late_by_minutes < 0:
            raise ValueError("partition row counts and lateness are invalid")
        completeness = min(item.observed_rows / item.expected_rows, 1.0)
        blockers: list[str] = []
        warnings: list[str] = []
        if not item.upstream_complete:
            blockers.append("UPSTREAM_NOT_COMPLETE")
        if not item.contract_valid:
            blockers.append("CONTRACT_INVALID")
        if not item.checksum_available:
            blockers.append("CHECKSUM_MISSING")
        if completeness < minimum_completeness:
            blockers.append("ROW_COMPLETENESS_BELOW_THRESHOLD")
        if item.late_by_minutes > maximum_lateness_minutes:
            warnings.append("PARTITION_LATE")

        if blockers:
            status, next_action = "BLOCKED", "REPAIR_SOURCE_PARTITION"
        elif warnings:
            status, next_action = "AT_RISK", "CONFIRM_LATE_ARRIVAL_WINDOW"
        else:
            status, next_action = "READY", "START_INCREMENTAL_LOAD"
        results.append({
            **asdict(item),
            "partition": item.partition.strip(),
            "completeness_ratio": round(completeness, 4),
            "status": status,
            "blockers": blockers,
            "warnings": warnings,
            "next_action": next_action,
        })

    counts = {
        status.lower(): sum(row["status"] == status for row in results)
        for status in ("READY", "AT_RISK", "BLOCKED")
    }
    overall = "BLOCKED" if counts["blocked"] else "AT_RISK" if counts["at_risk"] else "READY"
    return {
        "status": f"PARTITION_READINESS_{overall}",
        "partition_count": len(results),
        "counts": counts,
        "minimum_completeness": minimum_completeness,
        "maximum_lateness_minutes": maximum_lateness_minutes,
        "partitions": results,
        "incremental_load_started": False,
    }
