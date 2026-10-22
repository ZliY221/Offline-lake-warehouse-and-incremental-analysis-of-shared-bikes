"""Deterministic, read-only evidence sampling for offline partitions."""

from __future__ import annotations

from dataclasses import dataclass
from math import floor
from typing import Any


@dataclass(frozen=True)
class PartitionEvidenceCandidate:
    partition: str
    row_count: int
    quality_risk_score: float
    freshness_hours: float


def plan_quality_evidence_samples(
    candidates: list[PartitionEvidenceCandidate], *, sample_budget: int,
    minimum_per_partition: int = 1,
) -> dict[str, Any]:
    """Allocate a bounded audit sample without reading or changing source rows."""
    if not candidates or sample_budget < 1 or minimum_per_partition < 0:
        raise ValueError("candidates and a positive sample budget are required")
    names = [item.partition.strip() for item in candidates]
    if (any(not name for name in names)
            or any(item.partition != item.partition.strip() for item in candidates)
            or len(names) != len(set(names))):
        raise ValueError("partition names must be non-empty and unique")
    for item in candidates:
        if item.row_count < 0 or not 0 <= item.quality_risk_score <= 1 or item.freshness_hours < 0:
            raise ValueError("partition evidence inputs are invalid")

    non_empty = [item for item in candidates if item.row_count > 0]
    required = len(non_empty) * minimum_per_partition
    if required > sample_budget:
        raise ValueError("sample budget cannot cover the minimum per non-empty partition")

    allocations = {item.partition: min(minimum_per_partition, item.row_count) for item in non_empty}
    remaining = sample_budget - sum(allocations.values())
    weights = {
        item.partition: item.row_count * (1 + 2 * item.quality_risk_score)
        * (1 + min(item.freshness_hours, 168) / 168)
        for item in non_empty
    }
    while remaining > 0:
        eligible = [item for item in non_empty if allocations[item.partition] < item.row_count]
        if not eligible:
            break
        total_weight = sum(weights[item.partition] for item in eligible)
        grants: dict[str, int] = {}
        for item in eligible:
            share = floor(remaining * weights[item.partition] / total_weight)
            grants[item.partition] = min(max(share, 1), item.row_count - allocations[item.partition])
        granted = min(remaining, sum(grants.values()))
        if granted == 0:
            break
        for item in sorted(eligible, key=lambda row: (-weights[row.partition], row.partition)):
            quantity = min(grants[item.partition], remaining)
            allocations[item.partition] += quantity
            remaining -= quantity
            if remaining == 0:
                break

    plan = []
    for item in sorted(candidates, key=lambda row: row.partition):
        count = allocations.get(item.partition, 0)
        plan.append({"partition": item.partition, "row_count": item.row_count,
                     "quality_risk_score": item.quality_risk_score,
                     "freshness_hours": item.freshness_hours,
                     "sample_count": count,
                     "selection_rule": "stable_hash(row_key)" if count else "EMPTY_PARTITION"})
    allocated = sum(row["sample_count"] for row in plan)
    return {"status": "READY" if allocated else "NO_ROWS",
            "partition_count": len(candidates), "sample_budget": sample_budget,
            "allocated_samples": allocated, "unused_budget": sample_budget - allocated,
            "samples": plan, "source_rows_read": False, "sampling_started": False}
