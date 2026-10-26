"""Plan capacity-bounded incremental load windows for ready partitions."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class IncrementalLoadCandidate:
    partition: str
    readiness: str
    estimated_minutes: int
    deadline_minute: int
    priority: int


def plan_incremental_load_windows(
    candidates: list[IncrementalLoadCandidate],
    *,
    parallel_slots: int = 2,
    window_minutes: int = 120,
) -> dict[str, Any]:
    """Schedule safe partitions without starting Spark or changing data."""
    if not candidates or not 1 <= parallel_slots <= 20 or window_minutes < 1:
        raise ValueError("candidates, slots, and window must be valid")
    names = [item.partition.strip() for item in candidates]
    if any(not name for name in names) or len(names) != len(set(names)):
        raise ValueError("partition names must be non-empty and unique")
    for item in candidates:
        if item.readiness not in {"READY", "AT_RISK", "BLOCKED"}:
            raise ValueError("unsupported partition readiness")
        if item.estimated_minutes < 1 or item.deadline_minute < 1:
            raise ValueError("duration and deadline must be positive")
        if not 1 <= item.priority <= 100:
            raise ValueError("priority must be between 1 and 100")

    ordered = sorted(
        candidates,
        key=lambda item: (
            {"READY": 0, "AT_RISK": 1, "BLOCKED": 2}[item.readiness],
            item.deadline_minute,
            -item.priority,
            item.partition,
        ),
    )
    slot_available = [0] * parallel_slots
    assignments = []
    deferred = []
    for item in ordered:
        base = {**asdict(item), "partition": item.partition.strip()}
        if item.readiness == "BLOCKED":
            deferred.append({**base, "reason": "PARTITION_NOT_READY"})
            continue
        slot = min(range(parallel_slots), key=lambda index: (slot_available[index], index))
        start = slot_available[slot]
        finish = start + item.estimated_minutes
        if finish > window_minutes:
            deferred.append({**base, "reason": "WINDOW_CAPACITY_EXCEEDED"})
            continue
        slot_available[slot] = finish
        assignments.append({
            **base,
            "slot": slot + 1,
            "start_minute": start,
            "finish_minute": finish,
            "deadline_margin_minutes": item.deadline_minute - finish,
            "status": "DEADLINE_RISK" if finish > item.deadline_minute else "PLANNED",
            "requires_confirmation": item.readiness == "AT_RISK",
        })

    return {
        "status": "INCREMENTAL_WINDOW_READY" if assignments else "NO_SAFE_LOAD_WINDOW",
        "candidate_count": len(candidates),
        "planned_count": len(assignments),
        "deferred_count": len(deferred),
        "parallel_slots": parallel_slots,
        "window_minutes": window_minutes,
        "slot_utilization_minutes": slot_available,
        "assignments": assignments,
        "deferred": deferred,
        "incremental_load_started": False,
    }
