"""Explain the operational value of a lakehouse compaction plan."""

from __future__ import annotations

from dataclasses import asdict, dataclass

from .compaction import CompactionPlan


@dataclass(frozen=True)
class StorageAdvice:
    action_count: int
    current_file_count: int
    target_file_count: int
    removable_file_count: int
    estimated_open_cost_ms_saved: int
    recommendation: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def advise_compaction(plan: CompactionPlan, *, open_cost_ms: int = 8) -> StorageAdvice:
    if open_cost_ms < 0:
        raise ValueError("open_cost_ms cannot be negative")
    current = sum(action.input_file_count for action in plan.actions)
    target = sum(action.target_file_count for action in plan.actions)
    removable = max(0, current - target)
    saved = removable * open_cost_ms
    recommendation = "COMPACT" if removable else "KEEP"
    return StorageAdvice(len(plan.actions), current, target, removable, saved, recommendation)
