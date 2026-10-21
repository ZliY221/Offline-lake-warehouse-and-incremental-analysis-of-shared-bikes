"""Cross-dataset release orchestration for offline data products."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class DatasetReleaseCandidate:
    dataset: str
    dependencies: tuple[str, ...] = ()
    quality_ready: bool = True
    consumer_ready: bool = True
    priority: int = 50
    estimated_compute_cost: float = 0.0


def plan_portfolio_release(candidates: list[DatasetReleaseCandidate], *,
                           portfolio_budget: float) -> dict[str, Any]:
    """Sequence interdependent datasets without applying any release action."""
    if not candidates or portfolio_budget <= 0:
        raise ValueError("candidates and portfolio budget are required")
    names = [item.dataset.strip() for item in candidates]
    if (any(not name for name in names)
            or any(item.dataset != item.dataset.strip() for item in candidates)
            or len(set(names)) != len(names)):
        raise ValueError("dataset names must be non-empty and unique")
    known = set(names)
    by_name = {item.dataset.strip(): item for item in candidates}
    for item in candidates:
        if not 1 <= item.priority <= 100 or item.estimated_compute_cost < 0:
            raise ValueError("priority or estimated cost is invalid")
        if (any(dep != dep.strip() for dep in item.dependencies)
                or item.dataset in item.dependencies
                or not set(item.dependencies) <= known):
            raise ValueError("dataset dependency is invalid")

    blocked: dict[str, list[str]] = {}
    for item in candidates:
        reasons = []
        if not item.quality_ready:
            reasons.append("quality gate is not ready")
        if not item.consumer_ready:
            reasons.append("consumer contract is not ready")
        if reasons:
            blocked[item.dataset] = reasons

    scheduled: list[str] = []
    scheduled_cost = 0.0
    remaining = set(names) - set(blocked)
    while remaining:
        ready = [by_name[name] for name in remaining
                 if set(by_name[name].dependencies) <= set(scheduled)]
        if not ready:
            for name in sorted(remaining):
                unavailable = [dep for dep in by_name[name].dependencies if dep not in scheduled]
                blocked[name] = ["dependency unavailable: " + ", ".join(unavailable)]
            break
        ready.sort(key=lambda item: (-item.priority, item.estimated_compute_cost, item.dataset))
        candidate = ready[0]
        remaining.remove(candidate.dataset)
        if scheduled_cost + candidate.estimated_compute_cost > portfolio_budget:
            blocked[candidate.dataset] = ["portfolio compute budget exceeded"]
            continue
        scheduled.append(candidate.dataset)
        scheduled_cost += candidate.estimated_compute_cost

    status = "READY" if not blocked else "PARTIAL" if scheduled else "BLOCKED"
    return {"status": status, "portfolio_budget": portfolio_budget,
            "scheduled_cost": round(scheduled_cost, 4),
            "remaining_budget": round(portfolio_budget - scheduled_cost, 4),
            "release_sequence": scheduled,
            "blocked": [{"dataset": name, "reasons": blocked[name]}
                        for name in sorted(blocked)],
            "release_actions_applied": False}
