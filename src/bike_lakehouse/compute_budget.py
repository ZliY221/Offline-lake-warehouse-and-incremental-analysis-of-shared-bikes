"""Pre-run compute budget planning for offline lakehouse jobs."""

from __future__ import annotations

from typing import Any


def plan_compute_budget(*, input_gb: float, executor_count: int,
                        runtime_minutes: int, executor_hour_cost: float,
                        budget_limit: float, shuffle_multiplier: float = 1.0) -> dict[str, Any]:
    """Estimate a batch cost and expose a safe runtime boundary before execution."""
    if input_gb <= 0 or executor_count < 1 or runtime_minutes < 1:
        raise ValueError("input, executor count and runtime must be positive")
    if executor_hour_cost < 0 or budget_limit <= 0 or shuffle_multiplier < 1:
        raise ValueError("cost, budget or shuffle multiplier is invalid")
    executor_hours = executor_count * runtime_minutes / 60
    shuffle_factor = 1 + (shuffle_multiplier - 1) * 0.2
    projected_cost = executor_hours * executor_hour_cost * shuffle_factor
    utilization = projected_cost / budget_limit
    maximum_runtime = int(budget_limit / (executor_count * executor_hour_cost * shuffle_factor) * 60) if executor_hour_cost else None
    status = "OVER_BUDGET" if utilization > 1 else "NEAR_LIMIT" if utilization >= 0.8 else "WITHIN_BUDGET"
    actions = []
    if status == "OVER_BUDGET":
        actions.append("Reduce executors or split the input into smaller partitions")
    if shuffle_multiplier >= 2:
        actions.append("Review joins and repartitioning before scheduling")
    if not actions:
        actions.append("Schedule the batch inside the approved compute window")
    return {
        "input_gb": input_gb,
        "executor_count": executor_count,
        "runtime_minutes": runtime_minutes,
        "executor_hours": round(executor_hours, 4),
        "shuffle_multiplier": shuffle_multiplier,
        "projected_cost": round(projected_cost, 4),
        "budget_limit": budget_limit,
        "budget_utilization": round(utilization, 6),
        "cost_per_input_gb": round(projected_cost / input_gb, 6),
        "maximum_runtime_minutes": maximum_runtime,
        "status": status,
        "actions": actions,
    }
