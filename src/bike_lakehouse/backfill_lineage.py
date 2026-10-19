"""Lineage and service-level risk summary for a backfill impact plan."""

from __future__ import annotations

from dataclasses import asdict, dataclass

from .backfill_plan import BackfillImpactPlan


@dataclass(frozen=True)
class BackfillLineageRisk:
    risk_level: str
    affected_datasets: tuple[str, ...]
    affected_partition_count: int
    estimated_minutes: int
    sla_minutes: int
    within_sla: bool

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def assess_lineage_risk(plan: BackfillImpactPlan, *, rows_per_minute: int = 50_000,
                        sla_minutes: int = 30) -> BackfillLineageRisk:
    if rows_per_minute <= 0 or sla_minutes <= 0:
        raise ValueError("capacity and SLA must be positive")
    datasets = ("bronze.trips", "silver.trips_valid", "gold.daily_metrics",
                "gold.popular_routes", "gold.cohort_retention")
    partition_count = sum(map(len, (
        plan.affected_bronze_partitions, plan.affected_silver_partitions,
        plan.affected_gold_daily_metric_partitions,
        plan.affected_gold_popular_route_partitions,
        plan.potential_gold_cohort_partitions,
    )))
    estimated = max(1, (plan.source_rows + rows_per_minute - 1) // rows_per_minute)
    within_sla = estimated <= sla_minutes
    risk = "HIGH" if not within_sla else "MEDIUM" if partition_count >= 8 else "LOW"
    return BackfillLineageRisk(risk, datasets, partition_count, estimated, sla_minutes, within_sla)
