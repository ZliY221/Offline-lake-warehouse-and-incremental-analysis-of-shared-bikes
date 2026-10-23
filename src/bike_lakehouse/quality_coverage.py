"""Portfolio quality-control coverage assessment for offline datasets."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class DatasetQualityCoverage:
    dataset: str
    critical_fields: tuple[str, ...]
    validated_fields: tuple[str, ...]
    freshness_sla_defined: bool
    lineage_registered: bool


def assess_quality_coverage(datasets: list[DatasetQualityCoverage]) -> dict[str, Any]:
    """Expose quality-control gaps without changing rules or data."""
    if not datasets:
        raise ValueError("dataset coverage is required")
    names = [item.dataset.strip() for item in datasets]
    if (any(not name for name in names)
            or any(item.dataset != item.dataset.strip() for item in datasets)
            or len(names) != len(set(names))):
        raise ValueError("dataset names must be non-empty and unique")

    rows = []
    for item in sorted(datasets, key=lambda row: row.dataset):
        critical = list(item.critical_fields)
        validated = list(item.validated_fields)
        if (not critical or len(critical) != len(set(critical))
                or len(validated) != len(set(validated))
                or any(not field.strip() or field != field.strip()
                       for field in critical + validated)
                or not set(validated) <= set(critical)):
            raise ValueError("quality coverage fields are invalid")
        missing = sorted(set(critical) - set(validated))
        checks = len(validated) + int(item.freshness_sla_defined) + int(item.lineage_registered)
        total = len(critical) + 2
        ratio = checks / total
        blockers = []
        if missing:
            blockers.append("critical fields lack validation")
        if not item.freshness_sla_defined:
            blockers.append("freshness SLA is missing")
        if not item.lineage_registered:
            blockers.append("lineage registration is missing")
        status = "BLOCKED" if missing else "REVIEW" if blockers else "READY"
        rows.append({"dataset": item.dataset, "status": status,
                     "coverage_ratio": ratio, "covered_check_count": checks,
                     "required_check_count": total,
                     "missing_critical_fields": missing, "gaps": blockers})
    blocked = sum(row["status"] == "BLOCKED" for row in rows)
    review = sum(row["status"] == "REVIEW" for row in rows)
    status = "BLOCKED" if blocked else "REVIEW" if review else "READY"
    return {"status": status, "dataset_count": len(rows),
            "blocked_dataset_count": blocked, "review_dataset_count": review,
            "datasets": rows, "quality_rules_changed": False}
