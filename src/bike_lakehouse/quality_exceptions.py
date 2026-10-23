"""Govern temporary quality exceptions for offline datasets."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class QualityException:
    exception_id: str
    dataset: str
    check_name: str
    owner: str
    reason: str
    days_until_expiry: int
    affected_partition_count: int


def review_quality_exceptions(exceptions: list[QualityException], *,
                              due_soon_days: int = 7,
                              maximum_partitions: int = 31) -> dict[str, Any]:
    """Review temporary waivers without changing quality enforcement."""
    if not exceptions or due_soon_days < 0 or maximum_partitions < 1:
        raise ValueError("exceptions and valid review thresholds are required")
    ids = [item.exception_id.strip() for item in exceptions]
    if (any(not value for value in ids) or len(ids) != len(set(ids))
            or any(item.exception_id != item.exception_id.strip() for item in exceptions)):
        raise ValueError("exception ids must be non-empty and unique")
    rows = []
    for item in sorted(exceptions, key=lambda row: (row.days_until_expiry, row.exception_id)):
        if (any(not value.strip() or value != value.strip()
                for value in (item.dataset, item.check_name, item.owner, item.reason))
                or item.affected_partition_count < 1):
            raise ValueError("quality exception is invalid")
        reasons = []
        if item.days_until_expiry < 0:
            reasons.append("exception has expired")
        elif item.days_until_expiry <= due_soon_days:
            reasons.append("exception expires soon")
        if item.affected_partition_count > maximum_partitions:
            reasons.append("exception scope exceeds partition limit")
        status = ("EXPIRED" if item.days_until_expiry < 0 else
                  "REVIEW" if reasons else "ACTIVE")
        rows.append({"exception_id": item.exception_id, "dataset": item.dataset,
                     "check_name": item.check_name, "owner": item.owner,
                     "days_until_expiry": item.days_until_expiry,
                     "affected_partition_count": item.affected_partition_count,
                     "status": status, "review_reasons": reasons})
    return {"status": "ACTION_REQUIRED" if any(row["status"] != "ACTIVE" for row in rows)
            else "CURRENT", "exception_count": len(rows),
            "expired_count": sum(row["status"] == "EXPIRED" for row in rows),
            "review_count": sum(row["status"] == "REVIEW" for row in rows),
            "exceptions": rows, "quality_enforcement_changed": False}
