"""Plan retention actions for offline lakehouse partitions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class PartitionRetentionInput:
    partition: str
    age_days: int
    contains_personal_data: bool = False
    legal_hold: bool = False


def plan_partition_retention(
    partitions: list[PartitionRetentionInput], *, online_retention_days: int = 90,
    personal_data_limit_days: int = 365, minimum_online_partitions: int = 3,
) -> dict[str, Any]:
    """Create a non-destructive archive and privacy review plan."""
    if not partitions:
        raise ValueError("at least one partition is required")
    if online_retention_days < 1 or personal_data_limit_days < online_retention_days:
        raise ValueError("retention boundaries are invalid")
    if minimum_online_partitions < 1:
        raise ValueError("minimum_online_partitions must be positive")
    if any(item.age_days < 0 or not item.partition.strip() for item in partitions):
        raise ValueError("partition identity and age must be valid")

    newest = sorted(partitions, key=lambda item: (item.age_days, item.partition))
    protected = {item.partition for item in newest[:minimum_online_partitions]}
    actions: list[dict[str, Any]] = []
    for item in sorted(partitions, key=lambda value: value.partition):
        if item.legal_hold:
            action, reason = "HOLD", "legal hold prevents lifecycle movement"
        elif item.contains_personal_data and item.age_days > personal_data_limit_days:
            action, reason = "ANONYMIZE_REVIEW", "personal data exceeds the configured limit"
        elif item.age_days > online_retention_days and item.partition not in protected:
            action, reason = "ARCHIVE", "partition exceeds online retention"
        else:
            action, reason = "KEEP_ONLINE", "partition is inside the active recovery window"
        actions.append({"partition": item.partition, "age_days": item.age_days,
                        "action": action, "reason": reason})

    counts = {name: sum(row["action"] == name for row in actions)
              for name in ("KEEP_ONLINE", "ARCHIVE", "ANONYMIZE_REVIEW", "HOLD")}
    status = "PRIVACY_REVIEW" if counts["ANONYMIZE_REVIEW"] else "READY"
    return {"status": status, "minimum_online_partitions": minimum_online_partitions,
            "online_retention_days": online_retention_days,
            "personal_data_limit_days": personal_data_limit_days,
            "counts": counts, "actions": actions, "destructive_changes_applied": False}
