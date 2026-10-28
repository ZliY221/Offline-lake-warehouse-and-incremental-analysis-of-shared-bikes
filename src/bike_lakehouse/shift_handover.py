"""Build a bounded handover packet for unfinished offline lakehouse work."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class HandoverWorkItem:
    item_id: str
    lane: str
    status: str
    owner: str
    updated_at: str
    resume_key: str
    next_action: str
    estimated_minutes: int
    requires_confirmation: bool = False


def build_shift_handover(
    items: list[HandoverWorkItem],
    *,
    generated_at: str,
    focus_limit: int = 12,
    stale_after_minutes: int = 120,
) -> dict[str, Any]:
    """Prioritize unfinished work without executing tasks or changing data."""
    if not items or not 1 <= focus_limit <= 25 or stale_after_minutes < 1:
        raise ValueError("items, focus limit, and stale threshold must be valid")
    now = _timestamp(generated_at)
    identifiers = [item.item_id.strip() for item in items]
    if any(not value for value in identifiers) or len(identifiers) != len(set(identifiers)):
        raise ValueError("item ids must be non-empty and unique")

    lane_order = {"ENVIRONMENT": 0, "SOURCE": 1, "PIPELINE": 2, "QUALITY": 3, "DELIVERY": 4}
    status_order = {"BLOCKED": 0, "IN_PROGRESS": 1, "OPEN": 2, "RESOLVED": 3}
    prepared: list[dict[str, Any]] = []
    for item in items:
        if item.lane not in lane_order or item.status not in status_order:
            raise ValueError("unsupported lane or status")
        if not item.owner.strip() or not item.resume_key.strip() or not item.next_action.strip():
            raise ValueError("owner, resume key, and next action are required")
        if item.estimated_minutes < 1 or item.estimated_minutes > 480:
            raise ValueError("estimated minutes must be between 1 and 480")
        updated = _timestamp(item.updated_at)
        age_minutes = int((now - updated).total_seconds() // 60)
        if age_minutes < 0:
            raise ValueError("updated_at must not be later than generated_at")
        if item.status == "RESOLVED":
            continue
        prepared.append({
            **asdict(item),
            "item_id": item.item_id.strip(),
            "owner": item.owner.strip(),
            "resume_key": item.resume_key.strip(),
            "next_action": item.next_action.strip(),
            "age_minutes": age_minutes,
            "stale": age_minutes >= stale_after_minutes,
        })

    prepared.sort(key=lambda item: (
        status_order[item["status"]],
        0 if item["stale"] else 1,
        lane_order[item["lane"]],
        -item["age_minutes"],
        item["item_id"],
    ))
    focus = []
    for rank, item in enumerate(prepared[:focus_limit], start=1):
        focus.append({**item, "rank": rank})
    owner_counts: dict[str, int] = {}
    lane_counts: dict[str, int] = {}
    for item in prepared:
        owner_counts[item["owner"]] = owner_counts.get(item["owner"], 0) + 1
        lane_counts[item["lane"]] = lane_counts.get(item["lane"], 0) + 1
    blocked = sum(item["status"] == "BLOCKED" for item in prepared)
    stale = sum(item["stale"] for item in prepared)
    return {
        "status": "HANDOVER_ATTENTION" if blocked or stale else "HANDOVER_READY",
        "generated_at": generated_at,
        "unfinished_count": len(prepared),
        "visible_count": len(focus),
        "remaining_count": max(0, len(prepared) - len(focus)),
        "blocked_count": blocked,
        "stale_count": stale,
        "estimated_focus_minutes": sum(item["estimated_minutes"] for item in focus),
        "lane_counts": dict(sorted(lane_counts.items())),
        "owner_counts": dict(sorted(owner_counts.items())),
        "focus_items": focus,
        "guidance": "先交接阻断和长时间未更新事项，再继续进行中工作，最后领取新任务。",
        "work_started": False,
    }


def _timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError("timestamps must use ISO 8601") from error
    if parsed.tzinfo is None:
        raise ValueError("timestamps must include a timezone")
    return parsed
