"""Review records that bind an approval to one immutable backfill plan."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib


@dataclass(frozen=True)
class BackfillApproval:
    approval_id: str
    plan_id: str
    source_sha256: str
    approved_by: str
    approved_at_utc: str
    expires_at_utc: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


def create_approval(*, plan_id: str, source_sha256: str, approved_by: str,
                    approved_at: datetime, expires_at: datetime) -> BackfillApproval:
    if not plan_id or not source_sha256 or not approved_by.strip():
        raise ValueError("plan_id, source_sha256 and approved_by are required")
    if approved_at.tzinfo is None or expires_at.tzinfo is None:
        raise ValueError("approval timestamps must be timezone-aware")
    if expires_at <= approved_at:
        raise ValueError("approval expiry must be after approval time")
    identity = f"{plan_id}:{source_sha256}:{approved_by.strip()}:{expires_at.isoformat()}"
    return BackfillApproval(
        approval_id="approval-" + hashlib.sha256(identity.encode()).hexdigest()[:20],
        plan_id=plan_id, source_sha256=source_sha256, approved_by=approved_by.strip(),
        approved_at_utc=approved_at.astimezone(timezone.utc).isoformat(),
        expires_at_utc=expires_at.astimezone(timezone.utc).isoformat(),
    )


def validate_approval(approval: BackfillApproval, *, plan_id: str,
                      source_sha256: str, observed_at: datetime) -> tuple[bool, str]:
    if approval.plan_id != plan_id or approval.source_sha256 != source_sha256:
        return False, "PLAN_CHANGED"
    if observed_at.tzinfo is None:
        raise ValueError("observed_at must be timezone-aware")
    if observed_at.astimezone(timezone.utc) >= datetime.fromisoformat(approval.expires_at_utc):
        return False, "EXPIRED"
    return True, "APPROVED"
