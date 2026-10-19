from datetime import datetime, timedelta, timezone

import pytest

from bike_lakehouse.backfill_approval import create_approval, validate_approval


def test_approval_is_bound_to_plan_and_source():
    now = datetime(2026, 10, 19, tzinfo=timezone.utc)
    approval = create_approval(plan_id="plan-1", source_sha256="abc", approved_by="operator", approved_at=now, expires_at=now + timedelta(hours=2))
    assert validate_approval(approval, plan_id="plan-1", source_sha256="abc", observed_at=now + timedelta(hours=1)) == (True, "APPROVED")
    assert validate_approval(approval, plan_id="plan-2", source_sha256="abc", observed_at=now) == (False, "PLAN_CHANGED")


def test_approval_expires_and_rejects_invalid_window():
    now = datetime(2026, 10, 19, tzinfo=timezone.utc)
    approval = create_approval(plan_id="plan-1", source_sha256="abc", approved_by="operator", approved_at=now, expires_at=now + timedelta(minutes=30))
    assert validate_approval(approval, plan_id="plan-1", source_sha256="abc", observed_at=now + timedelta(minutes=30)) == (False, "EXPIRED")
    with pytest.raises(ValueError):
        create_approval(plan_id="plan-1", source_sha256="abc", approved_by="operator", approved_at=now, expires_at=now)
