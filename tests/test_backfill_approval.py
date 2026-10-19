from datetime import datetime, timedelta, timezone
import unittest

from bike_lakehouse.backfill_approval import create_approval, validate_approval


class BackfillApprovalTests(unittest.TestCase):
    def test_approval_is_bound_to_plan_and_source(self):
        now = datetime(2026, 10, 19, tzinfo=timezone.utc)
        approval = create_approval(plan_id="plan-1", source_sha256="abc", approved_by="operator", approved_at=now, expires_at=now + timedelta(hours=2))
        self.assertEqual(validate_approval(approval, plan_id="plan-1", source_sha256="abc", observed_at=now + timedelta(hours=1)), (True, "APPROVED"))
        self.assertEqual(validate_approval(approval, plan_id="plan-2", source_sha256="abc", observed_at=now), (False, "PLAN_CHANGED"))

    def test_approval_expires_and_rejects_invalid_window(self):
        now = datetime(2026, 10, 19, tzinfo=timezone.utc)
        approval = create_approval(plan_id="plan-1", source_sha256="abc", approved_by="operator", approved_at=now, expires_at=now + timedelta(minutes=30))
        self.assertEqual(validate_approval(approval, plan_id="plan-1", source_sha256="abc", observed_at=now + timedelta(minutes=30)), (False, "EXPIRED"))
        with self.assertRaises(ValueError):
            create_approval(plan_id="plan-1", source_sha256="abc", approved_by="operator", approved_at=now, expires_at=now)
