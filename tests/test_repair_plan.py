from __future__ import annotations

from datetime import date
import unittest

from bike_lakehouse.repair_plan import audit_repair_plan, build_repair_plan


SHA_A = "a" * 64
SHA_B = "b" * 64


class RepairPlanTests(unittest.TestCase):
    def test_plan_distinguishes_ready_and_blocked_dates(self) -> None:
        plan = build_repair_plan(
            {"2026-10-01", "2026-10-04"},
            date(2026, 10, 1),
            date(2026, 10, 4),
            [{"ingestion_date": "2026-10-02", "source_name": "trips-2026-10-02.ndjson", "source_sha256": SHA_A}],
        )
        self.assertEqual(2, plan["missing_partition_count"])
        self.assertEqual(1, plan["ready_action_count"])
        self.assertEqual(1, plan["blocked_action_count"])
        self.assertEqual(["READY", "BLOCKED_SOURCE_MISSING"], [row["status"] for row in plan["actions"]])

    def test_plan_is_deterministic_and_rejects_unsafe_catalog_entries(self) -> None:
        arguments = (
            {"2026-10-01"}, date(2026, 10, 1), date(2026, 10, 2),
            [{"ingestion_date": "2026-10-02", "source_name": "day.ndjson", "source_sha256": SHA_A}],
        )
        self.assertEqual(build_repair_plan(*arguments), build_repair_plan(*arguments))
        with self.assertRaisesRegex(ValueError, "file name"):
            build_repair_plan(
                {"2026-10-01"}, date(2026, 10, 1), date(2026, 10, 2),
                [{"ingestion_date": "2026-10-02", "source_name": "../day.ndjson", "source_sha256": SHA_A}],
            )

    def test_audit_requires_partition_and_matching_success_manifest(self) -> None:
        plan = build_repair_plan(
            {"2026-10-01"}, date(2026, 10, 1), date(2026, 10, 2),
            [{"ingestion_date": "2026-10-02", "source_name": "day.ndjson", "source_sha256": SHA_A}],
        )
        complete = audit_repair_plan(
            plan, {"2026-10-01", "2026-10-02"},
            [{"pipeline": "trip_date_backfill", "status": "SUCCEEDED", "target_date": "2026-10-02", "source_sha256": SHA_A}],
        )
        wrong_source = audit_repair_plan(
            plan, {"2026-10-01", "2026-10-02"},
            [{"pipeline": "trip_date_backfill", "status": "SUCCEEDED", "target_date": "2026-10-02", "source_sha256": SHA_B}],
        )
        self.assertEqual("COMPLETE", complete["status"])
        self.assertEqual("INCOMPLETE", wrong_source["status"])


if __name__ == "__main__":
    unittest.main()
