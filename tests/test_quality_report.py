from __future__ import annotations

from datetime import date
import unittest

from bike_lakehouse.quality_report import (
    evaluate_partition_continuity,
    evaluate_partition_freshness,
)


class PartitionFreshnessTests(unittest.TestCase):
    def test_latest_partition_meets_required_date(self) -> None:
        check = evaluate_partition_freshness(
            {"2026-09-30", "2026-10-01"}, date(2026, 10, 1)
        )

        self.assertEqual("PASS", check["status"])
        self.assertEqual("2026-10-01", check["observed_latest_ingestion_date"])
        self.assertEqual(2, check["observed_partition_count"])

    def test_stale_or_empty_partitions_fail(self) -> None:
        stale = evaluate_partition_freshness({"2026-09-30"}, date(2026, 10, 1))
        empty = evaluate_partition_freshness(set(), date(2026, 10, 1))

        self.assertEqual("FAIL", stale["status"])
        self.assertEqual("FAIL", empty["status"])
        self.assertIsNone(empty["observed_latest_ingestion_date"])

    def test_invalid_partition_value_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "invalid Bronze ingestion_date"):
            evaluate_partition_freshness({"2026-10-99"}, date(2026, 10, 1))


class PartitionContinuityTests(unittest.TestCase):
    def test_complete_required_range_passes(self) -> None:
        check = evaluate_partition_continuity(
            {"2026-10-01", "2026-10-02", "2026-10-03"},
            date(2026, 10, 1),
            date(2026, 10, 3),
        )

        self.assertEqual("PASS", check["status"])
        self.assertEqual(3, check["expected_partition_count"])
        self.assertEqual([], check["missing_ingestion_dates"])

    def test_missing_dates_are_reported_in_order(self) -> None:
        check = evaluate_partition_continuity(
            {"2026-10-01", "2026-10-04"},
            date(2026, 10, 1),
            date(2026, 10, 4),
        )

        self.assertEqual("FAIL", check["status"])
        self.assertEqual(2, check["missing_partition_count"])
        self.assertEqual(["2026-10-02", "2026-10-03"], check["missing_ingestion_dates"])

    def test_invalid_or_unbounded_ranges_are_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "must not be later"):
            evaluate_partition_continuity(set(), date(2026, 10, 2), date(2026, 10, 1))
        with self.assertRaisesRegex(ValueError, "366 days"):
            evaluate_partition_continuity(set(), date(2025, 1, 1), date(2026, 1, 2))


if __name__ == "__main__":
    unittest.main()
