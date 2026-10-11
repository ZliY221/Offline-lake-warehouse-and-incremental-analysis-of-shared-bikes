from __future__ import annotations

from datetime import date
import unittest

from bike_lakehouse.quality_report import evaluate_partition_freshness


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


if __name__ == "__main__":
    unittest.main()
