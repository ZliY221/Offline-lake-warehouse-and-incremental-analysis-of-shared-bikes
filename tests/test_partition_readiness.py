import unittest

from bike_lakehouse.partition_readiness import (
    PartitionReadinessInput,
    assess_partition_readiness,
)


class PartitionReadinessTests(unittest.TestCase):
    def test_ready_at_risk_and_blocked_partitions_are_distinguished(self) -> None:
        result = assess_partition_readiness([
            PartitionReadinessInput("2026-10-24", 100, 100, True, True, True, 5),
            PartitionReadinessInput("2026-10-25", 100, 99, True, True, True, 45),
            PartitionReadinessInput("2026-10-26", 100, 80, False, True, True, 10),
        ])
        self.assertEqual("PARTITION_READINESS_BLOCKED", result["status"])
        self.assertEqual({"ready": 1, "at_risk": 1, "blocked": 1}, result["counts"])
        self.assertEqual("CONFIRM_LATE_ARRIVAL_WINDOW", result["partitions"][1]["next_action"])
        self.assertIn("UPSTREAM_NOT_COMPLETE", result["partitions"][2]["blockers"])
        self.assertFalse(result["incremental_load_started"])

    def test_missing_contract_and_checksum_block_the_load(self) -> None:
        result = assess_partition_readiness([
            PartitionReadinessInput("2026-10-26", 20, 20, True, False, False)
        ])
        self.assertEqual(
            ["CONTRACT_INVALID", "CHECKSUM_MISSING"], result["partitions"][0]["blockers"]
        )

    def test_invalid_thresholds_and_duplicate_partitions_are_rejected(self) -> None:
        item = PartitionReadinessInput("p1", 10, 10, True, True, True)
        with self.assertRaises(ValueError):
            assess_partition_readiness([item], minimum_completeness=0)
        with self.assertRaises(ValueError):
            assess_partition_readiness([item, item])
