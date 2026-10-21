import unittest

from bike_lakehouse.partition_retention import (
    PartitionRetentionInput,
    plan_partition_retention,
)


class PartitionRetentionTests(unittest.TestCase):
    def test_plan_protects_newest_partitions_and_flags_old_personal_data(self):
        result = plan_partition_retention([
            PartitionRetentionInput("2025-01", 500, contains_personal_data=True),
            PartitionRetentionInput("2026-01", 200),
            PartitionRetentionInput("2026-09", 30),
            PartitionRetentionInput("2026-10", 5),
        ], minimum_online_partitions=2)
        actions = {item["partition"]: item["action"] for item in result["actions"]}
        self.assertEqual("ANONYMIZE_REVIEW", actions["2025-01"])
        self.assertEqual("ARCHIVE", actions["2026-01"])
        self.assertEqual("KEEP_ONLINE", actions["2026-10"])
        self.assertEqual("PRIVACY_REVIEW", result["status"])
        self.assertFalse(result["destructive_changes_applied"])

    def test_legal_hold_overrides_archive(self):
        result = plan_partition_retention([
            PartitionRetentionInput("2024-01", 900, legal_hold=True),
        ])
        self.assertEqual("HOLD", result["actions"][0]["action"])

    def test_plan_rejects_invalid_policy(self):
        with self.assertRaises(ValueError):
            plan_partition_retention([PartitionRetentionInput("2026-10", 1)],
                                     online_retention_days=400,
                                     personal_data_limit_days=365)
