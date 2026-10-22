import unittest

from bike_lakehouse.quality_evidence_sampling import (
    PartitionEvidenceCandidate,
    plan_quality_evidence_samples,
)


class QualityEvidenceSamplingTests(unittest.TestCase):
    def test_allocates_minimum_and_prioritises_risk(self):
        result = plan_quality_evidence_samples([
            PartitionEvidenceCandidate("2026-10-20", 100, 0.1, 12),
            PartitionEvidenceCandidate("2026-10-21", 100, 0.9, 48),
        ], sample_budget=20, minimum_per_partition=2)
        counts = {row["partition"]: row["sample_count"] for row in result["samples"]}
        self.assertEqual(20, result["allocated_samples"])
        self.assertGreater(counts["2026-10-21"], counts["2026-10-20"])
        self.assertFalse(result["sampling_started"])

    def test_empty_partition_does_not_consume_budget(self):
        result = plan_quality_evidence_samples([
            PartitionEvidenceCandidate("empty", 0, 1.0, 200),
            PartitionEvidenceCandidate("ready", 3, 0.2, 1),
        ], sample_budget=10)
        self.assertEqual(3, result["allocated_samples"])
        self.assertEqual(7, result["unused_budget"])

    def test_rejects_budget_below_partition_minimum(self):
        with self.assertRaises(ValueError):
            plan_quality_evidence_samples([
                PartitionEvidenceCandidate("a", 10, 0.2, 1),
                PartitionEvidenceCandidate("b", 10, 0.2, 1),
            ], sample_budget=1)
