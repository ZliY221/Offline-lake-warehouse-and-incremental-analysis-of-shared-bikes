import unittest

from bike_lakehouse.compaction import CompactionAction, CompactionPlan
from bike_lakehouse.storage_advisor import advise_compaction


class StorageAdvisorTests(unittest.TestCase):
    def test_advisor_quantifies_small_file_reduction(self):
        action = CompactionAction("a", "date=1", 12, 1200, 2, "sha")
        plan = CompactionPlan("1", "now", "trips", "date", 128, 4, (action,))
        advice = advise_compaction(plan, open_cost_ms=10)
        self.assertEqual(advice.removable_file_count, 10)
        self.assertEqual(advice.estimated_open_cost_ms_saved, 100)
        self.assertEqual(advice.recommendation, "COMPACT")

    def test_advisor_handles_empty_plan(self):
        plan = CompactionPlan("1", "now", "trips", "date", 128, 4, ())
        self.assertEqual(advise_compaction(plan).recommendation, "KEEP")
        with self.assertRaises(ValueError):
            advise_compaction(plan, open_cost_ms=-1)
