import unittest

from bike_lakehouse.backfill_lineage import assess_lineage_risk
from bike_lakehouse.backfill_plan import BackfillImpactPlan


def _plan(rows=10_000, cohorts=("2026-10-01",)):
    return BackfillImpactPlan("p", "2026-10-01", "trips.ndjson", "sha", 100, rows, rows, 10, 0, 0, 10, ("2026-10-01",), ("2026-10-01",), ("2026-10-01",), ("2026-10-01",), cohorts, 1, 1)


class BackfillLineageTests(unittest.TestCase):
    def test_lineage_risk_summarizes_downstream_impact(self):
        result = assess_lineage_risk(_plan(cohorts=("a", "b", "c", "d")))
        self.assertEqual(result.affected_partition_count, 8)
        self.assertEqual(result.risk_level, "MEDIUM")
        self.assertTrue(result.within_sla)

    def test_lineage_risk_flags_capacity_breach(self):
        result = assess_lineage_risk(_plan(rows=2_000_000), rows_per_minute=50_000, sla_minutes=30)
        self.assertEqual(result.estimated_minutes, 40)
        self.assertEqual(result.risk_level, "HIGH")
        self.assertFalse(result.within_sla)
