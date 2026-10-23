import unittest

from bike_lakehouse.quality_coverage import DatasetQualityCoverage, assess_quality_coverage


class QualityCoverageTests(unittest.TestCase):
    def test_reports_field_and_operational_gaps(self):
        result = assess_quality_coverage([
            DatasetQualityCoverage("gold_trips", ("trip_id", "duration"),
                                   ("trip_id",), False, True),
        ])
        row = result["datasets"][0]
        self.assertEqual("BLOCKED", result["status"])
        self.assertEqual(["duration"], row["missing_critical_fields"])
        self.assertIn("freshness SLA is missing", row["gaps"])
        self.assertFalse(result["quality_rules_changed"])

    def test_ready_portfolio_has_complete_coverage(self):
        result = assess_quality_coverage([
            DatasetQualityCoverage("silver", ("trip_id",), ("trip_id",), True, True),
            DatasetQualityCoverage("gold", ("date",), ("date",), True, True),
        ])
        self.assertEqual("READY", result["status"])
        self.assertEqual(1.0, result["datasets"][0]["coverage_ratio"])

    def test_rejects_validated_field_outside_critical_set(self):
        with self.assertRaises(ValueError):
            assess_quality_coverage([
                DatasetQualityCoverage("silver", ("trip_id",), ("other",), True, True),
            ])
