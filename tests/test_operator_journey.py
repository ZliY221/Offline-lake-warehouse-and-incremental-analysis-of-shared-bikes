import unittest

from bike_lakehouse.operator_journey import (
    OfflineOperatorJourney,
    rehearse_operator_journeys,
    standard_operator_journeys,
)


class OperatorJourneyTests(unittest.TestCase):
    def test_one_hundred_journeys_all_have_a_recovery_path(self):
        result = rehearse_operator_journeys(standard_operator_journeys())
        self.assertEqual("USABILITY_REHEARSAL_PASSED", result["status"])
        self.assertEqual(100, result["journey_count"])
        self.assertEqual(0, result["unresolved_count"])
        self.assertEqual(50, result["ready_count"])
        self.assertTrue(all(row["primary_action"] for row in result["journeys"]))
        self.assertFalse(result["operator_action_started"])

    def test_blocked_runtime_gets_preflight_first(self):
        journeys = standard_operator_journeys()
        result = rehearse_operator_journeys(journeys)
        self.assertEqual("RUN_ENVIRONMENT_PREFLIGHT", result["journeys"][0]["primary_action"])
        self.assertEqual("BLOCKED", result["journeys"][0]["status"])

    def test_requires_exactly_one_hundred_unique_journeys(self):
        with self.assertRaises(ValueError):
            rehearse_operator_journeys(standard_operator_journeys()[:99])
        duplicate = standard_operator_journeys()
        duplicate[-1] = OfflineOperatorJourney(
            "offline-001", "RELEASE", True, True, True, True, True
        )
        with self.assertRaises(ValueError):
            rehearse_operator_journeys(duplicate)
