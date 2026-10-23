import unittest

from bike_lakehouse.quality_exceptions import QualityException, review_quality_exceptions


class QualityExceptionTests(unittest.TestCase):
    def test_flags_expired_and_overbroad_exceptions(self):
        result = review_quality_exceptions([
            QualityException("e-1", "silver", "station_exists", "data-owner",
                             "source repair pending", -1, 2),
            QualityException("e-2", "bronze", "row_count", "platform-owner",
                             "historical import", 20, 60),
        ])
        self.assertEqual("ACTION_REQUIRED", result["status"])
        self.assertEqual(1, result["expired_count"])
        self.assertEqual("REVIEW", result["exceptions"][1]["status"])
        self.assertFalse(result["quality_enforcement_changed"])

    def test_current_exception_needs_no_action(self):
        result = review_quality_exceptions([
            QualityException("e-1", "silver", "distance", "owner", "known gap", 30, 3),
        ])
        self.assertEqual("CURRENT", result["status"])

    def test_rejects_duplicate_exception_id(self):
        with self.assertRaises(ValueError):
            review_quality_exceptions([
                QualityException("e", "a", "c", "o", "r", 1, 1),
                QualityException("e", "b", "c", "o", "r", 1, 1),
            ])
