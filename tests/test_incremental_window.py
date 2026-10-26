import unittest

from bike_lakehouse.incremental_window import (
    IncrementalLoadCandidate,
    plan_incremental_load_windows,
)


class IncrementalWindowTests(unittest.TestCase):
    def test_plan_uses_parallel_slots_and_defers_blocked_partition(self) -> None:
        result = plan_incremental_load_windows([
            IncrementalLoadCandidate("p1", "READY", 40, 60, 90),
            IncrementalLoadCandidate("p2", "READY", 30, 50, 80),
            IncrementalLoadCandidate("p3", "AT_RISK", 20, 90, 70),
            IncrementalLoadCandidate("p4", "BLOCKED", 10, 30, 100),
        ], parallel_slots=2, window_minutes=60)
        self.assertEqual("INCREMENTAL_WINDOW_READY", result["status"])
        self.assertEqual(3, result["planned_count"])
        self.assertEqual("PARTITION_NOT_READY", result["deferred"][0]["reason"])
        self.assertTrue(result["assignments"][2]["requires_confirmation"])
        self.assertFalse(result["incremental_load_started"])

    def test_capacity_excess_is_deferred(self) -> None:
        result = plan_incremental_load_windows([
            IncrementalLoadCandidate("p1", "READY", 40, 50, 90),
            IncrementalLoadCandidate("p2", "READY", 40, 80, 80),
        ], parallel_slots=1, window_minutes=60)
        self.assertEqual(1, result["planned_count"])
        self.assertEqual("WINDOW_CAPACITY_EXCEEDED", result["deferred"][0]["reason"])

    def test_duplicate_partition_is_rejected(self) -> None:
        item = IncrementalLoadCandidate("p1", "READY", 10, 20, 50)
        with self.assertRaises(ValueError):
            plan_incremental_load_windows([item, item])
