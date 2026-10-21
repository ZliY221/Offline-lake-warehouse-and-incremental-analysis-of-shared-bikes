import unittest

from bike_lakehouse.compute_budget import plan_compute_budget


class ComputeBudgetTests(unittest.TestCase):
    def test_plan_flags_expensive_shuffle_before_execution(self):
        result = plan_compute_budget(
            input_gb=500, executor_count=20, runtime_minutes=180,
            executor_hour_cost=1.5, budget_limit=80, shuffle_multiplier=2,
        )
        self.assertEqual("OVER_BUDGET", result["status"])
        self.assertEqual(2, len(result["actions"]))
        self.assertLess(result["maximum_runtime_minutes"], 180)

    def test_plan_keeps_small_batch_inside_budget(self):
        result = plan_compute_budget(
            input_gb=50, executor_count=4, runtime_minutes=30,
            executor_hour_cost=1, budget_limit=20,
        )
        self.assertEqual("WITHIN_BUDGET", result["status"])
        self.assertEqual(0.04, result["cost_per_input_gb"])

    def test_plan_rejects_invalid_capacity(self):
        with self.assertRaises(ValueError):
            plan_compute_budget(input_gb=0, executor_count=1, runtime_minutes=1,
                                executor_hour_cost=1, budget_limit=1)
