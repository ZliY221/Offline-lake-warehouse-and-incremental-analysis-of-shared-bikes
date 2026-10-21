import unittest

from bike_lakehouse.portfolio_release import DatasetReleaseCandidate, plan_portfolio_release


class PortfolioReleaseTests(unittest.TestCase):
    def test_plan_orders_dependencies_and_blocks_budget_overflow(self):
        result = plan_portfolio_release([
            DatasetReleaseCandidate("bronze", priority=100, estimated_compute_cost=10),
            DatasetReleaseCandidate("silver", ("bronze",), priority=90,
                                    estimated_compute_cost=20),
            DatasetReleaseCandidate("gold", ("silver",), priority=80,
                                    estimated_compute_cost=30),
            DatasetReleaseCandidate("catalog", priority=70, estimated_compute_cost=5),
        ], portfolio_budget=40)
        self.assertEqual(["bronze", "silver", "catalog"], result["release_sequence"])
        self.assertEqual("PARTIAL", result["status"])
        self.assertEqual("gold", result["blocked"][0]["dataset"])
        self.assertFalse(result["release_actions_applied"])

    def test_unready_dependency_blocks_downstream_dataset(self):
        result = plan_portfolio_release([
            DatasetReleaseCandidate("silver", quality_ready=False),
            DatasetReleaseCandidate("gold", ("silver",)),
        ], portfolio_budget=100)
        self.assertEqual("BLOCKED", result["status"])
        self.assertEqual(2, len(result["blocked"]))

    def test_unknown_dependency_is_rejected(self):
        with self.assertRaises(ValueError):
            plan_portfolio_release([
                DatasetReleaseCandidate("gold", ("missing",)),
            ], portfolio_budget=100)
