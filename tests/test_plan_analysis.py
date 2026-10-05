from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from bike_lakehouse.plan_analysis import build_plan_analysis, normalize_plan, plan_features
from bike_lakehouse.spark import create_local_spark


class PlanAnalysisTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark_temp = tempfile.TemporaryDirectory()
        cls.spark = create_local_spark(
            "bike-trip-plan-analysis-tests",
            Path(cls.spark_temp.name) / "warehouse",
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()
        cls.spark_temp.cleanup()

    def test_normalization_and_feature_counts_are_deterministic(self) -> None:
        normalized = normalize_plan("SortMergeJoin [id#12L], plan_id=987")
        self.assertEqual(normalized, "SortMergeJoin [id#<id>], plan_id=<id>")
        self.assertEqual(plan_features(normalized)["SortMergeJoin"], 1)

    def test_real_plans_change_strategy_without_changing_results(self) -> None:
        report = build_plan_analysis(self.spark, fact_rows=256)
        baseline = report["baseline_sort_merge"]
        broadcasted = report["explicit_broadcast"]
        adaptive = report["adaptive_aggregation"]
        skew_baseline = report["skew_baseline"]
        adaptive_skew = report["adaptive_skew_join"]
        self.assertEqual(baseline["result"], broadcasted["result"])
        self.assertGreater(baseline["features"]["SortMergeJoin"], 0)
        self.assertGreater(baseline["features"]["Exchange"], 0)
        self.assertGreater(broadcasted["features"]["BroadcastHashJoin"], 0)
        self.assertGreater(broadcasted["features"]["BroadcastExchange"], 0)
        self.assertGreater(adaptive["features"]["AdaptiveSparkPlan"], 0)
        self.assertGreater(adaptive["features"]["isFinalPlan=true"], 0)
        self.assertGreater(adaptive["features"]["coalesced"], 0)
        self.assertEqual(skew_baseline["result"], adaptive_skew["result"])
        self.assertGreater(skew_baseline["features"]["SortMergeJoin"], 0)
        self.assertGreater(adaptive_skew["features"]["AdaptiveSparkPlan"], 0)
        self.assertGreater(adaptive_skew["features"]["isFinalPlan=true"], 0)
        self.assertGreater(adaptive_skew["features"]["skew=true"], 0)
        self.assertGreater(adaptive_skew["features"]["skewed"], 0)
        self.assertEqual(report["skew_input"]["hot_key_rows"], 18_000)
        self.assertEqual(report["input"]["fact_rows"], 256)


if __name__ == "__main__":
    unittest.main()
