from __future__ import annotations

import unittest

from bike_lakehouse.benchmark import summarize_rounds


class BenchmarkSummaryTests(unittest.TestCase):
    def test_summary_uses_median_and_keeps_stage_timings(self) -> None:
        rounds = [
            {
                "bronze_seconds": 2.0,
                "silver_seconds": 4.0,
                "gold_seconds": 6.0,
                "total_seconds": 12.0,
            },
            {
                "bronze_seconds": 1.0,
                "silver_seconds": 3.0,
                "gold_seconds": 5.0,
                "total_seconds": 9.0,
            },
            {
                "bronze_seconds": 9.0,
                "silver_seconds": 8.0,
                "gold_seconds": 7.0,
                "total_seconds": 24.0,
            },
        ]
        summary = summarize_rounds(rounds, input_rows=1_200)
        self.assertEqual(summary["median_bronze_seconds"], 2.0)
        self.assertEqual(summary["median_silver_seconds"], 4.0)
        self.assertEqual(summary["median_gold_seconds"], 6.0)
        self.assertEqual(summary["median_total_seconds"], 12.0)
        self.assertEqual(summary["median_end_to_end_input_rows_per_second"], 100.0)

    def test_empty_rounds_are_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "At least one measured round"):
            summarize_rounds([], input_rows=1)


if __name__ == "__main__":
    unittest.main()
