from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from bike_lakehouse.spark import create_local_spark
from bike_lakehouse.stage_metrics import collect_stage_metrics, summarize_tasks


class StageMetricsTests(unittest.TestCase):
    def test_task_metrics_are_normalized_and_summarized(self) -> None:
        summary = summarize_tasks(
            [
                {
                    "taskId": 2,
                    "index": 1,
                    "attempt": 0,
                    "status": "SUCCESS",
                    "taskMetrics": {
                        "executorRunTime": 30,
                        "shuffleReadMetrics": {
                            "remoteBytesRead": 7,
                            "localBytesRead": 5,
                        },
                    },
                },
                {
                    "taskId": 1,
                    "index": 0,
                    "attempt": 0,
                    "status": "SUCCESS",
                    "taskMetrics": {"executorRunTime": 10},
                },
            ]
        )
        self.assertEqual(summary["task_count"], 2)
        self.assertEqual(summary["executor_run_time_ms"]["median"], 20)
        self.assertEqual(
            summary["executor_run_time_ms"]["max_to_median_ratio"],
            1.5,
        )
        self.assertEqual(summary["tasks"][0]["task_id"], 1)
        self.assertEqual(summary["tasks"][1]["shuffle_read_bytes"], 12)

    def test_live_spark_ui_reports_jobs_stages_and_tasks(self) -> None:
        with tempfile.TemporaryDirectory() as temp_directory:
            spark = create_local_spark(
                "bike-trip-stage-metrics-tests",
                Path(temp_directory) / "warehouse",
                ui_enabled=True,
            )
            try:
                report = collect_stage_metrics(
                    spark,
                    row_count=2_000,
                    shuffle_partitions=4,
                )
            finally:
                spark.stop()
        self.assertEqual(report["result"]["total_rows"], 2_000)
        self.assertEqual(report["result"]["group_count"], 32)
        self.assertGreater(len(report["jobs"]), 0)
        self.assertGreater(len(report["stages"]), 0)
        self.assertGreater(report["stage_totals"]["num_tasks"], 0)
        self.assertGreater(
            report["stage_totals"]["shuffle_write_bytes"],
            0,
        )
        self.assertGreater(
            report["busiest_shuffle_stage"]["task_summary"]["task_count"],
            0,
        )


if __name__ == "__main__":
    unittest.main()
