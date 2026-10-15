import tempfile
import unittest
from pathlib import Path

from bike_lakehouse.operations_history import build_operations_history, render_operations_history_html, write_operations_history


class OperationsHistoryTest(unittest.TestCase):
    def snapshot(self, run_id: str, ready: bool, failed: int = 0) -> dict:
        return {
            "generated_at_utc": f"2026-10-15T00:00:0{run_id[-1]}Z",
            "status": "READY" if ready else "ATTENTION_REQUIRED",
            "release_ready": ready,
            "run": {"run_id": run_id, "pipeline_status": "SUCCEEDED", "quality_status": "PASS" if ready else "FAIL"},
            "summary": {"failed_quality_check_count": failed, "remediation_action_count": failed, "partition_count": 2},
        }

    def test_builds_release_summary_and_ordered_runs(self) -> None:
        history = build_operations_history([self.snapshot("run-2", False, 2), self.snapshot("run-1", True)])
        self.assertEqual(2, history["summary"]["run_count"])
        self.assertEqual(0.5, history["summary"]["readiness_rate"])
        self.assertEqual(False, history["summary"]["latest_release_ready"])
        self.assertEqual("ATTENTION_REQUIRED", history["summary"]["latest_status"])
        self.assertEqual(1, history["summary"]["current_status_streak"])
        self.assertEqual(0.5, history["summary"]["recent_readiness_rate"])
        self.assertEqual("AT_RISK", history["summary"]["trend_status"])
        self.assertEqual(["run-1", "run-2"], [row["run_id"] for row in history["runs"]])

    def test_rejects_incomplete_snapshot(self) -> None:
        with self.assertRaisesRegex(ValueError, "run and summary"):
            build_operations_history([{"status": "READY"}])

    def test_empty_history_has_zero_summary_and_readable_html(self) -> None:
        history = build_operations_history([])
        self.assertEqual(0, history["summary"]["run_count"])
        self.assertEqual(0.0, history["summary"]["readiness_rate"])
        self.assertIsNone(history["summary"]["latest_release_ready"])
        self.assertEqual("NO_RUNS", history["summary"]["latest_status"])
        self.assertEqual("NO_DATA", history["summary"]["trend_status"])
        self.assertIn("暂无运行快照", render_operations_history_html(history))

    def test_history_html_escapes_run_identity(self) -> None:
        history = build_operations_history([self.snapshot("<script>alert(1)</script>", True)])
        html = render_operations_history_html(history)
        self.assertNotIn("<script>", html)
        self.assertIn("&lt;script&gt;", html)

    def test_recent_window_and_threshold_are_configurable(self) -> None:
        history = build_operations_history(
            [self.snapshot("run-1", False), self.snapshot("run-2", True)],
            recent_window=1,
            readiness_threshold=1.0,
        )
        self.assertEqual(1, history["summary"]["recent_run_count"])
        self.assertEqual("HEALTHY", history["summary"]["trend_status"])
        with self.assertRaisesRegex(ValueError, "recent_window"):
            build_operations_history([], recent_window=0)

    def test_writes_json_and_standalone_html(self) -> None:
        history = build_operations_history([self.snapshot("run-1", True)])
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_operations_history(history, root / "history.json", root / "index.html")
            self.assertIn('"ready_count": 1', (root / "history.json").read_text(encoding="utf-8"))
            self.assertIn("多批次运行历史", (root / "index.html").read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
