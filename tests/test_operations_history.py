import tempfile
import unittest
from pathlib import Path

from bike_lakehouse.operations_history import build_operations_history, write_operations_history


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
        self.assertEqual(["run-1", "run-2"], [row["run_id"] for row in history["runs"]])

    def test_rejects_incomplete_snapshot(self) -> None:
        with self.assertRaisesRegex(ValueError, "run and summary"):
            build_operations_history([{"status": "READY"}])

    def test_writes_json_and_standalone_html(self) -> None:
        history = build_operations_history([self.snapshot("run-1", True)])
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_operations_history(history, root / "history.json", root / "index.html")
            self.assertIn('"ready_count": 1', (root / "history.json").read_text(encoding="utf-8"))
            self.assertIn("多批次运行历史", (root / "index.html").read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
