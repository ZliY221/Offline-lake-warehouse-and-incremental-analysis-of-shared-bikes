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
        self.assertEqual(1, history["summary"]["longest_ready_streak"])
        self.assertEqual(1, history["summary"]["longest_attention_streak"])
        self.assertEqual(6, history["summary"]["latest_quality_debt_score"])
        self.assertEqual(6, history["runs"][-1]["quality_debt_score"])
        self.assertEqual(3.0, history["summary"]["recent_average_quality_debt_score"])
        self.assertEqual(6, history["summary"]["peak_quality_debt_score"])
        self.assertEqual(6, history["summary"]["latest_quality_debt_delta"])
        self.assertEqual({"CLEAN": 1, "MANAGEABLE": 0, "HIGH": 1}, history["summary"]["quality_debt_bands"])
        self.assertEqual(1, history["summary"]["recent_high_debt_count"])
        self.assertEqual("INVESTIGATE_QUALITY_DEBT", history["summary"]["recommended_action"])
        self.assertEqual("P0", history["summary"]["recommended_action_priority"])
        self.assertEqual(1, history["summary"]["recommended_review_window_hours"])
        self.assertTrue(history["summary"]["escalation_required"])
        work_order = history["summary"]["recovery_work_order"]
        self.assertRegex(work_order["work_order_id"], r"^REC-[0-9A-F]{12}$")
        self.assertEqual("OPEN", work_order["state"])
        self.assertEqual("run-2", work_order["run_id"])
        self.assertEqual("P0", work_order["priority"])
        self.assertEqual("2026-10-15T01:00:02Z", work_order["review_due_at_utc"])
        overdue = build_operations_history(
            [self.snapshot("run-2", False, 2)], as_of_utc="2026-10-15T03:00:02Z"
        )["summary"]["recovery_work_order"]
        self.assertEqual("OVERDUE", overdue["review_status"])
        self.assertEqual(2, overdue["overdue_by_hours"])
        self.assertEqual("DATA_PLATFORM_LEAD", overdue["owner_route"])
        self.assertIn("OVERDUE", overdue["handoff_summary"])
        self.assertEqual(3, overdue["age_hours"])
        self.assertEqual("FRESH", overdue["age_status"])
        self.assertEqual("BREACHED", overdue["sla_status"])
        self.assertIsNone(overdue["remaining_review_hours"])
        self.assertEqual(
            ["REVIEW_QUALITY_CHECKS", "VERIFY_REPAIR_EVIDENCE", "NOTIFY_OWNER_ROUTE", "RECORD_REVIEW_RESULT"],
            overdue["handoff_checklist"],
        )
        progressed = build_operations_history(
            [self.snapshot("run-2", False, 2)],
            as_of_utc="2026-10-15T03:00:02Z",
            completed_handoff_steps=["REVIEW_QUALITY_CHECKS", "VERIFY_REPAIR_EVIDENCE"],
        )["summary"]["recovery_work_order"]["handoff_progress"]
        self.assertEqual(4, progressed["total_count"])
        self.assertEqual(2, progressed["completed_count"])
        self.assertEqual(2, progressed["remaining_count"])
        self.assertEqual(0.5, progressed["completion_rate"])
        self.assertEqual(["NOTIFY_OWNER_ROUTE", "RECORD_REVIEW_RESULT"], progressed["remaining_steps"])
        self.assertEqual(0.5, history["summary"]["recent_readiness_rate"])
        self.assertEqual("AT_RISK", history["summary"]["trend_status"])
        self.assertEqual(2, history["summary"]["latest_failed_quality_check_delta"])
        self.assertEqual(2, history["summary"]["latest_remediation_action_delta"])
        self.assertEqual(0, history["summary"]["latest_partition_count_delta"])
        self.assertEqual("REGRESSED", history["summary"]["latest_change_status"])
        self.assertEqual(
            ["RELEASE_READINESS_LOST", "FAILED_CHECKS_INCREASED", "REMEDIATION_ACTIONS_INCREASED"],
            history["summary"]["latest_regression_reasons"],
        )
        self.assertEqual(["run-1", "run-2"], [row["run_id"] for row in history["runs"]])

    def test_rejects_incomplete_snapshot(self) -> None:
        with self.assertRaisesRegex(ValueError, "run and summary"):
            build_operations_history([{"status": "READY"}])

    def test_rejects_invalid_snapshot_counts_and_boolean_flags(self) -> None:
        invalid_count = self.snapshot("run-1", True)
        invalid_count["summary"]["partition_count"] = -1
        with self.assertRaisesRegex(ValueError, "partition_count must be non-negative"):
            build_operations_history([invalid_count])
        invalid_flag = self.snapshot("run-2", True)
        invalid_flag["release_ready"] = "false"
        with self.assertRaisesRegex(ValueError, "release_ready must be a boolean"):
            build_operations_history([invalid_flag])
        with self.assertRaisesRegex(ValueError, "unknown completed handoff steps"):
            build_operations_history([self.snapshot("run-1", True)], completed_handoff_steps=["UNKNOWN_STEP"])

    def test_empty_history_has_zero_summary_and_readable_html(self) -> None:
        history = build_operations_history([])
        self.assertEqual(0, history["summary"]["run_count"])
        self.assertEqual(0.0, history["summary"]["readiness_rate"])
        self.assertIsNone(history["summary"]["latest_release_ready"])
        self.assertEqual("NO_RUNS", history["summary"]["latest_status"])
        self.assertEqual("NO_DATA", history["summary"]["trend_status"])
        self.assertEqual(0, history["summary"]["longest_ready_streak"])
        self.assertEqual(0, history["summary"]["longest_attention_streak"])
        self.assertEqual(0, history["summary"]["latest_quality_debt_score"])
        self.assertEqual(0.0, history["summary"]["recent_average_quality_debt_score"])
        self.assertEqual(0, history["summary"]["peak_quality_debt_score"])
        self.assertIsNone(history["summary"]["latest_quality_debt_delta"])
        self.assertEqual({"CLEAN": 0, "MANAGEABLE": 0, "HIGH": 0}, history["summary"]["quality_debt_bands"])
        self.assertEqual("NO_DATA", history["summary"]["recommended_action"])
        self.assertEqual("NONE", history["summary"]["recommended_action_priority"])
        self.assertIsNone(history["summary"]["recommended_review_window_hours"])
        self.assertFalse(history["summary"]["escalation_required"])
        self.assertEqual("MONITORING", history["summary"]["recovery_work_order"]["state"])
        self.assertIsNone(history["summary"]["recovery_work_order"]["run_id"])
        self.assertIsNone(history["summary"]["recovery_work_order"]["review_due_at_utc"])
        self.assertEqual("NOT_REQUIRED", history["summary"]["recovery_work_order"]["review_status"])
        self.assertEqual("UNASSIGNED", history["summary"]["recovery_work_order"]["owner_route"])
        self.assertEqual("NO_DATA", history["summary"]["recovery_work_order"]["age_status"])
        self.assertEqual("NOT_APPLICABLE", history["summary"]["recovery_work_order"]["sla_status"])
        self.assertEqual(["COLLECT_FIRST_RUN"], history["summary"]["recovery_work_order"]["handoff_checklist"])
        self.assertIsNone(history["summary"]["latest_failed_quality_check_delta"])
        self.assertEqual("NO_BASELINE", history["summary"]["latest_change_status"])
        self.assertIn("暂无运行快照", render_operations_history_html(history))

    def test_history_html_escapes_run_identity(self) -> None:
        history = build_operations_history([self.snapshot("<script>alert(1)</script>", True)])
        work_order = history["summary"]["recovery_work_order"]
        self.assertIsNone(work_order["age_hours"])
        self.assertEqual("UNKNOWN", work_order["age_status"])
        html = render_operations_history_html(history)
        self.assertNotIn("<script>", html)
        self.assertIn("&lt;script&gt;", html)
        self.assertIn("暂无上一批次", html)

    def test_recent_window_and_threshold_are_configurable(self) -> None:
        history = build_operations_history(
            [self.snapshot("run-1", False), self.snapshot("run-2", True)],
            recent_window=1,
            readiness_threshold=1.0,
        )
        self.assertEqual(1, history["summary"]["recent_run_count"])
        self.assertEqual("HEALTHY", history["summary"]["trend_status"])
        self.assertEqual("IMPROVED", history["summary"]["latest_change_status"])
        self.assertEqual("CONTINUE_RELEASE", history["summary"]["recommended_action"])
        self.assertEqual("P3", history["summary"]["recommended_action_priority"])
        self.assertEqual(72, history["summary"]["recommended_review_window_hours"])
        with self.assertRaisesRegex(ValueError, "recent_window"):
            build_operations_history([], recent_window=0)

    def test_tracks_longest_ready_and_attention_streaks(self) -> None:
        history = build_operations_history([
            self.snapshot("run-1", True),
            self.snapshot("run-2", True),
            self.snapshot("run-3", False),
            self.snapshot("run-4", False),
            self.snapshot("run-5", False),
            self.snapshot("run-6", True),
        ])
        self.assertEqual(2, history["summary"]["longest_ready_streak"])
        self.assertEqual(3, history["summary"]["longest_attention_streak"])
        self.assertEqual(1, history["summary"]["current_status_streak"])
        self.assertIn("最长连续就绪", render_operations_history_html(history))

    def test_recommends_monitoring_when_recent_readiness_is_at_risk(self) -> None:
        history = build_operations_history([
            self.snapshot("run-1", False),
            self.snapshot("run-2", True),
        ], recent_window=2, readiness_threshold=0.8)
        self.assertEqual("AT_RISK", history["summary"]["trend_status"])
        self.assertEqual("MONITOR_RECENT_RUNS", history["summary"]["recommended_action"])
        self.assertEqual("P2", history["summary"]["recommended_action_priority"])
        self.assertEqual(24, history["summary"]["recommended_review_window_hours"])

    def test_writes_json_and_standalone_html(self) -> None:
        history = build_operations_history([self.snapshot("run-1", True)])
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            write_operations_history(history, root / "history.json", root / "index.html")
            self.assertIn('"ready_count": 1', (root / "history.json").read_text(encoding="utf-8"))
            self.assertIn("多批次运行历史", (root / "index.html").read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
