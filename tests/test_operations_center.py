from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from bike_lakehouse.operations_center import (
    build_operations_snapshot,
    render_operations_html,
    write_operations_bundle,
)


def quality(status: str = "PASS") -> dict:
    return {
        "overall_status": status,
        "checks": [{"name": "silver_row_reconciliation", "status": status}],
        "dataset_counts": {"bronze_rows": 20, "silver_valid_rows": 20},
        "bronze_partition_rows": {"2026-10-01": 20},
        "manifest_summary": {"status_counts": {"SUCCEEDED": 1}},
    }


def orchestration(status: str = "SUCCEEDED") -> dict:
    return {
        "pipeline_name": "bike-lakehouse",
        "run_id": "run-1",
        "status": status,
        "resume_count": 0,
        "tasks": {
            "bronze": {"status": status, "attempts": 1, "dependencies": []},
            "silver": {"status": status, "attempts": 1, "dependencies": ["bronze"]},
        },
    }


class OperationsCenterTests(unittest.TestCase):
    def test_ready_run_combines_quality_and_orchestration(self) -> None:
        snapshot = build_operations_snapshot(quality(), orchestration())

        self.assertTrue(snapshot["release_ready"])
        self.assertEqual("READY", snapshot["status"])
        self.assertEqual(["bronze", "silver"], [task["name"] for task in snapshot["tasks"]])
        self.assertEqual(20, snapshot["quality"]["dataset_counts"]["bronze_rows"])

    def test_failed_quality_creates_visible_action_queue(self) -> None:
        report = quality("FAIL")
        snapshot = build_operations_snapshot(report, orchestration())
        html = render_operations_html(snapshot)

        self.assertFalse(snapshot["release_ready"])
        self.assertEqual(1, snapshot["summary"]["remediation_action_count"])
        self.assertIn("ATTENTION_REQUIRED", html)
        self.assertIn("silver_row_reconciliation", html)

    def test_report_uses_accessible_offline_workspace_layout(self) -> None:
        html = render_operations_html(build_operations_snapshot(quality(), orchestration()))

        self.assertIn('class="skip" href="#content"', html)
        self.assertIn("OFFLINE LAKEHOUSE · BATCH DESK", html)
        self.assertIn('class="workspace page-view"', html)
        self.assertIn("@media(max-width:680px)", html)

    def test_report_exposes_four_distinct_operation_pages(self) -> None:
        html = render_operations_html(build_operations_snapshot(quality(), orchestration()))

        for page in ("overview", "pipeline", "datasets", "quality"):
            self.assertIn(f'data-screen-link="{page}"', html)
            self.assertIn(f'data-page="{page}"', html)
        self.assertIn("function openScreen(screen)", html)
        self.assertIn('body[data-screen="quality"]', html)

    def test_bundle_writes_json_and_standalone_html(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            snapshot = build_operations_snapshot(quality(), orchestration())
            write_operations_bundle(snapshot, root / "snapshot.json", root / "index.html")

            self.assertEqual(snapshot, json.loads((root / "snapshot.json").read_text(encoding="utf-8")))
            self.assertIn("共享单车离线湖仓运行工作台", (root / "index.html").read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
