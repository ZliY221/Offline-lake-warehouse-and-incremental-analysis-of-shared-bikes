from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from bike_lakehouse.quality_actions import build_remediation_plan, build_remediation_plan_file


class QualityActionTests(unittest.TestCase):
    def test_failed_checks_become_ordered_actions(self) -> None:
        plan = build_remediation_plan({
            "overall_status": "FAIL",
            "checks": [
                {"name": "silver_rejected_ratio", "status": "FAIL", "observed_ratio": 0.2},
                {"name": "bronze_partition_continuity", "status": "FAIL", "missing_ingestion_dates": ["2026-10-02"]},
                {"name": "gold_trip_reconciliation", "status": "PASS"},
            ],
            "manifest_summary": {"status_counts": {"FAILED": 1}},
        })

        self.assertEqual(3, plan["action_count"])
        self.assertFalse(plan["ready_for_release"])
        self.assertEqual("bronze_partition_continuity", plan["actions"][0]["check_name"])
        self.assertEqual("P0", plan["actions"][0]["priority"])
        self.assertEqual(["P0", "P1", "P1"], [item["priority"] for item in plan["actions"]])

    def test_clean_report_is_release_ready_and_can_be_saved(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report = root / "quality.json"
            output = root / "actions.json"
            report.write_text(json.dumps({
                "overall_status": "PASS",
                "checks": [{"name": "silver_row_reconciliation", "status": "PASS"}],
                "manifest_summary": {"status_counts": {}},
            }), encoding="utf-8")

            plan = build_remediation_plan_file(report, output)

            self.assertTrue(plan["ready_for_release"])
            self.assertEqual([], plan["actions"])
            self.assertEqual(plan, json.loads(output.read_text(encoding="utf-8")))


if __name__ == "__main__":
    unittest.main()
