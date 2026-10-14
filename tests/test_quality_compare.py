from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from bike_lakehouse.quality_compare import compare_quality_report_files, compare_quality_reports


class QualityReportComparisonTests(unittest.TestCase):
    def test_regressions_and_recoveries_are_reported_deterministically(self) -> None:
        baseline = {"overall_status": "FAIL", "checks": [
            {"name": "reconciliation", "status": "PASS"},
            {"name": "freshness", "status": "FAIL"},
        ]}
        candidate = {"overall_status": "FAIL", "checks": [
            {"name": "reconciliation", "status": "FAIL"},
            {"name": "freshness", "status": "PASS"},
        ]}
        result = compare_quality_reports(baseline, candidate)
        self.assertEqual("FAIL", result["comparison_status"])
        self.assertEqual(["reconciliation"], result["regressions"])
        self.assertEqual(["freshness"], result["recoveries"])

    def test_missing_candidate_check_is_a_regression(self) -> None:
        result = compare_quality_reports(
            {"checks": [{"name": "contract", "status": "PASS"}]},
            {"checks": []},
        )
        self.assertEqual(["contract"], result["regressions"])

    def test_file_comparison_writes_machine_readable_result(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            baseline = root / "baseline.json"
            candidate = root / "candidate.json"
            output = root / "comparison.json"
            baseline.write_text(json.dumps({"checks": []}), encoding="utf-8")
            candidate.write_text(json.dumps({"checks": []}), encoding="utf-8")
            result = compare_quality_report_files(baseline, candidate, output)
            self.assertEqual("PASS", result["comparison_status"])
            self.assertEqual(result, json.loads(output.read_text(encoding="utf-8")))

    def test_duplicate_check_names_are_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "duplicate"):
            compare_quality_reports(
                {"checks": [{"name": "same", "status": "PASS"}, {"name": "same", "status": "FAIL"}]},
                {"checks": []},
            )


if __name__ == "__main__":
    unittest.main()
