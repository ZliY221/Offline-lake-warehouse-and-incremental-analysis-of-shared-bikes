from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "project_doctor.py"


class ProjectDoctorTest(unittest.TestCase):
    def test_source_profile_has_no_platform_specific_runtime_requirement(self) -> None:
        completed = subprocess.run(
            [sys.executable, str(SCRIPT), "--profile", "source"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        self.assertNotIn("Java runtime", completed.stdout)
        self.assertNotIn("Windows Subsystem for Linux", completed.stdout)

    def test_pipeline_profile_writes_portable_reports(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            json_output = Path(temp_dir) / "report.json"
            markdown_output = Path(temp_dir) / "report.md"
            completed = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--profile",
                    "pipeline",
                    "--json-output",
                    str(json_output),
                    "--markdown-output",
                    str(markdown_output),
                ],
                cwd=ROOT,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
            report = json.loads(json_output.read_text(encoding="utf-8"))
            self.assertEqual(report["project"], "bike-trip-lakehouse")
            self.assertEqual(report["profile"], "pipeline")
            self.assertEqual(report["status"], "READY")
            markdown = markdown_output.read_text(encoding="utf-8")
            self.assertIn("| Check | Status | Detail |", markdown)


if __name__ == "__main__":
    unittest.main()
