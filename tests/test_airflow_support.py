from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from bike_lakehouse.airflow_audit import append_callback_event, build_callback_event
from bike_lakehouse.airflow_runtime import resolve_data_input


class FakeTaskInstance:
    dag_id = "example_dag"
    task_id = "example_task"
    run_id = "manual__test"
    try_number = 2


class AirflowSupportTests(unittest.TestCase):
    def test_callback_audit_keeps_minimal_fields_and_appends_jsonl(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "audit.jsonl"
            event = build_callback_event(
                "TASK_FAILURE",
                {
                    "task_instance": FakeTaskInstance(),
                    "exception": ValueError("sensitive details must not be copied"),
                },
            )
            append_callback_event(path, event)
            record = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(record["event"], "TASK_FAILURE")
        self.assertEqual(record["try_number"], 2)
        self.assertEqual(record["exception_type"], "ValueError")
        self.assertNotIn("sensitive", json.dumps(record))

    def test_backfill_input_is_restricted_to_repository_data(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data_file = root / "data" / "sample.ndjson"
            data_file.parent.mkdir(parents=True)
            data_file.write_text("{}\n", encoding="utf-8")
            self.assertEqual(resolve_data_input(root, "data/sample.ndjson"), data_file)
            with self.assertRaisesRegex(ValueError, "stay under data"):
                resolve_data_input(root, "../outside.ndjson")
            with self.assertRaisesRegex(ValueError, "repository-relative"):
                resolve_data_input(root, str(data_file.resolve()))


if __name__ == "__main__":
    unittest.main()
