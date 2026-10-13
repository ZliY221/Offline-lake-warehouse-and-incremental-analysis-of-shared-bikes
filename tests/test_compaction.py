from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from bike_lakehouse.compaction import (
    WriteResult,
    build_compaction_plan,
    execute_compaction_plan,
    summarize_compaction_journal,
)


class CompactionTests(unittest.TestCase):
    def _partition(self, root: Path, name: str, file_count: int) -> Path:
        partition = root / name
        partition.mkdir(parents=True)
        for index in range(file_count):
            (partition / f"part-{index:05d}.parquet").write_bytes(
                (f"row-{index}\n" * 3).encode("utf-8")
            )
        (partition / "_SUCCESS").write_text("", encoding="utf-8")
        return partition

    def test_plan_selects_only_eligible_small_file_partitions(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "trips"
            self._partition(root, "ingestion_date=2026-10-01", 5)
            self._partition(root, "ingestion_date=2026-10-02", 2)
            plan = build_compaction_plan(
                root,
                partition_column="ingestion_date",
                target_file_size_bytes=1024,
                min_files=4,
            )
            self.assertEqual(1, len(plan.actions))
            self.assertEqual(
                "ingestion_date=2026-10-01", plan.actions[0].partition
            )
            self.assertEqual(1, plan.actions[0].target_file_count)

    def test_execution_replaces_partition_and_writes_audit_report(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "trips"
            self._partition(root, "ingestion_date=2026-10-01", 5)
            plan = build_compaction_plan(
                root,
                partition_column="ingestion_date",
                target_file_size_bytes=1024,
                min_files=4,
            )

            def writer(source: Path, staging: Path, target_files: int) -> WriteResult:
                rows = sum(
                    path.read_text(encoding="utf-8").count("\n")
                    for path in source.glob("*.parquet")
                )
                staging.mkdir(parents=True)
                (staging / "part-00000.parquet").write_text(
                    "row\n" * rows, encoding="utf-8"
                )
                return WriteResult(input_rows=rows, output_rows=rows)

            report_path = Path(directory) / "report.json"
            report = execute_compaction_plan(
                root,
                plan,
                writer=writer,
                report_path=report_path,
            )
            self.assertEqual("SUCCEEDED", report["status"])
            self.assertEqual(1, report["actions"][0]["output_file_count"])
            self.assertEqual(1, len(list((root / plan.actions[0].partition).glob("*.parquet"))))
            self.assertEqual(report, json.loads(report_path.read_text(encoding="utf-8")))

    def test_execution_refuses_partition_changed_after_planning(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "trips"
            partition = self._partition(root, "ingestion_date=2026-10-01", 5)
            plan = build_compaction_plan(
                root,
                partition_column="ingestion_date",
                target_file_size_bytes=1024,
                min_files=4,
            )
            (partition / "part-00000.parquet").write_text("changed", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "changed after planning"):
                execute_compaction_plan(
                    root,
                    plan,
                    writer=lambda *_: WriteResult(0, 0),
                    report_path=Path(directory) / "report.json",
                )

    def test_journal_resumes_after_an_interrupted_plan(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "trips"
            self._partition(root, "ingestion_date=2026-10-01", 5)
            self._partition(root, "ingestion_date=2026-10-02", 5)
            plan = build_compaction_plan(
                root,
                partition_column="ingestion_date",
                target_file_size_bytes=1024,
                min_files=4,
            )
            journal_path = Path(directory) / "journal.json"
            calls: list[str] = []

            def interrupted_writer(source: Path, staging: Path, _: int) -> WriteResult:
                calls.append(source.name)
                if source.name.endswith("10-02"):
                    raise RuntimeError("interrupted")
                rows = sum(path.read_bytes().count(b"\n") for path in source.glob("*.parquet"))
                staging.mkdir(parents=True)
                (staging / "part-00000.parquet").write_bytes(b"row\n" * rows)
                return WriteResult(rows, rows)

            with self.assertRaisesRegex(RuntimeError, "interrupted"):
                execute_compaction_plan(
                    root,
                    plan,
                    writer=interrupted_writer,
                    report_path=Path(directory) / "first-report.json",
                    journal_path=journal_path,
                )

            resumed_calls: list[str] = []

            def resumed_writer(source: Path, staging: Path, _: int) -> WriteResult:
                resumed_calls.append(source.name)
                rows = sum(path.read_bytes().count(b"\n") for path in source.glob("*.parquet"))
                staging.mkdir(parents=True)
                (staging / "part-00000.parquet").write_bytes(b"row\n" * rows)
                return WriteResult(rows, rows)

            report = execute_compaction_plan(
                root,
                plan,
                writer=resumed_writer,
                report_path=Path(directory) / "final-report.json",
                journal_path=journal_path,
            )
            self.assertEqual(["ingestion_date=2026-10-02"], resumed_calls)
            self.assertEqual(1, report["reused_action_count"])
            self.assertEqual(1, report["executed_action_count"])
            journal = json.loads(journal_path.read_text(encoding="utf-8"))
            self.assertEqual("SUCCEEDED", journal["status"])
            self.assertEqual(2, len(journal["completed_actions"]))
            summary = summarize_compaction_journal(journal_path)
            self.assertEqual(2, summary["completed_action_count"])
            self.assertEqual(2, summary["action_count"])
            self.assertEqual(0, summary["pending_action_count"])
            self.assertEqual(
                ["ingestion_date=2026-10-01", "ingestion_date=2026-10-02"],
                summary["completed_partitions"],
            )
            self.assertFalse(summary["resumable"])

    def test_failed_journal_summary_marks_run_as_resumable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "journal.json"
            path.write_text(
                json.dumps({
                    "journal_version": "1.0",
                    "plan_id": "plan-1",
                    "dataset": "trips",
                    "status": "FAILED",
                    "updated_at_utc": "2026-10-13T00:00:00Z",
                    "failed_action_id": "compact-2",
                    "action_count": 2,
                    "completed_actions": {
                        "compact-1": {
                            "status": "SUCCEEDED",
                            "partition": "ingestion_date=2026-10-01",
                        }
                    },
                }),
                encoding="utf-8",
            )
            summary = summarize_compaction_journal(path)
            self.assertTrue(summary["resumable"])
            self.assertEqual("compact-2", summary["failed_action_id"])
            self.assertEqual(1, summary["completed_action_count"])
            self.assertEqual(1, summary["pending_action_count"])
