from __future__ import annotations

import json
from pathlib import Path
import tempfile
import unittest

from bike_lakehouse.compaction import (
    WriteResult,
    build_compaction_plan,
    execute_compaction_plan,
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
