from __future__ import annotations

from datetime import date
import hashlib
from pathlib import Path
import tempfile
import unittest

from bike_lakehouse.repair_execution import execute_repair_plan


class RepairExecutionTests(unittest.TestCase):
    def _fixture(self, root: Path) -> tuple[dict, str]:
        source = root / "trips-2026-10-02.ndjson"
        source.write_text('{"trip_id":"trip-1"}\n', encoding="utf-8")
        digest = hashlib.sha256(source.read_bytes()).hexdigest()
        return (
            {
                "plan_version": "1.0",
                "actions": [
                    {
                        "action_id": "repair-one",
                        "ingestion_date": "2026-10-02",
                        "status": "READY",
                        "source_name": source.name,
                        "source_sha256": digest,
                    }
                ],
            },
            digest,
        )

    def test_dry_run_validates_sources_without_executing(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan, _ = self._fixture(root)
            calls: list[str] = []
            report = execute_repair_plan(
                plan,
                source_root=root,
                current_partitions=set(),
                backfill_manifests=[],
                executor=lambda target, source, action: calls.append(action) or {},
                dry_run=True,
            )
            self.assertEqual("READY", report["status"])
            self.assertEqual("WOULD_EXECUTE", report["actions"][0]["status"])
            self.assertEqual([], calls)
            self.assertNotIn("source_path", report["actions"][0])

    def test_hash_mismatch_blocks_every_action_before_execution(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan, _ = self._fixture(root)
            plan["actions"][0]["source_sha256"] = "0" * 64
            calls: list[str] = []
            report = execute_repair_plan(
                plan,
                source_root=root,
                current_partitions=set(),
                backfill_manifests=[],
                executor=lambda target, source, action: calls.append(action) or {},
            )
            self.assertEqual("BLOCKED", report["status"])
            self.assertEqual("BLOCKED_SOURCE_HASH_MISMATCH", report["actions"][0]["status"])
            self.assertEqual([], calls)

    def test_resume_skips_partition_with_matching_success_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan, digest = self._fixture(root)
            report = execute_repair_plan(
                plan,
                source_root=root,
                current_partitions={"2026-10-02"},
                backfill_manifests=[
                    {
                        "pipeline": "trip_date_backfill",
                        "status": "SUCCEEDED",
                        "target_date": "2026-10-02",
                        "source_sha256": digest,
                    }
                ],
                executor=lambda *_: self.fail("completed action must not execute"),
            )
            self.assertEqual("COMPLETE", report["status"])
            self.assertEqual("SKIPPED_ALREADY_COMPLETE", report["actions"][0]["status"])

    def test_execution_stops_after_failure_and_records_safe_error(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan, _ = self._fixture(root)

            def fail(target: date, source: Path, action_id: str) -> dict:
                raise RuntimeError(f"failed {action_id}")

            report = execute_repair_plan(
                plan,
                source_root=root,
                current_partitions=set(),
                backfill_manifests=[],
                executor=fail,
            )
            self.assertEqual("FAILED", report["status"])
            self.assertEqual("RuntimeError", report["actions"][0]["error_type"])

    def test_successful_action_records_backfill_result(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan, _ = self._fixture(root)
            report = execute_repair_plan(
                plan,
                source_root=root,
                current_partitions=set(),
                backfill_manifests=[],
                executor=lambda target, source, action: {"target_date": target.isoformat()},
            )
            self.assertEqual("COMPLETE", report["status"])
            self.assertEqual("EXECUTED", report["actions"][0]["status"])
            self.assertEqual("2026-10-02", report["actions"][0]["result"]["target_date"])


if __name__ == "__main__":
    unittest.main()
