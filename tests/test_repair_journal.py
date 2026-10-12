from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path
import tempfile
import unittest

from bike_lakehouse.repair_execution import execute_repair_plan
from bike_lakehouse.repair_journal import RepairJournal


class RepairJournalTests(unittest.TestCase):
    def test_verified_execution_writes_ordered_minimal_events(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "trips-2026-10-02.ndjson"
            source.write_text('{}\n', encoding="utf-8")
            import hashlib

            digest = hashlib.sha256(source.read_bytes()).hexdigest()
            plan = {
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
            }
            journal_path = root / "journal.ndjson"
            journal = RepairJournal(
                journal_path,
                "execution-one",
                clock=lambda: datetime(2026, 10, 12, tzinfo=UTC),
            )
            report = execute_repair_plan(
                plan,
                source_root=root,
                current_partitions=set(),
                backfill_manifests=[],
                executor=lambda *_: {"written": True},
                verifier=lambda *_: {
                    "partition_present": True,
                    "matching_success_manifest": True,
                },
                event_sink=journal.record,
            )
            rows = [json.loads(line) for line in journal_path.read_text().splitlines()]
            self.assertEqual("COMPLETE", report["status"])
            self.assertEqual([1, 2, 3, 4], [row["sequence"] for row in rows])
            self.assertEqual(
                ["BATCH_STARTED", "ACTION_STARTED", "ACTION_VERIFIED", "BATCH_COMPLETED"],
                [row["event_type"] for row in rows],
            )
            self.assertNotIn(str(source), journal_path.read_text())

    def test_existing_journal_continues_sequence_and_rejects_unknown_fields(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "journal.ndjson"
            first = RepairJournal(path, "execution-one")
            first.record({"event_type": "BATCH_STARTED", "status": "RUNNING"})
            second = RepairJournal(path, "execution-two")
            second.record({"event_type": "BATCH_COMPLETED", "status": "COMPLETE"})
            rows = [json.loads(line) for line in path.read_text().splitlines()]
            self.assertEqual([1, 2], [row["sequence"] for row in rows])
            with self.assertRaisesRegex(ValueError, "unsupported fields"):
                second.record({"event_type": "BATCH_STARTED", "source_path": "private"})


if __name__ == "__main__":
    unittest.main()
