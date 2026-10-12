"""Append-only execution journal for controlled partition repairs."""

from __future__ import annotations

from datetime import UTC, datetime
import json
import os
from pathlib import Path
from typing import Any, Callable


EVENT_TYPES = {
    "BATCH_STARTED",
    "ACTION_STARTED",
    "ACTION_SKIPPED",
    "ACTION_VERIFIED",
    "ACTION_FAILED",
    "ACTION_POSTCONDITION_FAILED",
    "BATCH_COMPLETED",
}
OPTIONAL_FIELDS = {
    "action_id",
    "ingestion_date",
    "source_sha256",
    "status",
    "error_type",
    "action_count",
}


class RepairJournal:
    """Persist minimal ordered events and flush each event before returning."""

    def __init__(
        self,
        path: Path,
        execution_id: str,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if not execution_id or len(execution_id) > 128:
            raise ValueError("execution_id must be non-empty and bounded")
        self.path = path
        self.execution_id = execution_id
        self.clock = clock or (lambda: datetime.now(UTC))
        self.sequence = self._load_last_sequence()

    def _load_last_sequence(self) -> int:
        if not self.path.is_file():
            return 0
        expected = 1
        with self.path.open(encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError as error:
                    raise ValueError(f"invalid repair journal line {line_number}") from error
                if not isinstance(row, dict) or row.get("sequence") != expected:
                    raise ValueError("repair journal sequence is not contiguous")
                expected += 1
        return expected - 1

    def record(self, event: dict[str, Any]) -> None:
        event_type = event.get("event_type")
        if event_type not in EVENT_TYPES:
            raise ValueError("unsupported repair journal event type")
        unexpected = set(event) - ({"event_type"} | OPTIONAL_FIELDS)
        if unexpected:
            raise ValueError(
                f"repair journal event contains unsupported fields: {sorted(unexpected)}"
            )
        recorded_at = self.clock()
        if recorded_at.tzinfo is None:
            raise ValueError("repair journal clock must include a timezone")
        self.sequence += 1
        row = {
            "journal_version": "1.0",
            "sequence": self.sequence,
            "execution_id": self.execution_id,
            "event_type": event_type,
            "recorded_at": recorded_at.astimezone(UTC).isoformat().replace("+00:00", "Z"),
            **{key: event[key] for key in sorted(OPTIONAL_FIELDS) if key in event},
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8", newline="\n") as handle:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
