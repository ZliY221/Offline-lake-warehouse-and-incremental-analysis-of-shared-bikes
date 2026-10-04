"""Small atomic JSON batch manifests for auditable local pipeline runs."""

from __future__ import annotations

from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
from typing import Any
from uuid import uuid4


def utc_timestamp() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def fingerprint_file(path: Path) -> tuple[str, int]:
    digest = hashlib.sha256()
    byte_count = 0
    with Path(path).open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
            byte_count += len(chunk)
    return digest.hexdigest(), byte_count


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.{uuid4().hex}.tmp"
    try:
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temporary.replace(path)
    finally:
        if temporary.exists():
            temporary.unlink()


class BatchManifest:
    """Persist one current record per deterministic batch, including rerun count."""

    def __init__(
        self,
        directory: Path,
        batch_id: str,
        base_payload: dict[str, Any],
    ) -> None:
        self.path = Path(directory).resolve() / f"{batch_id}.json"
        attempt_count = 1
        if self.path.is_file():
            previous = json.loads(self.path.read_text(encoding="utf-8"))
            attempt_count = int(previous.get("attempt_count", 0)) + 1
        self.payload = {
            **base_payload,
            "batch_id": batch_id,
            "attempt_count": attempt_count,
            "status": "RUNNING",
            "started_at_utc": utc_timestamp(),
            "completed_at_utc": None,
            "input_rows": None,
            "corrupt_rows": None,
            "output_rows_in_partition": None,
            "error_type": None,
            "error_message": None,
        }
        write_json_atomic(self.path, self.payload)

    def complete(self, **metrics: Any) -> None:
        self.payload.update(metrics)
        self.payload.update(
            status="SUCCEEDED",
            completed_at_utc=utc_timestamp(),
        )
        write_json_atomic(self.path, self.payload)

    def fail(self, error: Exception) -> None:
        self.payload.update(
            status="FAILED",
            completed_at_utc=utc_timestamp(),
            error_type=type(error).__name__,
            error_message=str(error)[:1000],
        )
        write_json_atomic(self.path, self.payload)
