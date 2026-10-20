"""Offline data-product releases with deterministic evidence."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from hashlib import sha256
import json
from typing import Any


@dataclass(frozen=True)
class DataRelease:
    release_id: str
    dataset: str
    partitions: tuple[str, ...]
    row_count: int
    schema_version: str
    source_fingerprint: str
    quality_passed: bool
    status: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_release(*, dataset: str, partitions: list[str], row_count: int,
                  schema_version: str, source_fingerprint: str,
                  quality_passed: bool) -> DataRelease:
    """Build a stable release identity and enforce publish gates."""
    if not dataset.strip() or not schema_version.strip() or not source_fingerprint.strip():
        raise ValueError("dataset, schema_version and source_fingerprint are required")
    if row_count < 0 or not partitions:
        raise ValueError("row_count must be non-negative and partitions cannot be empty")
    normalized = tuple(sorted(set(partitions)))
    payload = json.dumps({"dataset": dataset, "partitions": normalized,
                          "row_count": row_count, "schema_version": schema_version,
                          "source_fingerprint": source_fingerprint}, sort_keys=True)
    release_id = "rel-" + sha256(payload.encode()).hexdigest()[:12]
    return DataRelease(release_id, dataset, normalized, row_count, schema_version,
                       source_fingerprint, quality_passed,
                       "READY" if quality_passed else "BLOCKED")
