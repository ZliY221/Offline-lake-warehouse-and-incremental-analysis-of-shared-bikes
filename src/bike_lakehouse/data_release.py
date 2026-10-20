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


@dataclass(frozen=True)
class RestorePoint:
    restore_id: str
    release_id: str
    dataset: str
    partitions: tuple[str, ...]
    source_fingerprint: str
    executable: bool

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


def build_restore_point(release: DataRelease) -> RestorePoint:
    """Capture the immutable inputs needed to restore a published dataset."""
    restore_id = "rst-" + sha256(f"{release.release_id}:{release.source_fingerprint}".encode()).hexdigest()[:12]
    return RestorePoint(restore_id, release.release_id, release.dataset, release.partitions,
                        release.source_fingerprint, release.status == "READY")


def retention_decision(*, age_days: int, retained_releases: int,
                       minimum_releases: int = 3, retention_days: int = 90) -> dict[str, Any]:
    """Recommend keeping or archiving without removing the recovery floor."""
    if min(age_days, retained_releases, minimum_releases, retention_days) < 0:
        raise ValueError("retention values cannot be negative")
    protected = retained_releases <= minimum_releases
    action = "KEEP_MINIMUM" if protected else "ARCHIVE" if age_days >= retention_days else "KEEP_RECENT"
    return {"action": action, "age_days": age_days, "retained_releases": retained_releases,
            "minimum_releases": minimum_releases, "retention_days": retention_days,
            "archive_eligible": action == "ARCHIVE"}


def release_audit(releases: list[DataRelease]) -> dict[str, Any]:
    """Summarize release readiness and detect duplicate identifiers."""
    ids = [item.release_id for item in releases]
    ready = sum(item.status == "READY" for item in releases)
    duplicates = sorted({item for item in ids if ids.count(item) > 1})
    return {"release_count": len(releases), "ready_count": ready,
            "blocked_count": len(releases) - ready,
            "datasets": sorted({item.dataset for item in releases}),
            "duplicate_release_ids": duplicates,
            "audit_status": "REVIEW" if duplicates or ready != len(releases) else "PASS"}


def compare_releases(previous: DataRelease, current: DataRelease) -> dict[str, Any]:
    """Explain what changed between two offline data-product releases."""
    if previous.dataset != current.dataset:
        raise ValueError("releases must belong to the same dataset")
    added = sorted(set(current.partitions) - set(previous.partitions))
    removed = sorted(set(previous.partitions) - set(current.partitions))
    return {"dataset": current.dataset, "previous_release_id": previous.release_id,
            "current_release_id": current.release_id, "added_partitions": added,
            "removed_partitions": removed, "row_count_delta": current.row_count - previous.row_count,
            "schema_changed": previous.schema_version != current.schema_version,
            "requires_review": bool(removed or previous.schema_version != current.schema_version)}


def recovery_drill(point: RestorePoint, *, restored_partitions: list[str],
                   fingerprint_matches: bool) -> dict[str, Any]:
    """Evaluate a restore rehearsal without changing the published dataset."""
    restored = tuple(sorted(set(restored_partitions)))
    missing = sorted(set(point.partitions) - set(restored))
    unexpected = sorted(set(restored) - set(point.partitions))
    passed = point.executable and fingerprint_matches and not missing and not unexpected
    return {"restore_id": point.restore_id, "passed": passed,
            "missing_partitions": missing, "unexpected_partitions": unexpected,
            "fingerprint_matches": fingerprint_matches,
            "status": "PASS" if passed else "REVIEW"}


def delivery_sla(*, ready_at_epoch: int, due_at_epoch: int,
                 delivered_at_epoch: int | None = None) -> dict[str, Any]:
    """Track an offline release against its delivery deadline."""
    if ready_at_epoch < 0 or due_at_epoch <= ready_at_epoch:
        raise ValueError("due time must be later than ready time")
    observed = delivered_at_epoch if delivered_at_epoch is not None else ready_at_epoch
    delay = observed - due_at_epoch
    status = "DELIVERED_LATE" if delivered_at_epoch is not None and delay > 0 else "DELIVERED_ON_TIME" if delivered_at_epoch is not None else "READY"
    return {"status": status, "delivery_window_seconds": due_at_epoch - ready_at_epoch,
            "delay_seconds": max(0, delay), "delivered": delivered_at_epoch is not None}
