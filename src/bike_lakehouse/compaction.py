"""Partition-level Parquet small-file compaction with auditable replacement."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
import math
from pathlib import Path
import shutil
from typing import Callable
from uuid import uuid4

from bike_lakehouse.batch_manifest import utc_timestamp, write_json_atomic


@dataclass(frozen=True)
class PartitionProfile:
    partition: str
    file_count: int
    total_bytes: int
    small_file_count: int
    fingerprint: str


@dataclass(frozen=True)
class CompactionAction:
    action_id: str
    partition: str
    input_file_count: int
    input_bytes: int
    target_file_count: int
    source_fingerprint: str


@dataclass(frozen=True)
class CompactionPlan:
    plan_version: str
    generated_at_utc: str
    dataset: str
    partition_column: str
    target_file_size_bytes: int
    min_files: int
    actions: tuple[CompactionAction, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            **asdict(self),
            "actions": [asdict(action) for action in self.actions],
        }


@dataclass(frozen=True)
class WriteResult:
    input_rows: int
    output_rows: int


PartitionWriter = Callable[[Path, Path, int], WriteResult]


def summarize_compaction_journal(path: Path) -> dict[str, object]:
    """Return a bounded operational view of a resumable compaction journal."""
    journal_path = Path(path)
    try:
        document = json.loads(journal_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"cannot read compaction journal: {journal_path}") from error
    required = {"journal_version", "plan_id", "dataset", "status", "completed_actions"}
    if not isinstance(document, dict) or not required.issubset(document):
        raise ValueError("compaction journal is incomplete")
    if document["journal_version"] != "1.0":
        raise ValueError("unsupported compaction journal version")
    completed = document["completed_actions"]
    if not isinstance(completed, dict):
        raise ValueError("compaction journal completed_actions must be an object")
    failed_action_id = document.get("failed_action_id")
    return {
        "journal_version": document["journal_version"],
        "plan_id": document["plan_id"],
        "dataset": document["dataset"],
        "status": document["status"],
        "updated_at_utc": document.get("updated_at_utc"),
        "completed_action_count": sum(
            isinstance(action, dict) and action.get("status") == "SUCCEEDED"
            for action in completed.values()
        ),
        "failed_action_id": failed_action_id,
        "resumable": document["status"] in {"RUNNING", "FAILED"},
    }


def compaction_plan_id(plan: CompactionPlan) -> str:
    identity = {
        "dataset": plan.dataset,
        "partition_column": plan.partition_column,
        "target_file_size_bytes": plan.target_file_size_bytes,
        "min_files": plan.min_files,
        "actions": [asdict(action) for action in plan.actions],
    }
    canonical = json.dumps(identity, sort_keys=True, separators=(",", ":"))
    return "plan-" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:20]


def profile_partition(
    partition_path: Path,
    *,
    small_file_bytes: int,
) -> PartitionProfile:
    files = sorted(
        path for path in Path(partition_path).glob("*.parquet") if path.is_file()
    )
    digest = hashlib.sha256()
    total_bytes = 0
    small_file_count = 0
    for path in files:
        size = path.stat().st_size
        total_bytes += size
        small_file_count += int(size < small_file_bytes)
        digest.update(path.name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(str(size).encode("ascii"))
        digest.update(b"\0")
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
    return PartitionProfile(
        partition=Path(partition_path).name,
        file_count=len(files),
        total_bytes=total_bytes,
        small_file_count=small_file_count,
        fingerprint=digest.hexdigest(),
    )


def build_compaction_plan(
    dataset: Path,
    *,
    partition_column: str,
    target_file_size_bytes: int = 128 * 1024 * 1024,
    min_files: int = 4,
) -> CompactionPlan:
    root = Path(dataset).resolve()
    if target_file_size_bytes <= 0:
        raise ValueError("target_file_size_bytes must be positive")
    if min_files < 2:
        raise ValueError("min_files must be at least two")
    actions: list[CompactionAction] = []
    for partition_path in sorted(root.glob(f"{partition_column}=*")):
        if not partition_path.is_dir():
            continue
        profile = profile_partition(
            partition_path,
            small_file_bytes=target_file_size_bytes,
        )
        target_count = max(1, math.ceil(profile.total_bytes / target_file_size_bytes))
        if (
            profile.file_count < min_files
            or profile.small_file_count < min_files
            or target_count >= profile.file_count
        ):
            continue
        action_key = (
            f"{profile.partition}:{profile.fingerprint}:{target_count}"
        ).encode("utf-8")
        actions.append(
            CompactionAction(
                action_id="compact-" + hashlib.sha256(action_key).hexdigest()[:16],
                partition=profile.partition,
                input_file_count=profile.file_count,
                input_bytes=profile.total_bytes,
                target_file_count=target_count,
                source_fingerprint=profile.fingerprint,
            )
        )
    return CompactionPlan(
        plan_version="1.0",
        generated_at_utc=utc_timestamp(),
        dataset=root.name,
        partition_column=partition_column,
        target_file_size_bytes=target_file_size_bytes,
        min_files=min_files,
        actions=tuple(actions),
    )


def execute_compaction_action(
    dataset: Path,
    action: CompactionAction,
    *,
    target_file_size_bytes: int,
    writer: PartitionWriter,
) -> dict[str, object]:
    root = Path(dataset).resolve()
    partition_path = root / action.partition
    current = profile_partition(
        partition_path,
        small_file_bytes=target_file_size_bytes,
    )
    if current.fingerprint != action.source_fingerprint:
        raise ValueError(f"partition changed after planning: {action.partition}")
    staging = root / f".{action.partition}.compacting-{uuid4().hex}"
    backup = root / f".{action.partition}.backup-{uuid4().hex}"
    try:
        result = writer(partition_path, staging, action.target_file_count)
        output = profile_partition(
            staging,
            small_file_bytes=target_file_size_bytes,
        )
        if result.input_rows != result.output_rows:
            raise ValueError("compaction row count changed")
        if output.file_count == 0 or output.file_count > action.target_file_count:
            raise ValueError("compaction output file count exceeds the plan")
        partition_path.replace(backup)
        try:
            staging.replace(partition_path)
        except Exception:
            backup.replace(partition_path)
            raise
        shutil.rmtree(backup)
        return {
            "action_id": action.action_id,
            "partition": action.partition,
            "status": "SUCCEEDED",
            "input_rows": result.input_rows,
            "output_rows": result.output_rows,
            "input_file_count": action.input_file_count,
            "output_file_count": output.file_count,
            "input_bytes": action.input_bytes,
            "output_bytes": output.total_bytes,
            "source_fingerprint": action.source_fingerprint,
            "output_fingerprint": output.fingerprint,
        }
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def execute_compaction_plan(
    dataset: Path,
    plan: CompactionPlan,
    *,
    writer: PartitionWriter,
    report_path: Path,
    journal_path: Path | None = None,
) -> dict[str, object]:
    plan_id = compaction_plan_id(plan)
    journal_file = Path(journal_path) if journal_path is not None else None
    journal: dict[str, object] = {
        "journal_version": "1.0",
        "plan_id": plan_id,
        "dataset": plan.dataset,
        "status": "RUNNING",
        "updated_at_utc": utc_timestamp(),
        "completed_actions": {},
    }
    if journal_file is not None and journal_file.exists():
        loaded = json.loads(journal_file.read_text(encoding="utf-8"))
        if loaded.get("journal_version") != "1.0":
            raise ValueError("unsupported compaction journal version")
        if loaded.get("plan_id") != plan_id:
            raise ValueError("compaction journal does not match the current plan")
        if not isinstance(loaded.get("completed_actions"), dict):
            raise ValueError("compaction journal completed_actions must be an object")
        journal = loaded
        journal["status"] = "RUNNING"
        journal["updated_at_utc"] = utc_timestamp()

    completed = journal["completed_actions"]
    assert isinstance(completed, dict)
    results: list[dict[str, object]] = []
    executed_count = 0
    reused_count = 0
    for action in plan.actions:
        previous = completed.get(action.action_id)
        if isinstance(previous, dict) and previous.get("status") == "SUCCEEDED":
            results.append({**previous, "execution": "REUSED"})
            reused_count += 1
            continue
        try:
            result = execute_compaction_action(
                dataset,
                action,
                target_file_size_bytes=plan.target_file_size_bytes,
                writer=writer,
            )
        except Exception as error:
            journal.update(
                {
                    "status": "FAILED",
                    "updated_at_utc": utc_timestamp(),
                    "failed_action_id": action.action_id,
                    "failure_type": type(error).__name__,
                }
            )
            if journal_file is not None:
                write_json_atomic(journal_file, journal)
            raise
        completed[action.action_id] = result
        results.append({**result, "execution": "EXECUTED"})
        executed_count += 1
        journal.update({"status": "RUNNING", "updated_at_utc": utc_timestamp()})
        if journal_file is not None:
            write_json_atomic(journal_file, journal)

    journal.update({"status": "SUCCEEDED", "updated_at_utc": utc_timestamp()})
    journal.pop("failed_action_id", None)
    journal.pop("failure_type", None)
    if journal_file is not None:
        write_json_atomic(journal_file, journal)
    report = {
        "report_version": "1.0",
        "generated_at_utc": utc_timestamp(),
        "dataset": plan.dataset,
        "plan_id": plan_id,
        "status": "SUCCEEDED",
        "action_count": len(results),
        "executed_action_count": executed_count,
        "reused_action_count": reused_count,
        "actions": results,
    }
    write_json_atomic(report_path, report)
    return report


def spark_partition_writer(spark: object) -> PartitionWriter:
    def write(source: Path, staging: Path, target_files: int) -> WriteResult:
        dataframe = spark.read.parquet(str(source))
        input_rows = dataframe.count()
        dataframe.coalesce(target_files).write.mode("overwrite").parquet(str(staging))
        output_rows = spark.read.parquet(str(staging)).count()
        return WriteResult(input_rows=input_rows, output_rows=output_rows)

    return write
