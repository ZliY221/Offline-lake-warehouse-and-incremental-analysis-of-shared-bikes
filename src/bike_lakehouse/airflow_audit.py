"""Minimal callback audit records for Airflow retry and failure events."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any


def build_callback_event(event: str, context: dict[str, Any]) -> dict[str, object]:
    task_instance = context.get("task_instance") or context.get("ti")
    if task_instance is None:
        raise ValueError("Airflow callback context has no task instance")
    exception = context.get("exception")
    return {
        "schema_version": 1,
        "observed_at_utc": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "event": event,
        "dag_id": str(task_instance.dag_id),
        "task_id": str(task_instance.task_id),
        "run_id": str(task_instance.run_id),
        "try_number": int(task_instance.try_number),
        "exception_type": type(exception).__name__ if exception else None,
    }


def append_callback_event(path: Path, event: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as stream:
        stream.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")
