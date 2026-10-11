"""Dependency-aware local orchestration with retries and resumable state."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import subprocess
from typing import Callable, Iterable

from .batch_manifest import write_json_atomic


def _utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


@dataclass(frozen=True)
class TaskSpec:
    name: str
    dependencies: tuple[str, ...]
    command: tuple[str, ...]
    max_attempts: int = 2

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("task name must not be empty")
        if self.max_attempts <= 0:
            raise ValueError("max_attempts must be greater than zero")


@dataclass(frozen=True)
class ExecutionResult:
    exit_code: int
    log_path: str
    log_sha256: str


TaskExecutor = Callable[[TaskSpec, int], ExecutionResult]


def validate_tasks(tasks: Iterable[TaskSpec]) -> list[TaskSpec]:
    ordered = list(tasks)
    by_name = {task.name: task for task in ordered}
    if len(by_name) != len(ordered):
        raise ValueError("task names must be unique")
    for task in ordered:
        missing = sorted(set(task.dependencies) - set(by_name))
        if missing:
            raise ValueError(f"task {task.name} has missing dependencies: {missing}")
    visiting: set[str] = set()
    visited: set[str] = set()
    result: list[TaskSpec] = []

    def visit(name: str) -> None:
        if name in visited:
            return
        if name in visiting:
            raise ValueError(f"task dependency cycle includes {name}")
        visiting.add(name)
        task = by_name[name]
        for dependency in task.dependencies:
            visit(dependency)
        visiting.remove(name)
        visited.add(name)
        result.append(task)

    for task in ordered:
        visit(task.name)
    return result


def task_graph_fingerprint(tasks: Iterable[TaskSpec]) -> str:
    payload = [
        {
            "name": task.name,
            "dependencies": list(task.dependencies),
            "command": list(task.command),
            "max_attempts": task.max_attempts,
        }
        for task in tasks
    ]
    return hashlib.sha256(
        json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ).hexdigest()


class CommandExecutor:
    def __init__(
        self,
        *,
        working_directory: Path,
        log_directory: Path,
        inject_failure_once: str | None = None,
    ) -> None:
        self.working_directory = working_directory.resolve()
        self.log_directory = log_directory.resolve()
        self.inject_failure_once = inject_failure_once
        self._injected = False

    def __call__(self, task: TaskSpec, attempt: int) -> ExecutionResult:
        self.log_directory.mkdir(parents=True, exist_ok=True)
        log_path = self.log_directory / f"{task.name}-attempt-{attempt}.log"
        if task.name == self.inject_failure_once and not self._injected:
            self._injected = True
            output = "Intentional one-time failure for retry evidence.\n"
            exit_code = 75
        else:
            completed = subprocess.run(
                task.command,
                cwd=self.working_directory,
                capture_output=True,
                check=False,
                text=True,
            )
            output = completed.stdout + completed.stderr
            exit_code = completed.returncode
        log_path.write_text(output, encoding="utf-8")
        log_sha256 = hashlib.sha256(log_path.read_bytes()).hexdigest()
        relative_log_path = log_path.relative_to(self.working_directory).as_posix()
        return ExecutionResult(
            exit_code=exit_code,
            log_path=relative_log_path,
            log_sha256=log_sha256,
        )


class PipelineOrchestrator:
    def __init__(
        self,
        *,
        pipeline_name: str,
        run_id: str,
        tasks: Iterable[TaskSpec],
        state_path: Path,
        executor: TaskExecutor,
    ) -> None:
        self.pipeline_name = pipeline_name
        self.run_id = run_id
        self.tasks = validate_tasks(tasks)
        self.state_path = state_path.resolve()
        self.executor = executor
        self.fingerprint = task_graph_fingerprint(self.tasks)

    def _new_state(self) -> dict[str, object]:
        now = _utc_now()
        return {
            "report_version": "1.0",
            "pipeline_name": self.pipeline_name,
            "run_id": self.run_id,
            "task_graph_sha256": self.fingerprint,
            "status": "PENDING",
            "created_at_utc": now,
            "updated_at_utc": now,
            "resume_count": 0,
            "claims_boundary": (
                "Scope: local dependency, retry, log hashing, and resume behavior. "
                "Production scheduling and distributed control planes are out of scope."
            ),
            "tasks": {
                task.name: {
                    "dependencies": list(task.dependencies),
                    "max_attempts_per_invocation": task.max_attempts,
                    "status": "PENDING",
                    "attempts": 0,
                    "started_at_utc": None,
                    "finished_at_utc": None,
                    "last_exit_code": None,
                    "last_log_path": None,
                    "last_log_sha256": None,
                    "attempt_history": [],
                }
                for task in self.tasks
            },
            "events": [],
        }

    def _load_state(self, *, resume: bool) -> dict[str, object]:
        if not self.state_path.exists():
            if resume:
                raise ValueError(f"cannot resume missing state: {self.state_path}")
            return self._new_state()
        if not resume:
            raise ValueError(
                f"state already exists; use --resume or choose another run root: {self.state_path}"
            )
        state = json.loads(self.state_path.read_text(encoding="utf-8"))
        if state.get("pipeline_name") != self.pipeline_name:
            raise ValueError("pipeline name does not match existing state")
        if state.get("run_id") != self.run_id:
            raise ValueError("run ID does not match existing state")
        if state.get("task_graph_sha256") != self.fingerprint:
            raise ValueError("task graph changed; refusing unsafe resume")
        state["resume_count"] = int(state.get("resume_count", 0)) + 1
        for task_state in state["tasks"].values():
            if task_state["status"] in {"RUNNING", "RETRYING", "FAILED"}:
                task_state["status"] = "PENDING"
        return state

    def _record(
        self,
        state: dict[str, object],
        *,
        task_name: str | None,
        event: str,
        attempt: int | None = None,
    ) -> None:
        now = _utc_now()
        state["updated_at_utc"] = now
        state["events"].append(
            {
                "sequence": len(state["events"]) + 1,
                "at_utc": now,
                "task": task_name,
                "event": event,
                "attempt": attempt,
            }
        )
        write_json_atomic(self.state_path, state)

    def run(self, *, resume: bool = False) -> dict[str, object]:
        state = self._load_state(resume=resume)
        state["status"] = "RUNNING"
        self._record(state, task_name=None, event="PIPELINE_STARTED")
        task_states = state["tasks"]
        for task in self.tasks:
            task_state = task_states[task.name]
            if task_state["status"] == "SUCCEEDED":
                self._record(state, task_name=task.name, event="TASK_RESUME_SKIPPED")
                continue
            if any(
                task_states[dependency]["status"] != "SUCCEEDED"
                for dependency in task.dependencies
            ):
                raise RuntimeError(f"dependencies are not complete for task {task.name}")
            succeeded = False
            for invocation_attempt in range(1, task.max_attempts + 1):
                task_state["attempts"] = int(task_state["attempts"]) + 1
                attempt = int(task_state["attempts"])
                task_state["status"] = "RUNNING"
                attempt_started_at = _utc_now()
                task_state["started_at_utc"] = attempt_started_at
                self._record(
                    state,
                    task_name=task.name,
                    event="TASK_STARTED",
                    attempt=attempt,
                )
                result = self.executor(task, attempt)
                task_state["last_exit_code"] = result.exit_code
                task_state["last_log_path"] = result.log_path
                task_state["last_log_sha256"] = result.log_sha256
                task_state["attempt_history"].append(
                    {
                        "attempt": attempt,
                        "status": "SUCCEEDED" if result.exit_code == 0 else "FAILED",
                        "started_at_utc": attempt_started_at,
                        "finished_at_utc": _utc_now(),
                        "exit_code": result.exit_code,
                        "log_path": result.log_path,
                        "log_sha256": result.log_sha256,
                    }
                )
                if result.exit_code == 0:
                    task_state["status"] = "SUCCEEDED"
                    task_state["finished_at_utc"] = _utc_now()
                    self._record(
                        state,
                        task_name=task.name,
                        event="TASK_SUCCEEDED",
                        attempt=attempt,
                    )
                    succeeded = True
                    break
                if invocation_attempt < task.max_attempts:
                    task_state["status"] = "RETRYING"
                    self._record(
                        state,
                        task_name=task.name,
                        event="TASK_RETRY_SCHEDULED",
                        attempt=attempt,
                    )
                else:
                    task_state["status"] = "FAILED"
                    task_state["finished_at_utc"] = _utc_now()
                    state["status"] = "FAILED"
                    self._record(
                        state,
                        task_name=task.name,
                        event="TASK_FAILED",
                        attempt=attempt,
                    )
            if not succeeded:
                return state
        state["status"] = "SUCCEEDED"
        self._record(state, task_name=None, event="PIPELINE_SUCCEEDED")
        return state


def render_orchestration_markdown(state: dict[str, object]) -> str:
    lines = [
        "# Managed pipeline retry and resume evidence",
        "",
        f"Pipeline: `{state['pipeline_name']}`",
        f"Run ID: `{state['run_id']}`",
        f"Status: `{state['status']}`",
        f"Resume count: `{state['resume_count']}`",
        "",
        str(state["claims_boundary"]),
        "",
        "## Task states",
        "",
        "| Task | Dependencies | Status | Attempts | Last exit | Log SHA-256 |",
        "| --- | --- | --- | ---: | ---: | --- |",
    ]
    for name, task in state["tasks"].items():
        dependencies = ", ".join(task["dependencies"]) or "-"
        lines.append(
            f"| {name} | {dependencies} | {task['status']} | {task['attempts']} | "
            f"{task['last_exit_code']} | `{task['last_log_sha256']}` |"
        )
    lines.extend(
        [
            "",
            "## Attempt history",
            "",
            "| Task | Attempt | Status | Exit | Log | Log SHA-256 |",
            "| --- | ---: | --- | ---: | --- | --- |",
        ]
    )
    for name, task in state["tasks"].items():
        for attempt in task["attempt_history"]:
            lines.append(
                f"| {name} | {attempt['attempt']} | {attempt['status']} | "
                f"{attempt['exit_code']} | `{attempt['log_path']}` | "
                f"`{attempt['log_sha256']}` |"
            )
    lines.extend(
        [
            "",
            "## State transitions",
            "",
            "| Seq | Task | Event | Attempt |",
            "| ---: | --- | --- | ---: |",
        ]
    )
    for event in state["events"]:
        lines.append(
            f"| {event['sequence']} | {event['task'] or '-'} | "
            f"{event['event']} | {event['attempt'] or '-'} |"
        )
    lines.append("")
    return "\n".join(lines)


def write_orchestration_evidence(
    state: dict[str, object], *, json_path: Path, markdown_path: Path
) -> None:
    write_json_atomic(json_path.resolve(), state)
    markdown_path = markdown_path.resolve()
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.write_text(render_orchestration_markdown(state), encoding="utf-8")
