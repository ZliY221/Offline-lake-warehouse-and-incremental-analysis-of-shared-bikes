"""Run the real local lakehouse pipeline with retries and resumable state."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .orchestration import (
    CommandExecutor,
    PipelineOrchestrator,
    TaskSpec,
    write_orchestration_evidence,
)
from .pipeline_definition import build_pipeline_commands


def _tasks(run_root: Path) -> list[TaskSpec]:
    return [
        TaskSpec(spec.name, spec.dependencies, spec.argv())
        for spec in build_pipeline_commands(run_root)
    ]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the managed local lakehouse pipeline")
    parser.add_argument("--run-root", type=Path, required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--evidence-json", type=Path)
    parser.add_argument("--evidence-markdown", type=Path)
    parser.add_argument(
        "--inject-failure-once",
        choices=("bronze", "silver", "station_dimension", "gold", "quality_gate"),
    )
    args = parser.parse_args()
    run_root = args.run_root.resolve()
    state_path = run_root / "control" / "orchestration-run.json"
    executor = CommandExecutor(
        working_directory=Path.cwd(),
        log_directory=run_root / "logs",
        inject_failure_once=args.inject_failure_once,
    )
    orchestrator = PipelineOrchestrator(
        pipeline_name="bike-trip-lakehouse-full-build",
        run_id=args.run_id,
        tasks=_tasks(run_root),
        state_path=state_path,
        executor=executor,
    )
    state = orchestrator.run(resume=args.resume)
    if bool(args.evidence_json) != bool(args.evidence_markdown):
        raise ValueError("--evidence-json and --evidence-markdown must be used together")
    if args.evidence_json and args.evidence_markdown:
        write_orchestration_evidence(
            state,
            json_path=args.evidence_json,
            markdown_path=args.evidence_markdown,
        )
    print(
        json.dumps(
            {
                "status": state["status"],
                "run_id": state["run_id"],
                "resume_count": state["resume_count"],
                "state": state_path.as_posix(),
                "evidence_json": (
                    args.evidence_json.as_posix() if args.evidence_json else None
                ),
                "task_attempts": {
                    name: task["attempts"] for name, task in state["tasks"].items()
                },
            },
            sort_keys=True,
        )
    )
    if state["status"] != "SUCCEEDED":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
