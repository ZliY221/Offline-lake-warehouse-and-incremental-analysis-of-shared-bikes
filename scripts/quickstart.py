#!/usr/bin/env python3
"""Run the portable source flow or the complete local bike lakehouse pipeline."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
VERSION = "0.11.2"


def _output_path(value: Path) -> Path:
    return value.resolve() if value.is_absolute() else (ROOT / value).resolve()


def _environment() -> dict[str, str]:
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(ROOT / "src")
    environment.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    return environment


def _run(label: str, command: list[str]) -> None:
    print(f"==> {label}", flush=True)
    completed = subprocess.run(
        command,
        cwd=ROOT,
        env=_environment(),
        check=False,
    )
    if completed.returncode != 0:
        raise SystemExit(f"{label} failed with exit code {completed.returncode}")


def _write_json_atomic(path: Path, document: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(document, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    temporary.replace(path)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _doctor(profile: str, output: Path) -> None:
    _run(
        f"Inspect {profile} environment",
        [
            sys.executable,
            "scripts/project_doctor.py",
            "--profile",
            profile,
            "--json-output",
            str(output / f"{profile}-doctor.json"),
            "--markdown-output",
            str(output / f"{profile}-doctor.md"),
        ],
    )


def _source_flow(output: Path, count: int, seed: int) -> dict[str, Any]:
    _doctor("source", output)
    trips = output / "source" / "trips.ndjson"
    stations = output / "source" / "stations.ndjson"
    _run(
        "Generate validated trip and station data",
        [
            sys.executable,
            "-m",
            "bike_lakehouse.cli",
            "--date",
            "2026-10-01",
            "--count",
            str(count),
            "--seed",
            str(seed),
            "--trips-output",
            str(trips),
            "--stations-output",
            str(stations),
        ],
    )
    trip_count = sum(1 for line in trips.read_text(encoding="utf-8").splitlines() if line)
    station_count = sum(
        1 for line in stations.read_text(encoding="utf-8").splitlines() if line
    )
    if trip_count != count or station_count == 0:
        raise SystemExit("source verification failed: generated row counts are invalid")
    return {
        "trip_count": trip_count,
        "station_count": station_count,
        "trips_sha256": _sha256(trips),
        "stations_sha256": _sha256(stations),
        "outputs": {"trips": str(trips), "stations": str(stations)},
    }


def _pipeline_flow(output: Path) -> dict[str, Any]:
    run_root = output / "pipeline"
    evidence_json = output / "pipeline-evidence.json"
    evidence_markdown = output / "pipeline-evidence.md"
    state_path = run_root / "control" / "orchestration-run.json"
    run_id = "quickstart-v1"
    resume = False
    if state_path.is_file():
        existing = json.loads(state_path.read_text(encoding="utf-8"))
        run_id = str(existing.get("run_id", run_id))
        resume = True

    if os.name == "nt":
        relative_run_root = run_root.relative_to(ROOT).as_posix()
        relative_evidence_json = evidence_json.relative_to(ROOT).as_posix()
        relative_evidence_markdown = evidence_markdown.relative_to(ROOT).as_posix()
        shell = shutil.which("pwsh") or shutil.which("powershell")
        if shell is None:
            raise SystemExit("PowerShell is required to start the Windows WSL pipeline")
        command = [
            shell,
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(ROOT / "scripts" / "run-managed-pipeline.ps1"),
            "-RunRoot",
            relative_run_root,
            "-RunId",
            run_id,
            "-EvidenceJson",
            relative_evidence_json,
            "-EvidenceMarkdown",
            relative_evidence_markdown,
        ]
        if resume:
            command.append("-Resume")
        _run("Build the complete lakehouse in WSL", command)
        _doctor("pipeline", output)
    else:
        _doctor("pipeline", output)
        command = [
            sys.executable,
            "-m",
            "bike_lakehouse.managed_pipeline_cli",
            "--run-root",
            str(run_root),
            "--run-id",
            run_id,
            "--evidence-json",
            str(evidence_json),
            "--evidence-markdown",
            str(evidence_markdown),
        ]
        if resume:
            command.append("--resume")
        _run("Build the complete local lakehouse", command)

    state = json.loads(state_path.read_text(encoding="utf-8"))
    quality_path = run_root / "reports" / "data-quality.json"
    quality = json.loads(quality_path.read_text(encoding="utf-8"))
    if state.get("status") != "SUCCEEDED" or quality.get("overall_status") != "PASS":
        raise SystemExit(
            "pipeline verification failed: orchestration or quality gate is incomplete"
        )
    return {
        "run_id": state["run_id"],
        "orchestration_status": state["status"],
        "quality_status": quality["overall_status"],
        "task_attempts": {
            name: task["attempts"] for name, task in state["tasks"].items()
        },
        "outputs": {
            "orchestration_state": str(state_path),
            "quality_report": str(quality_path),
            "pipeline_evidence": str(evidence_json),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the portable source flow or complete bike lakehouse pipeline"
    )
    parser.add_argument("--mode", choices=("source", "pipeline"), default="source")
    parser.add_argument("--output-dir", type=Path, default=Path("build/quickstart"))
    parser.add_argument("--count", type=int, default=20)
    parser.add_argument("--seed", type=int, default=2027)
    args = parser.parse_args()
    if args.count <= 0:
        parser.error("--count must be greater than zero")
    output = _output_path(args.output_dir)
    if args.mode == "pipeline":
        try:
            output.relative_to(ROOT)
        except ValueError:
            parser.error("pipeline output must be inside the repository")

    details = (
        _source_flow(output, args.count, args.seed)
        if args.mode == "source"
        else _pipeline_flow(output)
    )
    summary = {
        "status": "COMPLETE",
        "project": "bike-trip-lakehouse",
        "version": VERSION,
        "mode": args.mode,
        **details,
    }
    summary_path = output / f"{args.mode}-summary.json"
    _write_json_atomic(summary_path, summary)
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
