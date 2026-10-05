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


def _tasks(run_root: Path) -> list[TaskSpec]:
    lakehouse = run_root / "lakehouse"
    bronze = lakehouse / "bronze" / "trips"
    silver_valid = lakehouse / "silver" / "trips_valid"
    silver_rejected = lakehouse / "silver" / "trips_rejected"
    silver_duplicates = lakehouse / "silver" / "trips_duplicates"
    station_dimension = lakehouse / "dim" / "stations"
    station_rejected = lakehouse / "dim" / "stations_rejected"
    daily_metrics = lakehouse / "gold" / "daily_metrics"
    popular_routes = lakehouse / "gold" / "popular_routes"
    cohort_retention = lakehouse / "gold" / "cohort_retention"
    bronze_manifests = lakehouse / "control" / "bronze_batches"
    backfill_manifests = lakehouse / "control" / "date_backfills"
    quality_report = run_root / "reports" / "data-quality.json"

    def command(module: str, *arguments: object) -> tuple[str, ...]:
        return ("python3", "-m", module, *(str(argument) for argument in arguments))

    return [
        TaskSpec(
            "bronze",
            (),
            command(
                "bike_lakehouse.bronze_cli",
                "--input",
                "data/sample/trips.ndjson",
                "--output",
                bronze,
                "--ingestion-date",
                "2026-10-01",
                "--manifest",
                bronze_manifests,
            ),
        ),
        TaskSpec(
            "silver",
            ("bronze",),
            command(
                "bike_lakehouse.silver_cli",
                "--bronze",
                bronze,
                "--valid",
                silver_valid,
                "--rejected",
                silver_rejected,
                "--duplicates",
                silver_duplicates,
            ),
        ),
        TaskSpec(
            "station_dimension",
            (),
            command(
                "bike_lakehouse.station_dimension_cli",
                "--input",
                "data/sample/stations.ndjson",
                "--input",
                "data/sample/stations-2026-10-02.ndjson",
                "--dimension",
                station_dimension,
                "--rejected",
                station_rejected,
            ),
        ),
        TaskSpec(
            "gold",
            ("silver", "station_dimension"),
            command(
                "bike_lakehouse.gold_cli",
                "--silver-trips",
                silver_valid,
                "--station-dimension",
                station_dimension,
                "--daily-metrics",
                daily_metrics,
                "--popular-routes",
                popular_routes,
                "--cohort-retention",
                cohort_retention,
            ),
        ),
        TaskSpec(
            "quality_gate",
            ("gold",),
            command(
                "bike_lakehouse.quality_report_cli",
                "--bronze",
                bronze,
                "--silver-valid",
                silver_valid,
                "--silver-rejected",
                silver_rejected,
                "--silver-duplicates",
                silver_duplicates,
                "--station-dimension",
                station_dimension,
                "--station-rejected",
                station_rejected,
                "--gold-daily-metrics",
                daily_metrics,
                "--gold-popular-routes",
                popular_routes,
                "--gold-cohort-retention",
                cohort_retention,
                "--bronze-manifest",
                bronze_manifests,
                "--backfill-manifest",
                backfill_manifests,
                "--output",
                quality_report,
                "--fail-on-error",
            ),
        ),
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
