"""Parameterized Airflow DAG for a controlled single-date lakehouse backfill."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import logging
import os
from pathlib import Path

from airflow.sdk import Param, dag, get_current_context, task

from bike_lakehouse.airflow_audit import append_callback_event, build_callback_event
from bike_lakehouse.airflow_runtime import (
    inject_controlled_failure,
    resolve_data_input,
    run_python_module,
)
from bike_lakehouse.pipeline_definition import build_pipeline_commands


DAG_ID = "bike_lakehouse_date_backfill"
REPO_ROOT = Path(os.environ["BIKE_LAKEHOUSE_REPO_ROOT"]).resolve()
RUN_ROOT = Path(
    os.environ.get(
        "BIKE_LAKEHOUSE_AIRFLOW_RUN_ROOT",
        REPO_ROOT / "build" / "airflow-backfill-run",
    )
).resolve()
CALLBACK_AUDIT = RUN_ROOT / "control" / "airflow-callback-audit.jsonl"


def _write_callback(event: str, context: dict[str, object]) -> None:
    try:
        append_callback_event(CALLBACK_AUDIT, build_callback_event(event, context))
    except Exception:
        logging.getLogger("airflow.task").exception(
            "Unable to write the sanitized backfill callback audit"
        )


def task_retry_audit(context: dict[str, object]) -> None:
    _write_callback("TASK_RETRY", context)


def task_failure_audit(context: dict[str, object]) -> None:
    _write_callback("TASK_FAILURE", context)


def _backfill_arguments(input_path: str, target_date: str) -> list[str]:
    lakehouse = RUN_ROOT / "lakehouse"
    return [
        "--input",
        input_path,
        "--target-date",
        target_date,
        "--bronze",
        str(lakehouse / "bronze" / "trips"),
        "--bronze-manifest",
        str(lakehouse / "control" / "bronze_batches"),
        "--silver-valid",
        str(lakehouse / "silver" / "trips_valid"),
        "--silver-rejected",
        str(lakehouse / "silver" / "trips_rejected"),
        "--silver-duplicates",
        str(lakehouse / "silver" / "trips_duplicates"),
        "--station-dimension",
        str(lakehouse / "dim" / "stations"),
        "--gold-daily-metrics",
        str(lakehouse / "gold" / "daily_metrics"),
        "--gold-popular-routes",
        str(lakehouse / "gold" / "popular_routes"),
        "--gold-cohort-retention",
        str(lakehouse / "gold" / "cohort_retention"),
        "--backfill-manifest",
        str(lakehouse / "control" / "date_backfills"),
    ]


@dag(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    params={
        "target_date": Param("2026-10-01", type="string", format="date"),
        "input_path": Param(
            "data/sample/trips.ndjson",
            type="string",
            minLength=1,
        ),
    },
    tags=("portfolio", "pyspark", "backfill"),
    description="Parameterized single-date backfill with blocking quality checks",
)
def bike_lakehouse_date_backfill():
    @task(on_failure_callback=task_failure_audit)
    def validate_request() -> dict[str, str]:
        params = get_current_context()["params"]
        target_date = date.fromisoformat(str(params["target_date"]))
        input_path = resolve_data_input(REPO_ROOT, str(params["input_path"]))
        return {
            "target_date": target_date.isoformat(),
            "input_path": input_path.relative_to(REPO_ROOT).as_posix(),
        }

    @task(
        retries=1,
        retry_delay=timedelta(seconds=1),
        on_retry_callback=task_retry_audit,
        on_failure_callback=task_failure_audit,
    )
    def backfill(request: dict[str, str]) -> None:
        inject_controlled_failure(RUN_ROOT, "backfill")
        run_python_module(
            REPO_ROOT,
            "bike_lakehouse.backfill_cli",
            _backfill_arguments(request["input_path"], request["target_date"]),
        )

    @task(on_failure_callback=task_failure_audit)
    def quality_gate() -> None:
        quality = build_pipeline_commands(RUN_ROOT)[-1]
        run_python_module(REPO_ROOT, quality.module, quality.arguments)

    validated = validate_request()
    backfill_task = backfill(validated)
    quality_task = quality_gate()
    backfill_task >> quality_task


bike_lakehouse_date_backfill()
