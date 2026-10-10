"""Airflow adapter for the canonical bike lakehouse task graph."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import os
from pathlib import Path

from airflow.sdk import dag, task

from bike_lakehouse.airflow_runtime import (
    inject_controlled_failure,
    run_python_module,
)
from bike_lakehouse.pipeline_definition import build_pipeline_commands


DAG_ID = "bike_lakehouse_full_build"
REPO_ROOT = Path(os.environ["BIKE_LAKEHOUSE_REPO_ROOT"]).resolve()
RUN_ROOT = Path(
    os.environ.get(
        "BIKE_LAKEHOUSE_AIRFLOW_RUN_ROOT",
        REPO_ROOT / "build" / "airflow-dag-run",
    )
).resolve()


@dag(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=("portfolio", "pyspark", "lakehouse"),
    description="Five-stage PySpark lakehouse build with a blocking quality gate",
)
def bike_lakehouse_full_build():
    @task(retries=1, retry_delay=timedelta(seconds=1))
    def run_stage(stage_name: str, module: str, arguments: list[str]) -> None:
        inject_controlled_failure(RUN_ROOT, stage_name)
        run_python_module(REPO_ROOT, module, arguments)

    nodes = {}
    for spec in build_pipeline_commands(RUN_ROOT):
        nodes[spec.name] = run_stage.override(task_id=spec.name)(
            spec.name,
            spec.module,
            list(spec.arguments),
        )
    for spec in build_pipeline_commands(RUN_ROOT):
        for dependency in spec.dependencies:
            nodes[dependency] >> nodes[spec.name]


bike_lakehouse_full_build()
