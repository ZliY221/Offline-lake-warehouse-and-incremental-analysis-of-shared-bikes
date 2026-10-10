"""Airflow adapter for the canonical bike lakehouse task graph."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import subprocess

from airflow.sdk import dag, task

from bike_lakehouse.pipeline_definition import build_pipeline_commands


DAG_ID = "bike_lakehouse_full_build"
REPO_ROOT = Path(os.environ["BIKE_LAKEHOUSE_REPO_ROOT"]).resolve()
RUN_ROOT = Path(
    os.environ.get(
        "BIKE_LAKEHOUSE_AIRFLOW_RUN_ROOT",
        REPO_ROOT / "build" / "airflow-dag-run",
    )
).resolve()


def _task_environment() -> dict[str, str]:
    tools_root = REPO_ROOT / "build" / "tools"
    environment = os.environ.copy()
    environment.update(
        {
            "JAVA_HOME": str(tools_root / "jdk17"),
            "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            "PYTHONPATH": os.pathsep.join(
                (str(tools_root / "python-packages"), str(REPO_ROOT / "src"))
            ),
            "PYSPARK_PYTHON": "/usr/bin/python3",
            "SPARK_LOCAL_IP": "127.0.0.1",
        }
    )
    return environment


def _inject_controlled_failure(stage_name: str) -> None:
    if os.environ.get("BIKE_LAKEHOUSE_AIRFLOW_FAIL_ONCE") != stage_name:
        return
    sentinel = RUN_ROOT / "control" / f"airflow-{stage_name}-failure-injected"
    sentinel.parent.mkdir(parents=True, exist_ok=True)
    if sentinel.exists():
        return
    sentinel.write_text("controlled failure for retry evidence\n", encoding="utf-8")
    raise RuntimeError(f"controlled first-attempt failure for {stage_name}")


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
        _inject_controlled_failure(stage_name)
        command = ["/usr/bin/python3", "-m", module, *arguments]
        subprocess.run(
            command,
            cwd=REPO_ROOT,
            env=_task_environment(),
            check=True,
        )

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
