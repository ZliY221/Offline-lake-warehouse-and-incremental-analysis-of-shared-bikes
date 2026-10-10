"""Runtime helpers shared by Airflow DAG adapters without importing Airflow."""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
from typing import Sequence


def task_environment(repo_root: Path) -> dict[str, str]:
    tools_root = repo_root / "build" / "tools"
    environment = os.environ.copy()
    environment.update(
        {
            "JAVA_HOME": str(tools_root / "jdk17"),
            "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            "PYTHONPATH": os.pathsep.join(
                (str(tools_root / "python-packages"), str(repo_root / "src"))
            ),
            "PYSPARK_PYTHON": "/usr/bin/python3",
            "SPARK_LOCAL_IP": "127.0.0.1",
        }
    )
    return environment


def run_python_module(repo_root: Path, module: str, arguments: Sequence[str]) -> None:
    subprocess.run(
        ["/usr/bin/python3", "-m", module, *arguments],
        cwd=repo_root,
        env=task_environment(repo_root),
        check=True,
    )


def inject_controlled_failure(run_root: Path, stage_name: str) -> None:
    if os.environ.get("BIKE_LAKEHOUSE_AIRFLOW_FAIL_ONCE") != stage_name:
        return
    sentinel = run_root / "control" / f"airflow-{stage_name}-failure-injected"
    sentinel.parent.mkdir(parents=True, exist_ok=True)
    if sentinel.exists():
        return
    sentinel.write_text("controlled failure for retry evidence\n", encoding="utf-8")
    raise RuntimeError(f"controlled first-attempt failure for {stage_name}")


def resolve_data_input(repo_root: Path, input_value: str) -> Path:
    """Resolve an allowlisted repository data file and reject path traversal."""

    raw_path = Path(input_value)
    if raw_path.is_absolute():
        raise ValueError("Airflow backfill input must be repository-relative")
    resolved = (repo_root / raw_path).resolve()
    data_root = (repo_root / "data").resolve()
    if not resolved.is_relative_to(data_root):
        raise ValueError("Airflow backfill input must stay under data/")
    if not resolved.is_file():
        raise ValueError(f"Airflow backfill input does not exist: {input_value}")
    return resolved
