"""Validate the Airflow DAG import, dependency graph, and retry contract."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from airflow.models import DagBag


EXPECTED_DEPENDENCIES = {
    "bronze": [],
    "silver": ["bronze"],
    "station_dimension": [],
    "gold": ["silver", "station_dimension"],
    "quality_gate": ["gold"],
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dag-folder", type=Path, default=Path("dags"))
    parser.add_argument("--dag-id", default="bike_lakehouse_full_build")
    args = parser.parse_args()

    dag_bag = DagBag(dag_folder=str(args.dag_folder.resolve()), include_examples=False)
    if dag_bag.import_errors:
        raise RuntimeError(f"DAG import failed: {dag_bag.import_errors}")
    dag = dag_bag.dags.get(args.dag_id)
    if dag is None:
        raise RuntimeError(f"DAG not found: {args.dag_id}")
    dependencies = {
        task_id: sorted(task.upstream_task_ids)
        for task_id, task in sorted(dag.task_dict.items())
    }
    if dependencies != EXPECTED_DEPENDENCIES:
        raise RuntimeError(f"Unexpected DAG dependencies: {dependencies}")
    retry_counts = {
        task_id: task.retries for task_id, task in sorted(dag.task_dict.items())
    }
    if set(retry_counts.values()) != {1}:
        raise RuntimeError(f"Every task must allow one retry: {retry_counts}")
    print(
        json.dumps(
            {
                "status": "PASS",
                "dag_id": dag.dag_id,
                "task_count": len(dag.task_dict),
                "dependencies": dependencies,
                "retries": retry_counts,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
