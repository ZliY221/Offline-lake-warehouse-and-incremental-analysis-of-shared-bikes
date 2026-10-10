"""Validate the Airflow DAG import, dependency graph, and retry contract."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from airflow.models import DagBag


EXPECTED_DAGS = {
    "bike_lakehouse_full_build": {
        "dependencies": {
            "bronze": [],
            "silver": ["bronze"],
            "station_dimension": [],
            "gold": ["silver", "station_dimension"],
            "quality_gate": ["gold"],
        },
        "retries": {
            "bronze": 1,
            "silver": 1,
            "station_dimension": 1,
            "gold": 1,
            "quality_gate": 1,
        },
    },
    "bike_lakehouse_date_backfill": {
        "dependencies": {
            "validate_request": [],
            "backfill": ["validate_request"],
            "quality_gate": ["backfill"],
        },
        "retries": {
            "validate_request": 0,
            "backfill": 1,
            "quality_gate": 0,
        },
    },
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dag-folder", type=Path, default=Path("dags"))
    args = parser.parse_args()

    dag_bag = DagBag(dag_folder=str(args.dag_folder.resolve()), include_examples=False)
    if dag_bag.import_errors:
        raise RuntimeError(f"DAG import failed: {dag_bag.import_errors}")
    results = {}
    for dag_id, expected in EXPECTED_DAGS.items():
        dag = dag_bag.dags.get(dag_id)
        if dag is None:
            raise RuntimeError(f"DAG not found: {dag_id}")
        dependencies = {
            task_id: sorted(task.upstream_task_ids)
            for task_id, task in sorted(dag.task_dict.items())
        }
        if dependencies != expected["dependencies"]:
            raise RuntimeError(f"Unexpected dependencies for {dag_id}: {dependencies}")
        retry_counts = {
            task_id: task.retries for task_id, task in sorted(dag.task_dict.items())
        }
        if retry_counts != expected["retries"]:
            raise RuntimeError(f"Unexpected retries for {dag_id}: {retry_counts}")
        results[dag_id] = {
            "task_count": len(dag.task_dict),
            "dependencies": dependencies,
            "retries": retry_counts,
            "params": sorted(dag.params),
        }
    backfill_params = results["bike_lakehouse_date_backfill"]["params"]
    if backfill_params != ["input_path", "target_date"]:
        raise RuntimeError(f"Unexpected backfill params: {backfill_params}")
    print(
        json.dumps(
            {
                "status": "PASS",
                "dag_count": len(results),
                "dags": results,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
