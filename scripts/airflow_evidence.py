"""Execute the lakehouse DAG with Airflow and save sanitized evidence."""

from __future__ import annotations

import argparse
from importlib.metadata import version
import json
from pathlib import Path
import platform
from typing import Any

from airflow.models import DagBag


def _state_value(state: Any) -> str:
    return str(getattr(state, "value", state)).upper()


def _render_markdown(evidence: dict[str, Any]) -> str:
    lines = [
        "# Airflow DAG 本地执行证据",
        "",
        f"- DAG：`{evidence['dag_id']}`",
        f"- Airflow：`{evidence['airflow_version']}`",
        f"- Python：`{evidence['python_version']}`",
        f"- DAG Run 状态：`{evidence['dag_run_state']}`",
        f"- 受控首次失败任务：`{evidence['failure_injection']}`",
        "- 执行方式：Airflow `DAG.test()`，本地 SQLite 元数据库",
        "",
        "## 任务结果",
        "",
        "| 任务 | 上游依赖 | 状态 | 尝试次数 |",
        "| --- | --- | --- | ---: |",
    ]
    for task in evidence["tasks"]:
        dependencies = ", ".join(task["upstream_task_ids"]) or "-"
        lines.append(
            f"| {task['task_id']} | {dependencies} | {task['state']} | "
            f"{task['try_number']} |"
        )
    lines.extend(
        [
            "",
            "## 数据验收",
            "",
            f"- 质量门禁：`{evidence['quality_report']['overall_status']}`",
            f"- 质量检查数：`{evidence['quality_report']['check_count']}`",
            f"- Bronze 行数：`{evidence['quality_report']['bronze_rows']}`",
            f"- Silver 合法行数：`{evidence['quality_report']['silver_valid_rows']}`",
            "",
            "## 证据边界",
            "",
            "该证据证明 Airflow 3.1.6 能解析并执行完整任务图、保存任务状态并完成原生重试。",
            "它使用单机 `DAG.test()`、SQLite 和 Spark `local[2]`，不代表生产 "
            "Scheduler、分布式 Executor、SLA 或集群容错经验。",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dag-folder", type=Path, required=True)
    parser.add_argument("--dag-id", required=True)
    parser.add_argument("--run-root", type=Path, required=True)
    parser.add_argument("--evidence-json", type=Path, required=True)
    parser.add_argument("--evidence-markdown", type=Path, required=True)
    parser.add_argument("--expected-retry-task", required=True)
    args = parser.parse_args()

    dag_bag = DagBag(dag_folder=str(args.dag_folder), include_examples=False)
    if dag_bag.import_errors:
        raise RuntimeError(f"DAG import failed: {dag_bag.import_errors}")
    dag = dag_bag.get_dag(args.dag_id)
    if dag is None:
        raise RuntimeError(f"DAG not found: {args.dag_id}")

    dag_run = dag.test()
    task_instances = {
        task_instance.task_id: task_instance
        for task_instance in dag_run.get_task_instances()
    }
    tasks = []
    for task_id in sorted(dag.task_dict):
        task = dag.task_dict[task_id]
        task_instance = task_instances[task_id]
        tasks.append(
            {
                "task_id": task_id,
                "upstream_task_ids": sorted(task.upstream_task_ids),
                "state": _state_value(task_instance.state),
                "try_number": task_instance.try_number,
            }
        )

    quality_path = args.run_root / "reports" / "data-quality.json"
    quality = json.loads(quality_path.read_text(encoding="utf-8"))
    evidence = {
        "schema_version": 1,
        "dag_id": dag.dag_id,
        "dag_run_state": _state_value(dag_run.state),
        "airflow_version": version("apache-airflow"),
        "python_version": platform.python_version(),
        "execution_mode": "DAG.test() with local SQLite metadata database",
        "failure_injection": args.expected_retry_task,
        "tasks": tasks,
        "quality_report": {
            "overall_status": quality["overall_status"],
            "check_count": len(quality["checks"]),
            "bronze_rows": quality["dataset_counts"]["bronze_rows"],
            "silver_valid_rows": quality["dataset_counts"]["silver_valid_rows"],
        },
        "claims_boundary": {
            "verified": [
                "Airflow parsed the five-task DAG",
                "Airflow executed all real PySpark stages",
                "Airflow retried the controlled first failure",
                "the blocking quality gate passed",
            ],
            "not_verified": [
                "production scheduler operation",
                "distributed executor behavior",
                "cluster Spark fault tolerance",
                "production SLA or alerting",
            ],
        },
    }
    expected_tasks = {
        "bronze",
        "silver",
        "station_dimension",
        "gold",
        "quality_gate",
    }
    if set(task_instances) != expected_tasks:
        raise RuntimeError(f"Unexpected task set: {set(task_instances)}")
    if evidence["dag_run_state"] != "SUCCESS":
        raise RuntimeError(f"DAG run did not succeed: {evidence['dag_run_state']}")
    if any(task["state"] != "SUCCESS" for task in tasks):
        raise RuntimeError("At least one Airflow task did not succeed")
    retry_task = next(
        task for task in tasks if task["task_id"] == args.expected_retry_task
    )
    if retry_task["try_number"] < 2:
        raise RuntimeError("Controlled failure did not produce an Airflow retry")
    if evidence["quality_report"]["overall_status"] != "PASS":
        raise RuntimeError("Quality gate did not pass")

    args.evidence_json.parent.mkdir(parents=True, exist_ok=True)
    args.evidence_markdown.parent.mkdir(parents=True, exist_ok=True)
    args.evidence_json.write_text(
        json.dumps(evidence, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    args.evidence_markdown.write_text(_render_markdown(evidence), encoding="utf-8")
    print(
        json.dumps(
            {
                "status": "PASS",
                "dag_id": evidence["dag_id"],
                "dag_run_state": evidence["dag_run_state"],
                "retried_task": retry_task["task_id"],
                "retried_task_attempts": retry_task["try_number"],
                "quality_checks": evidence["quality_report"]["check_count"],
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
