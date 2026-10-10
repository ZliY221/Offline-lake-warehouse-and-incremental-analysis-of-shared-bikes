"""Run successful and failed Airflow backfills and save sanitized evidence."""

from __future__ import annotations

import argparse
import hashlib
from importlib.metadata import version
import json
from pathlib import Path
import platform
from typing import Any

from airflow.models import DagBag


def _state_value(state: Any) -> str:
    return str(getattr(state, "value", state)).upper()


def _task_results(dag: Any, dag_run: Any) -> list[dict[str, object]]:
    instances = {
        instance.task_id: instance for instance in dag_run.get_task_instances()
    }
    return [
        {
            "task_id": task_id,
            "state": _state_value(instances[task_id].state),
            "try_number": instances[task_id].try_number,
        }
        for task_id in sorted(dag.task_dict)
    ]


def _directory_fingerprint(path: Path) -> dict[str, str]:
    return {
        file.relative_to(path).as_posix(): hashlib.sha256(file.read_bytes()).hexdigest()
        for file in sorted(path.rglob("*"))
        if file.is_file()
    }


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def _find_manifest(path: Path, target_date: str, status: str) -> dict[str, object]:
    for manifest_path in sorted(path.glob("*.json")):
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("target_date") == target_date and manifest.get("status") == status:
            return manifest
    raise RuntimeError(f"No {status} manifest for {target_date}")


def _render_markdown(evidence: dict[str, Any]) -> str:
    successful_tasks = evidence["successful_backfill"]["tasks"]
    failed_tasks = evidence["failed_backfill"]["tasks"]
    lines = [
        "# Airflow 参数化回填与失败审计证据",
        "",
        f"- Airflow：`{evidence['airflow_version']}`",
        f"- Python：`{evidence['python_version']}`",
        f"- 回填 DAG：`{evidence['dag_id']}`",
        f"- 成功参数：`{json.dumps(evidence['successful_backfill']['params'], ensure_ascii=False)}`",
        f"- 失败参数：`{json.dumps(evidence['failed_backfill']['params'], ensure_ascii=False)}`",
        "",
        "## 成功回填",
        "",
        "| 任务 | 状态 | 尝试次数 |",
        "| --- | --- | ---: |",
    ]
    lines.extend(
        f"| {task['task_id']} | {task['state']} | {task['try_number']} |"
        for task in successful_tasks
    )
    lines.extend(
        [
            "",
            f"成功 DAG Run：`{evidence['successful_backfill']['state']}`；"
            f"质量门禁：`{evidence['successful_backfill']['quality_status']}`。",
            "",
            "## 失败回填",
            "",
            "| 任务 | 状态 | 尝试次数 |",
            "| --- | --- | ---: |",
        ]
    )
    lines.extend(
        f"| {task['task_id']} | {task['state']} | {task['try_number']} |"
        for task in failed_tasks
    )
    lines.extend(
        [
            "",
            f"失败 DAG Run：`{evidence['failed_backfill']['state']}`；"
            f"写前保护后 Bronze 未变化：`{evidence['failed_backfill']['bronze_unchanged']}`。",
            "",
            "## 回调审计",
            "",
            f"- 重试事件：`{evidence['callback_summary']['retry_events']}`",
            f"- 最终失败事件：`{evidence['callback_summary']['failure_events']}`",
            "- 审计只保存 DAG、任务、Run ID、尝试次数和上下文可提供的异常类型；未提供时为 `null`，不复制异常正文或数据内容。",
            "",
            "## 证据边界",
            "",
            "该实验使用 Airflow `DAG.test()`、SQLite 和 Spark `local[2]`。回调写入本地 JSONL，"
            "不是邮件、短信或企业告警平台，也未验证并发补数、权限审批或生产 SLA。",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dag-folder", type=Path, required=True)
    parser.add_argument("--run-root", type=Path, required=True)
    parser.add_argument("--evidence-json", type=Path, required=True)
    parser.add_argument("--evidence-markdown", type=Path, required=True)
    parser.add_argument("--input-path", default="data/sample/trips.ndjson")
    parser.add_argument("--success-date", default="2026-10-01")
    parser.add_argument("--failure-date", default="2026-10-02")
    args = parser.parse_args()

    dag_bag = DagBag(dag_folder=str(args.dag_folder), include_examples=False)
    if dag_bag.import_errors:
        raise RuntimeError(f"DAG import failed: {dag_bag.import_errors}")
    full_dag = dag_bag.get_dag("bike_lakehouse_full_build")
    backfill_dag = dag_bag.get_dag("bike_lakehouse_date_backfill")
    if full_dag is None or backfill_dag is None:
        raise RuntimeError("Required Airflow DAG was not found")

    baseline_run = full_dag.test()
    if _state_value(baseline_run.state) != "SUCCESS":
        raise RuntimeError("Baseline full-build DAG failed")

    success_params = {
        "target_date": args.success_date,
        "input_path": args.input_path,
    }
    successful_run = backfill_dag.test(run_conf=success_params)
    successful_tasks = _task_results(backfill_dag, successful_run)
    quality_path = args.run_root / "reports" / "data-quality.json"
    quality = json.loads(quality_path.read_text(encoding="utf-8"))
    bronze_path = args.run_root / "lakehouse" / "bronze" / "trips"
    before_failed_run = _directory_fingerprint(bronze_path)

    failure_params = {
        "target_date": args.failure_date,
        "input_path": args.input_path,
    }
    failed_run = backfill_dag.test(run_conf=failure_params)
    failed_tasks = _task_results(backfill_dag, failed_run)
    after_failed_run = _directory_fingerprint(bronze_path)

    callback_path = args.run_root / "control" / "airflow-callback-audit.jsonl"
    callback_events = _read_jsonl(callback_path)
    retry_events = [event for event in callback_events if event["event"] == "TASK_RETRY"]
    failure_events = [
        event for event in callback_events if event["event"] == "TASK_FAILURE"
    ]
    manifest_path = args.run_root / "lakehouse" / "control" / "date_backfills"
    successful_manifest = _find_manifest(manifest_path, args.success_date, "SUCCEEDED")
    failed_manifest = _find_manifest(manifest_path, args.failure_date, "FAILED")
    evidence = {
        "schema_version": 1,
        "dag_id": backfill_dag.dag_id,
        "airflow_version": version("apache-airflow"),
        "python_version": platform.python_version(),
        "execution_mode": "DAG.test() with local SQLite metadata database",
        "successful_backfill": {
            "params": success_params,
            "state": _state_value(successful_run.state),
            "tasks": successful_tasks,
            "manifest_status": successful_manifest["status"],
            "quality_status": quality["overall_status"],
            "quality_checks": len(quality["checks"]),
        },
        "failed_backfill": {
            "params": failure_params,
            "state": _state_value(failed_run.state),
            "tasks": failed_tasks,
            "manifest_status": failed_manifest["status"],
            "manifest_error_type": failed_manifest["error_type"],
            "bronze_unchanged": before_failed_run == after_failed_run,
        },
        "callback_summary": {
            "retry_events": len(retry_events),
            "failure_events": len(failure_events),
            "events": callback_events,
        },
        "claims_boundary": {
            "verified": [
                "typed manual-run parameters reached the backfill task",
                "Airflow retried a controlled transient failure",
                "a successful backfill passed the blocking quality gate",
                "a date mismatch failed and emitted a sanitized callback audit",
                "the failed backfill did not change Bronze files",
            ],
            "not_verified": [
                "production alert delivery",
                "concurrent backfill locking",
                "approval or authorization workflow",
                "production scheduler or SLA operation",
            ],
        },
    }

    successful_backfill = next(
        task for task in successful_tasks if task["task_id"] == "backfill"
    )
    failed_backfill = next(task for task in failed_tasks if task["task_id"] == "backfill")
    if evidence["successful_backfill"]["state"] != "SUCCESS":
        raise RuntimeError("Parameterized backfill did not succeed")
    if successful_backfill["try_number"] < 2:
        raise RuntimeError("Successful backfill did not exercise Airflow retry")
    if evidence["failed_backfill"]["state"] != "FAILED":
        raise RuntimeError("Invalid backfill did not fail")
    if failed_backfill["state"] != "FAILED" or failed_backfill["try_number"] < 2:
        raise RuntimeError("Invalid backfill did not exhaust its attempts")
    if not retry_events or not failure_events:
        raise RuntimeError("Expected retry and failure callback audits were not written")
    if not evidence["failed_backfill"]["bronze_unchanged"]:
        raise RuntimeError("Failed backfill changed Bronze files")
    if evidence["successful_backfill"]["quality_status"] != "PASS":
        raise RuntimeError("Successful backfill did not pass quality checks")

    args.evidence_json.parent.mkdir(parents=True, exist_ok=True)
    args.evidence_markdown.parent.mkdir(parents=True, exist_ok=True)
    args.evidence_json.write_text(
        json.dumps(evidence, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    args.evidence_markdown.write_text(_render_markdown(evidence), encoding="utf-8")
    print(
        json.dumps(
            {
                "status": "PASS",
                "successful_backfill": evidence["successful_backfill"]["state"],
                "failed_backfill": evidence["failed_backfill"]["state"],
                "bronze_unchanged_after_failure": evidence["failed_backfill"][
                    "bronze_unchanged"
                ],
                "retry_events": len(retry_events),
                "failure_events": len(failure_events),
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
