"""Build a standalone operations snapshot for an offline lakehouse run."""

from __future__ import annotations

from html import escape
import json
from pathlib import Path
from typing import Any
from uuid import uuid4

from .batch_manifest import utc_timestamp, write_json_atomic
from .quality_actions import build_remediation_plan


TASK_ORDER = ("bronze", "silver", "station_dimension", "gold", "quality_gate")


def _require_mapping(payload: Any, label: str) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError(f"{label} must be a JSON object")
    return payload


def build_operations_snapshot(
    quality_report: dict[str, Any],
    orchestration_state: dict[str, Any],
    remediation_plan: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Combine quality, orchestration and remediation evidence for one run."""
    quality = _require_mapping(quality_report, "quality report")
    orchestration = _require_mapping(orchestration_state, "orchestration state")
    remediation = remediation_plan or build_remediation_plan(quality)
    remediation = _require_mapping(remediation, "remediation plan")

    checks = quality.get("checks")
    tasks = orchestration.get("tasks")
    actions = remediation.get("actions")
    if not isinstance(checks, list):
        raise ValueError("quality report checks must be a list")
    if not isinstance(tasks, dict):
        raise ValueError("orchestration tasks must be an object")
    if not isinstance(actions, list):
        raise ValueError("remediation actions must be a list")

    task_rows = []
    task_names = [name for name in TASK_ORDER if name in tasks]
    task_names.extend(sorted(set(tasks) - set(task_names)))
    for name in task_names:
        task = _require_mapping(tasks[name], f"task {name}")
        task_rows.append({
            "name": name,
            "status": task.get("status", "UNKNOWN"),
            "attempts": int(task.get("attempts", 0)),
            "dependencies": list(task.get("dependencies", [])),
            "started_at_utc": task.get("started_at_utc"),
            "finished_at_utc": task.get("finished_at_utc"),
            "log_sha256": task.get("last_log_sha256"),
        })

    check_status_counts: dict[str, int] = {}
    for check in checks:
        if not isinstance(check, dict):
            raise ValueError("every quality check must be an object")
        status = str(check.get("status", "UNKNOWN"))
        check_status_counts[status] = check_status_counts.get(status, 0) + 1

    pipeline_ready = orchestration.get("status") == "SUCCEEDED" and all(
        task["status"] == "SUCCEEDED" for task in task_rows
    )
    quality_ready = quality.get("overall_status") == "PASS"
    release_ready = pipeline_ready and quality_ready and not actions
    return {
        "snapshot_version": "1.0",
        "generated_at_utc": utc_timestamp(),
        "status": "READY" if release_ready else "ATTENTION_REQUIRED",
        "release_ready": release_ready,
        "run": {
            "pipeline_name": orchestration.get("pipeline_name"),
            "run_id": orchestration.get("run_id"),
            "pipeline_status": orchestration.get("status", "UNKNOWN"),
            "quality_status": quality.get("overall_status", "UNKNOWN"),
            "resume_count": int(orchestration.get("resume_count", 0)),
        },
        "summary": {
            "task_count": len(task_rows),
            "succeeded_task_count": sum(task["status"] == "SUCCEEDED" for task in task_rows),
            "quality_check_count": len(checks),
            "failed_quality_check_count": check_status_counts.get("FAIL", 0),
            "remediation_action_count": len(actions),
            "partition_count": len(quality.get("bronze_partition_rows", {})),
        },
        "tasks": task_rows,
        "quality": {
            "status_counts": dict(sorted(check_status_counts.items())),
            "checks": checks,
            "dataset_counts": quality.get("dataset_counts", {}),
            "bronze_partition_rows": quality.get("bronze_partition_rows", {}),
            "manifest_summary": quality.get("manifest_summary", {}),
        },
        "remediation": {
            "ready_for_release": bool(remediation.get("ready_for_release", not actions)),
            "actions": actions,
        },
    }


def render_operations_html(snapshot: dict[str, Any]) -> str:
    """Render a dependency-free HTML report that can be opened locally."""
    status = snapshot["status"]
    summary = snapshot["summary"]
    run = snapshot["run"]
    task_rows = "".join(
        "<tr>"
        f"<td>{escape(task['name'])}</td><td><span class='state'>{escape(task['status'])}</span></td>"
        f"<td>{task['attempts']}</td><td>{escape(', '.join(task['dependencies']) or '—')}</td>"
        "</tr>"
        for task in snapshot["tasks"]
    )
    action_rows = "".join(
        "<li>"
        f"<b>{escape(str(action['priority']))}</b>"
        f"<span>{escape(str(action['check_name']))}</span>"
        f"<p>{escape(str(action['recommendation']))}</p>"
        "</li>"
        for action in snapshot["remediation"]["actions"]
    ) or "<li class='empty'>当前没有待处理质量动作</li>"
    counts = snapshot["quality"]["dataset_counts"]
    dataset_cards = "".join(
        f"<div><span>{escape(str(name))}</span><strong>{int(value):,}</strong></div>"
        for name, value in sorted(counts.items())
    ) or "<div><span>数据集</span><strong>暂无统计</strong></div>"
    embedded = json.dumps(snapshot, ensure_ascii=False, sort_keys=True).replace("</", "<\\/")
    return f"""<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>共享单车离线湖仓运行控制中心</title><style>
:root{{font-family:Inter,'Microsoft YaHei',sans-serif;color:#17324d;background:#eef3f6}}*{{box-sizing:border-box}}body{{margin:0}}header{{padding:38px max(28px,7vw);background:#12344f;color:white;border-bottom:5px solid #27a39a}}header p{{color:#a9c7d8}}main{{max-width:1200px;margin:auto;padding:28px}}.grid{{display:grid;grid-template-columns:repeat(4,1fr);gap:12px}}.card,section{{background:white;border:1px solid #d9e4ea;border-radius:12px;box-shadow:0 12px 36px #17324d0d}}.card{{padding:18px}}.card span{{display:block;color:#718696;font-size:12px}}.card strong{{display:block;margin-top:10px;font-size:27px}}section{{margin-top:16px;padding:22px}}h2{{margin:0 0 16px}}table{{width:100%;border-collapse:collapse}}th,td{{padding:11px;text-align:left;border-bottom:1px solid #e6edf1}}.state{{font-weight:700;color:#087f72}}.datasets{{display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:10px}}.datasets div{{padding:14px;background:#f4f8fa}}.datasets span{{display:block;color:#718696;font-size:11px}}.datasets strong{{display:block;margin-top:6px}}ol{{padding:0;list-style:none}}li{{padding:13px 0;border-bottom:1px solid #e6edf1}}li b{{display:inline-block;margin-right:10px;color:#b45309}}li p{{margin:6px 0 0;color:#607586}}.empty{{color:#087f72}}@media(max-width:760px){{.grid{{grid-template-columns:1fr 1fr}}main{{padding:16px}}}}
</style></head><body><header><small>OFFLINE LAKEHOUSE · RUN CONTROL</small><h1>共享单车离线湖仓运行控制中心</h1><p>运行 {escape(str(run.get('run_id') or '未命名'))} · {escape(str(run['pipeline_status']))} · {escape(str(run['quality_status']))}</p></header>
<main><div class="grid"><div class="card"><span>发布状态</span><strong>{escape(status)}</strong></div><div class="card"><span>成功任务</span><strong>{summary['succeeded_task_count']} / {summary['task_count']}</strong></div><div class="card"><span>质量检查</span><strong>{summary['quality_check_count']}</strong></div><div class="card"><span>待处理动作</span><strong>{summary['remediation_action_count']}</strong></div></div>
<section><h2>离线任务链路</h2><table><thead><tr><th>任务</th><th>状态</th><th>尝试次数</th><th>依赖</th></tr></thead><tbody>{task_rows}</tbody></table></section>
<section><h2>分层数据规模</h2><div class="datasets">{dataset_cards}</div></section>
<section><h2>质量处置队列</h2><ol>{action_rows}</ol></section></main><script type="application/json" id="operations-snapshot">{embedded}</script></body></html>"""


def write_operations_bundle(
    snapshot: dict[str, Any], json_path: Path, html_path: Path
) -> None:
    write_json_atomic(Path(json_path).resolve(), snapshot)
    target = Path(html_path).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.parent / f".{target.name}.{uuid4().hex}.tmp"
    try:
        temporary.write_text(render_operations_html(snapshot), encoding="utf-8")
        temporary.replace(target)
    finally:
        if temporary.exists():
            temporary.unlink()
