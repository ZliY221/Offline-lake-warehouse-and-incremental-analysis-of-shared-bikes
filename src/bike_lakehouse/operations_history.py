"""Aggregate offline operations snapshots into a release history report."""

from __future__ import annotations

from html import escape
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4

from .batch_manifest import utc_timestamp, write_json_atomic


def build_operations_history(
    snapshots: Iterable[dict[str, Any]], *, recent_window: int = 5, readiness_threshold: float = 0.8
) -> dict[str, Any]:
    if not 1 <= recent_window <= 50:
        raise ValueError("recent_window must be between 1 and 50")
    if not 0.0 <= readiness_threshold <= 1.0:
        raise ValueError("readiness_threshold must be between 0 and 1")
    rows: list[dict[str, Any]] = []
    for snapshot in snapshots:
        if not isinstance(snapshot, dict):
            raise ValueError("every operations snapshot must be a JSON object")
        run = snapshot.get("run")
        summary = snapshot.get("summary")
        if not isinstance(run, dict) or not isinstance(summary, dict):
            raise ValueError("snapshot run and summary must be JSON objects")
        rows.append({
            "generated_at_utc": str(snapshot.get("generated_at_utc", "")),
            "run_id": str(run.get("run_id") or "未命名"),
            "status": str(snapshot.get("status", "UNKNOWN")),
            "release_ready": bool(snapshot.get("release_ready", False)),
            "pipeline_status": str(run.get("pipeline_status", "UNKNOWN")),
            "quality_status": str(run.get("quality_status", "UNKNOWN")),
            "failed_quality_check_count": int(summary.get("failed_quality_check_count", 0)),
            "remediation_action_count": int(summary.get("remediation_action_count", 0)),
            "partition_count": int(summary.get("partition_count", 0)),
        })
    rows.sort(key=lambda row: (row["generated_at_utc"], row["run_id"]))
    ready_count = sum(row["release_ready"] for row in rows)
    latest_ready = rows[-1]["release_ready"] if rows else None
    current_streak = 0
    for row in reversed(rows):
        if row["release_ready"] != latest_ready:
            break
        current_streak += 1
    recent_rows = rows[-recent_window:]
    recent_ready_count = sum(row["release_ready"] for row in recent_rows)
    recent_readiness_rate = round(recent_ready_count / len(recent_rows), 4) if recent_rows else 0.0
    trend_status = "NO_DATA" if not recent_rows else (
        "HEALTHY" if recent_readiness_rate >= readiness_threshold else "AT_RISK"
    )
    latest = rows[-1] if rows else None
    previous = rows[-2] if len(rows) > 1 else None

    def latest_delta(field: str) -> int | None:
        if latest is None or previous is None:
            return None
        return int(latest[field]) - int(previous[field])

    failed_delta = latest_delta("failed_quality_check_count")
    remediation_delta = latest_delta("remediation_action_count")
    regression_reasons: list[str] = []
    improvement_reasons: list[str] = []
    if latest is not None and previous is not None:
        if previous["release_ready"] and not latest["release_ready"]:
            regression_reasons.append("RELEASE_READINESS_LOST")
        elif not previous["release_ready"] and latest["release_ready"]:
            improvement_reasons.append("RELEASE_READINESS_RECOVERED")
        if failed_delta is not None and failed_delta > 0:
            regression_reasons.append("FAILED_CHECKS_INCREASED")
        elif failed_delta is not None and failed_delta < 0:
            improvement_reasons.append("FAILED_CHECKS_DECREASED")
        if remediation_delta is not None and remediation_delta > 0:
            regression_reasons.append("REMEDIATION_ACTIONS_INCREASED")
        elif remediation_delta is not None and remediation_delta < 0:
            improvement_reasons.append("REMEDIATION_ACTIONS_DECREASED")
    latest_change_status = "NO_BASELINE"
    if regression_reasons and improvement_reasons:
        latest_change_status = "MIXED"
    elif regression_reasons:
        latest_change_status = "REGRESSED"
    elif improvement_reasons:
        latest_change_status = "IMPROVED"
    elif previous is not None:
        latest_change_status = "STABLE"

    return {
        "history_version": "1.2",
        "generated_at_utc": utc_timestamp(),
        "summary": {
            "run_count": len(rows),
            "ready_count": ready_count,
            "attention_count": len(rows) - ready_count,
            "readiness_rate": round(ready_count / len(rows), 4) if rows else 0.0,
            "latest_release_ready": latest_ready,
            "latest_status": rows[-1]["status"] if rows else "NO_RUNS",
            "current_status_streak": current_streak,
            "recent_run_count": len(recent_rows),
            "recent_window": recent_window,
            "recent_readiness_rate": recent_readiness_rate,
            "readiness_threshold": readiness_threshold,
            "trend_status": trend_status,
            "has_previous_run": previous is not None,
            "latest_change_status": latest_change_status,
            "latest_regression_reasons": regression_reasons,
            "latest_failed_quality_check_delta": failed_delta,
            "latest_remediation_action_delta": remediation_delta,
            "latest_partition_count_delta": latest_delta("partition_count"),
            "failed_quality_check_count": sum(row["failed_quality_check_count"] for row in rows),
            "remediation_action_count": sum(row["remediation_action_count"] for row in rows),
        },
        "runs": rows,
    }


def render_operations_history_html(history: dict[str, Any]) -> str:
    summary = history["summary"]
    def format_delta(value: int | None) -> str:
        if value is None:
            return "暂无上一批次"
        return f"{value:+d}"

    delta_cards = (
        f"<div class='delta-grid'><article><span>批次变化判定</span><strong>{escape(summary['latest_change_status'])}</strong>"
        f"<small>{escape('、'.join(summary['latest_regression_reasons']) or '无退化信号')}</small></article>"
        f"<article><span>失败检查环比</span><strong>{format_delta(summary['latest_failed_quality_check_delta'])}</strong></article>"
        f"<article><span>处置动作环比</span><strong>{format_delta(summary['latest_remediation_action_delta'])}</strong></article>"
        f"<article><span>分区数量环比</span><strong>{format_delta(summary['latest_partition_count_delta'])}</strong></article></div>"
    )
    rows = "".join(
        "<tr>"
        f"<td>{escape(run['run_id'])}</td><td>{escape(run['generated_at_utc'] or '—')}</td>"
        f"<td class={'ready' if run['release_ready'] else 'attention'}>{escape(run['status'])}</td>"
        f"<td>{escape(run['pipeline_status'])}</td><td>{escape(run['quality_status'])}</td>"
        f"<td>{run['failed_quality_check_count']}</td><td>{run['remediation_action_count']}</td>"
        "</tr>"
        for run in reversed(history["runs"])
    ) or "<tr><td colspan='7'>暂无运行快照</td></tr>"
    return f"""<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>离线湖仓运行历史</title><style>
:root{{font-family:Inter,'Microsoft YaHei',sans-serif;color:#17324d;background:#eef3f6}}body{{margin:0}}header{{padding:36px max(24px,7vw);background:#12344f;color:#fff;border-bottom:5px solid #27a39a}}main{{max-width:1180px;margin:auto;padding:26px}}.grid{{display:grid;grid-template-columns:repeat(5,1fr);gap:12px}}.delta-grid{{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-top:12px}}article,section{{background:#fff;border:1px solid #d9e4ea;border-radius:12px;padding:20px}}article span,article small{{color:#718696;font-size:12px}}article small{{display:block;margin-top:8px}}article strong{{display:block;margin-top:8px;font-size:26px}}section{{margin-top:16px;overflow:auto}}table{{width:100%;border-collapse:collapse}}th,td{{padding:11px;text-align:left;border-bottom:1px solid #e6edf1;white-space:nowrap}}.ready{{color:#087f72;font-weight:700}}.attention{{color:#b45309;font-weight:700}}@media(max-width:900px){{.grid,.delta-grid{{grid-template-columns:1fr 1fr}}}}
</style></head><body><header><small>OFFLINE LAKEHOUSE · RELEASE HISTORY</small><h1>多批次运行历史</h1><p>集中查看离线批次发布状态、质量失败与修复积压。</p></header><main><div class="grid"><article><span>运行批次</span><strong>{summary['run_count']}</strong></article><article><span>发布就绪</span><strong>{summary['ready_count']}</strong></article><article><span>总体就绪率</span><strong>{summary['readiness_rate']:.1%}</strong></article><article><span>近 {summary['recent_run_count']} 次 · {escape(summary['trend_status'])}</span><strong>{summary['recent_readiness_rate']:.1%}</strong></article><article><span>当前连续状态</span><strong>{escape(summary['latest_status'])} · {summary['current_status_streak']} 次</strong></article></div>{delta_cards}<section><h2>批次明细</h2><table><thead><tr><th>运行</th><th>生成时间</th><th>发布状态</th><th>流水线</th><th>质量</th><th>失败检查</th><th>处置动作</th></tr></thead><tbody>{rows}</tbody></table></section></main></body></html>"""


def write_operations_history(history: dict[str, Any], json_path: Path, html_path: Path) -> None:
    write_json_atomic(Path(json_path).resolve(), history)
    target = Path(html_path).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{uuid4().hex}.tmp")
    try:
        temporary.write_text(render_operations_history_html(history), encoding="utf-8")
        temporary.replace(target)
    finally:
        temporary.unlink(missing_ok=True)
