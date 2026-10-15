"""Aggregate offline operations snapshots into a release history report."""

from __future__ import annotations

from html import escape
from pathlib import Path
from typing import Any, Iterable

from .batch_manifest import utc_timestamp, write_json_atomic


def build_operations_history(snapshots: Iterable[dict[str, Any]]) -> dict[str, Any]:
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
    return {
        "history_version": "1.0",
        "generated_at_utc": utc_timestamp(),
        "summary": {
            "run_count": len(rows),
            "ready_count": ready_count,
            "attention_count": len(rows) - ready_count,
            "readiness_rate": round(ready_count / len(rows), 4) if rows else 0.0,
            "failed_quality_check_count": sum(row["failed_quality_check_count"] for row in rows),
            "remediation_action_count": sum(row["remediation_action_count"] for row in rows),
        },
        "runs": rows,
    }


def render_operations_history_html(history: dict[str, Any]) -> str:
    summary = history["summary"]
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
:root{{font-family:Inter,'Microsoft YaHei',sans-serif;color:#17324d;background:#eef3f6}}body{{margin:0}}header{{padding:36px max(24px,7vw);background:#12344f;color:#fff;border-bottom:5px solid #27a39a}}main{{max-width:1180px;margin:auto;padding:26px}}.grid{{display:grid;grid-template-columns:repeat(4,1fr);gap:12px}}article,section{{background:#fff;border:1px solid #d9e4ea;border-radius:12px;padding:20px}}article span{{color:#718696;font-size:12px}}article strong{{display:block;margin-top:8px;font-size:26px}}section{{margin-top:16px;overflow:auto}}table{{width:100%;border-collapse:collapse}}th,td{{padding:11px;text-align:left;border-bottom:1px solid #e6edf1;white-space:nowrap}}.ready{{color:#087f72;font-weight:700}}.attention{{color:#b45309;font-weight:700}}@media(max-width:760px){{.grid{{grid-template-columns:1fr 1fr}}}}
</style></head><body><header><small>OFFLINE LAKEHOUSE · RELEASE HISTORY</small><h1>多批次运行历史</h1><p>集中查看离线批次发布状态、质量失败与修复积压。</p></header><main><div class="grid"><article><span>运行批次</span><strong>{summary['run_count']}</strong></article><article><span>发布就绪</span><strong>{summary['ready_count']}</strong></article><article><span>就绪率</span><strong>{summary['readiness_rate']:.1%}</strong></article><article><span>待处置动作</span><strong>{summary['remediation_action_count']}</strong></article></div><section><h2>批次明细</h2><table><thead><tr><th>运行</th><th>生成时间</th><th>发布状态</th><th>流水线</th><th>质量</th><th>失败检查</th><th>处置动作</th></tr></thead><tbody>{rows}</tbody></table></section></main></body></html>"""


def write_operations_history(history: dict[str, Any], json_path: Path, html_path: Path) -> None:
    write_json_atomic(Path(json_path).resolve(), history)
    target = Path(html_path).resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(render_operations_history_html(history), encoding="utf-8")
