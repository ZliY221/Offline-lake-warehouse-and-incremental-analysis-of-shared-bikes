"""Build a focused, read-only workspace from the offline operator rehearsal."""

from __future__ import annotations

from html import escape
from typing import Any

from .operator_journey import OfflineOperatorJourney, rehearse_operator_journeys


STATUS_ORDER = {"BLOCKED": 0, "RECOVERY": 1, "REVIEW": 2, "READY": 3}
ACTION_MINUTES = {
    "RUN_ENVIRONMENT_PREFLIGHT": 8,
    "VALIDATE_SOURCE_PARTITIONS": 10,
    "RESUME_FAILED_STAGE": 15,
    "OPEN_REMEDIATION_QUEUE": 20,
    "COMPLETE_RELEASE_EVIDENCE": 12,
    "OPEN_OPERATIONS_SNAPSHOT": 5,
}


def build_operator_workspace(
    journeys: list[OfflineOperatorJourney], *, visible_limit: int = 12
) -> dict[str, Any]:
    """Turn 100 rehearsed journeys into a bounded, priority-ordered focus queue."""
    if not 1 <= visible_limit <= 25:
        raise ValueError("visible limit must be between 1 and 25")
    rehearsal = rehearse_operator_journeys(journeys)
    ordered = sorted(
        rehearsal["journeys"],
        key=lambda item: (
            STATUS_ORDER[item["status"]],
            -int(item["requires_confirmation"]),
            item["journey_id"],
        ),
    )
    focus_items = []
    for rank, item in enumerate(ordered[:visible_limit], start=1):
        focus_items.append({
            **item,
            "rank": rank,
            "estimated_minutes": ACTION_MINUTES[item["primary_action"]],
            "resume_key": f"{item['journey_id']}:{item['primary_action']}",
            "safe_to_start": item["status"] != "BLOCKED",
        })
    counts = {
        status.lower(): sum(item["status"] == status for item in ordered)
        for status in STATUS_ORDER
    }
    return {
        "status": "OPERATOR_WORKSPACE_READY",
        "journey_count": 100,
        "visible_count": len(focus_items),
        "remaining_count": 100 - len(focus_items),
        "filters": counts,
        "estimated_focus_minutes": sum(item["estimated_minutes"] for item in focus_items),
        "focus_items": focus_items,
        "guidance": "先处理阻断与恢复项，再完成质量复核和交付登记。",
        "execution_started": False,
    }


def render_operator_workspace_html(workspace: dict[str, Any]) -> str:
    """Render a standalone, accessible snapshot without external assets."""
    cards = []
    for item in workspace["focus_items"]:
        steps = "".join(f"<li>{escape(step)}</li>" for step in item["steps"])
        cards.append(
            f'<article class="task {escape(item["status"].lower())}">'
            f'<div><span class="rank">{item["rank"]:02d}</span>'
            f'<strong>{escape(item["journey_id"])}</strong></div>'
            f'<p class="meta">{escape(item["status"])} · {item["estimated_minutes"]} 分钟</p>'
            f'<h2>{escape(item["primary_action"])}</h2>'
            f'<p>{escape(item["guidance"])}</p><ol>{steps}</ol></article>'
        )
    filters = "".join(
        f'<span><b>{value}</b>{escape(name.upper())}</span>'
        for name, value in workspace["filters"].items()
    )
    return f"""<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>离线运行焦点队列</title>
<style>
:root{{--ink:#15223b;--muted:#64748b;--line:#dbe4ef;--paper:#f6f8fb;--navy:#243664;--amber:#d89b2b}}
*{{box-sizing:border-box}}body{{margin:0;background:var(--paper);color:var(--ink);font:16px/1.55 "Microsoft YaHei",sans-serif}}
header{{padding:38px 6vw 28px;background:linear-gradient(120deg,#17264b,#314b87);color:white;border-bottom:6px solid #e5b54d}}
header p{{max-width:760px;margin:8px 0 0;color:#dbe7ff}}main{{padding:28px 6vw 56px;max-width:1440px;margin:auto}}
.summary{{display:flex;gap:28px;flex-wrap:wrap;margin-bottom:28px}}.summary span{{min-width:110px;color:var(--muted)}}.summary b{{display:block;font-size:30px;color:var(--navy)}}
.queue{{display:grid;grid-template-columns:repeat(auto-fit,minmax(310px,1fr));gap:18px}}.task{{background:white;border:1px solid var(--line);border-left:7px solid #7b8ca8;border-radius:14px;padding:20px;box-shadow:0 8px 24px #17264b12}}
.task.blocked{{border-left-color:#c54b4b}}.task.recovery{{border-left-color:#d89b2b}}.task.review{{border-left-color:#3478b8}}.task.ready{{border-left-color:#26806e}}
.rank{{display:inline-grid;place-items:center;width:34px;height:34px;border-radius:50%;background:#eef3fa;margin-right:10px;font-weight:700}}h1{{margin:0;font-size:34px}}h2{{font-size:18px;margin:14px 0 8px}}.meta{{color:var(--muted);margin:8px 0}}ol{{padding-left:24px;margin-bottom:0}}footer{{color:var(--muted);margin-top:28px}}
</style></head><body><header><h1>离线运行焦点队列</h1><p>{escape(workspace['guidance'])}</p></header>
<main><section class="summary" aria-label="队列统计">{filters}<span><b>{workspace['estimated_focus_minutes']}</b>预计分钟</span></section>
<section class="queue">{''.join(cards)}</section><footer>只读计划：不会自动启动批次、修复或发布操作。</footer></main></body></html>"""
