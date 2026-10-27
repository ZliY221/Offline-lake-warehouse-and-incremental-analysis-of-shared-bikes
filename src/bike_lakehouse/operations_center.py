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
    status_tone = "ready" if snapshot["release_ready"] else "attention"
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
        f"<article><span>{escape(str(name))}</span><strong>{int(value):,}</strong>"
        "<small>已纳入本批次核对</small></article>"
        for name, value in sorted(counts.items())
    ) or "<article><span>数据集</span><strong>暂无统计</strong><small>等待批次产出</small></article>"
    task_flow = "".join(
        "<li>"
        f"<span>{escape(str(index).zfill(2))}</span>"
        f"<div><strong>{escape(task['name'])}</strong>"
        f"<small>{escape(task['status'])} · 尝试 {task['attempts']} 次</small></div>"
        "</li>"
        for index, task in enumerate(snapshot["tasks"], start=1)
    )
    embedded = json.dumps(snapshot, ensure_ascii=False, sort_keys=True).replace("</", "<\\/")
    return f"""<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>共享单车离线湖仓运行工作台</title><style>
:root{{--ink:#17263c;--muted:#68788d;--line:#dbe3ea;--paper:#fffdfa;--canvas:#f3f6f4;--navy:#173f5f;--blue:#2563eb;--teal:#0f9d8c;--amber:#e58a1f;--red:#c84d4d;font-family:"Segoe UI","Microsoft YaHei",sans-serif;color:var(--ink);background:var(--canvas);font-synthesis:none}}*{{box-sizing:border-box}}html{{scroll-behavior:smooth}}body{{margin:0;min-width:320px;min-height:100vh;background:#eef4f1;transition:background .25s ease}}body[data-screen="overview"]{{background:radial-gradient(circle at 85% 8%,rgba(15,157,140,.16),transparent 28%),linear-gradient(125deg,#edf5f0,#fbf7ec)}}body[data-screen="pipeline"]{{background:linear-gradient(rgba(37,99,235,.045) 1px,transparent 1px),linear-gradient(90deg,rgba(37,99,235,.045) 1px,transparent 1px),#edf3f8;background-size:28px 28px}}body[data-screen="datasets"]{{background:radial-gradient(circle at 18% 82%,rgba(229,138,31,.18),transparent 32%),#f8f4e9}}body[data-screen="quality"]{{background:linear-gradient(145deg,#f9eee9 0,#f3f6f4 55%,#eaf4f2 100%)}}a,button{{touch-action:manipulation}}.skip{{position:absolute;left:-999px;top:8px;z-index:20;padding:10px 14px;background:#fff;color:var(--navy)}}.skip:focus{{left:8px}}.shell{{min-height:100vh;display:grid;grid-template-columns:244px minmax(0,1fr)}}aside{{position:sticky;top:0;height:100vh;padding:30px 22px;color:#eff7fb;background:linear-gradient(180deg,#123c58,#1b506d);overflow:hidden}}aside:after{{content:"";position:absolute;inset:auto -70px -50px auto;width:190px;height:190px;border:24px solid rgba(255,255,255,.06);border-radius:50%}}.brand{{display:flex;align-items:center;gap:12px}}.brand-mark{{width:44px;height:44px;display:grid;place-items:center;border-radius:50% 50% 14px 14px;background:#f0b648;color:#17344f;font-weight:800;font-size:20px}}.brand strong,.brand small{{display:block}}.brand small{{margin-top:3px;color:#9fc1d2;font-size:11px}}nav{{display:grid;gap:8px;margin-top:58px}}nav a{{padding:12px 14px;border-radius:10px;color:#a9c5d4;text-decoration:none;border:1px solid transparent}}nav a.active,nav a:hover,nav a:focus-visible{{color:#fff;background:rgba(255,255,255,.1);border-color:rgba(255,255,255,.14)}}.run-note{{position:absolute;left:22px;right:22px;bottom:28px;padding-top:18px;border-top:1px solid rgba(255,255,255,.15)}}.run-note span,.run-note strong{{display:block}}.run-note span{{color:#91b3c5;font-size:11px}}.run-note strong{{margin-top:5px;font-size:13px}}main{{width:min(1500px,100%);padding:34px 42px 64px;overflow:hidden}}.page-view[hidden]{{display:none!important}}.page-view{{animation:page-in .24s ease both}}@keyframes page-in{{from{{opacity:0;transform:translateY(8px)}}to{{opacity:1;transform:none}}}}header{{display:flex;justify-content:space-between;align-items:flex-end;gap:24px;margin-bottom:22px}}.eyebrow{{margin:0 0 8px;color:var(--teal);font-size:10px;font-weight:800;letter-spacing:.18em}}h1{{margin:0;font-size:clamp(28px,3vw,42px);letter-spacing:-.04em}}header p:last-child{{margin:9px 0 0;color:var(--muted)}}.release{{min-width:210px;padding:14px 16px;border:1px solid #bdd4c9;border-radius:14px;background:#edf9f4}}.release span,.release strong{{display:block}}.release span{{font-size:11px;color:#527165}}.release strong{{margin-top:4px;color:#087767}}.release.attention{{border-color:#ecc9b0;background:#fff5ec}}.release.attention strong{{color:#a65021}}.metrics{{display:grid;grid-template-columns:1.2fr repeat(3,1fr);gap:12px;margin-bottom:14px}}.metric,.panel{{background:rgba(255,253,250,.94);border:1px solid var(--line);box-shadow:0 16px 44px rgba(23,63,95,.07)}}.metric{{position:relative;min-height:136px;padding:20px;border-radius:16px;overflow:hidden}}.metric:before{{content:"";position:absolute;left:0;right:0;top:0;height:4px;background:var(--blue)}}.metric:nth-child(2):before{{background:var(--teal)}}.metric:nth-child(3):before{{background:var(--amber)}}.metric:nth-child(4):before{{background:#7957c8}}.metric span,.metric small{{display:block;color:var(--muted)}}.metric span{{font-size:12px}}.metric strong{{display:block;margin:16px 0 9px;font-size:30px;font-variant-numeric:tabular-nums}}.metric small{{font-size:11px}}.workspace{{display:grid;grid-template-columns:minmax(0,1.7fr) minmax(300px,.8fr);gap:14px}}.panel{{margin-bottom:14px;padding:22px;border-radius:16px}}.panel-head{{display:flex;align-items:flex-start;justify-content:space-between;gap:16px;margin-bottom:18px}}.panel-head h2{{margin:2px 0 0;font-size:19px}}.panel-head span{{padding:6px 9px;border-radius:999px;background:#edf4f7;color:#49667a;font-size:10px}}.flow{{display:grid;gap:10px;margin:0;padding:0;list-style:none}}.flow li{{position:relative;display:flex;align-items:center;gap:12px;padding:12px;border:1px solid #e2e8ed;border-radius:12px;background:#fff}}.flow li:not(:last-child):after{{content:"";position:absolute;left:26px;top:100%;height:11px;border-left:2px dashed #8cbab2}}.flow li>span{{width:30px;height:30px;display:grid;place-items:center;border-radius:9px;background:#e8f4f1;color:#087767;font-weight:800;font-size:11px}}.flow strong,.flow small{{display:block}}.flow small{{margin-top:3px;color:var(--muted);font-size:10px}}.route-key{{display:flex;gap:7px;align-items:center;color:var(--muted);font-size:11px}}.route-key:before{{content:"";width:28px;height:4px;border-radius:3px;background:linear-gradient(90deg,var(--blue),var(--teal),var(--amber))}}.table-wrap{{overflow-x:auto}}table{{width:100%;border-collapse:collapse;font-size:12px}}th,td{{padding:12px 10px;text-align:left;border-bottom:1px solid #e8edf0}}th{{color:#64758a;font-weight:600;background:#f5f8f8}}.state{{font-weight:700;color:#087767}}.datasets{{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:16px}}.datasets article{{min-height:150px;padding:22px;border-radius:18px;background:#f1f6f7;border:1px solid #dfe9ea}}.datasets article:nth-child(3n+2){{background:#fff5e7;border-color:#f1debf}}.datasets article:nth-child(3n){{background:#eef2fb;border-color:#d9e1f3}}.datasets span,.datasets small{{display:block;color:var(--muted);font-size:10px}}.datasets strong{{display:block;margin:18px 0 8px;font-size:28px}}.actions{{margin:0;padding:0;list-style:none}}.actions li{{padding:18px;border-bottom:1px solid #e7ecee;background:rgba(255,255,255,.72)}}.actions li b{{display:inline-flex;margin-right:9px;padding:3px 7px;border-radius:6px;background:#fff0dc;color:#9a5515}}.actions li p{{margin:7px 0 0;color:var(--muted);line-height:1.55}}.actions .empty{{color:#087767}}:focus-visible{{outline:3px solid #f0b648;outline-offset:3px}}@media(max-width:980px){{.shell{{grid-template-columns:82px minmax(0,1fr)}}aside{{padding:24px 14px}}.brand div,nav a span,.run-note{{display:none}}nav a{{padding:12px 8px;text-align:center}}main{{padding:28px 24px}}.workspace{{grid-template-columns:1fr}}}}@media(max-width:680px){{.shell{{display:block}}aside{{position:static;height:auto;padding:16px 18px;display:flex;align-items:center;justify-content:space-between}}aside:after,.run-note{{display:none}}.brand div{{display:block}}nav{{display:flex;margin:0}}nav a{{font-size:11px}}main{{padding:22px 14px 44px}}header{{align-items:flex-start;flex-direction:column}}.release{{width:100%}}.metrics{{grid-template-columns:1fr 1fr}}.metric{{min-height:116px}}}}@media(max-width:420px){{.metrics{{grid-template-columns:1fr}}nav{{display:none}}}}@media(prefers-reduced-motion:reduce){{html{{scroll-behavior:auto}}.page-view{{animation:none}}}}
</style></head><body data-screen="overview"><a class="skip" href="#content">跳到主要内容</a><div class="shell"><aside><div class="brand"><span class="brand-mark">B</span><div><strong>Bike Lakehouse</strong><small>离线批次运营</small></div></div><nav aria-label="报告导航"><a class="active" data-screen-link="overview" href="#overview"><span>01 运行总览</span></a><a data-screen-link="pipeline" href="#pipeline"><span>02 任务链路</span></a><a data-screen-link="datasets" href="#datasets"><span>03 数据资产</span></a><a data-screen-link="quality" href="#quality"><span>04 质量处置</span></a></nav><div class="run-note"><span>当前批次</span><strong>{escape(str(run.get('run_id') or '未命名'))}</strong></div></aside>
<main id="content"><header class="page-view" data-page="overview" id="overview"><div><p class="eyebrow">OFFLINE LAKEHOUSE · BATCH DESK</p><h1>共享单车离线湖仓工作台</h1><p>一次看清批次进度、分层产出与质量处置，报告可在无网络环境直接打开。</p></div><div class="release {status_tone}"><span>当前发布结论</span><strong>{escape(status)}</strong></div></header>
<section class="metrics page-view" data-page="overview" aria-label="批次概览"><article class="metric"><span>流水线状态</span><strong>{escape(str(run['pipeline_status']))}</strong><small>质量门禁：{escape(str(run['quality_status']))}</small></article><article class="metric"><span>成功任务</span><strong>{summary['succeeded_task_count']} / {summary['task_count']}</strong><small>按依赖顺序执行</small></article><article class="metric"><span>质量检查</span><strong>{summary['quality_check_count']}</strong><small>失败 {summary['failed_quality_check_count']} 项</small></article><article class="metric"><span>待处理动作</span><strong>{summary['remediation_action_count']}</strong><small>发布前处置队列</small></article></section>
<div class="workspace page-view" data-page="pipeline" hidden><section class="panel" id="pipeline"><div class="panel-head"><div><p class="eyebrow">BATCH ROUTE</p><h2>离线任务链路</h2></div><span>按依赖推进</span></div><div class="table-wrap"><table><thead><tr><th>任务</th><th>状态</th><th>尝试次数</th><th>依赖</th></tr></thead><tbody>{task_rows}</tbody></table></div></section><section class="panel"><div class="panel-head"><div><p class="eyebrow">PIPELINE MAP</p><h2>分层处理路线</h2></div><div class="route-key">批次轨迹</div></div><ol class="flow">{task_flow}</ol></section></div>
<section class="panel page-view" data-page="datasets" id="datasets" hidden><div class="panel-head"><div><p class="eyebrow">LAYER OUTPUT</p><h2>分层数据资产</h2></div><span>{summary['partition_count']} 个分区</span></div><div class="datasets">{dataset_cards}</div></section>
<section class="panel page-view" data-page="quality" id="quality" hidden><div class="panel-head"><div><p class="eyebrow">QUALITY QUEUE</p><h2>质量处置队列</h2></div><span>{summary['remediation_action_count']} 项待办</span></div><ol class="actions">{action_rows}</ol></section></main></div><script type="application/json" id="operations-snapshot">{embedded}</script><script>function openScreen(screen){{document.body.dataset.screen=screen;document.querySelectorAll('[data-page]').forEach(function(view){{view.hidden=view.dataset.page!==screen}});document.querySelectorAll('[data-screen-link]').forEach(function(link){{link.classList.toggle('active',link.dataset.screenLink===screen);link.setAttribute('aria-current',link.dataset.screenLink===screen?'page':'false')}})}}document.querySelectorAll('[data-screen-link]').forEach(function(link){{link.addEventListener('click',function(event){{event.preventDefault();openScreen(link.dataset.screenLink);history.replaceState(null,'','#'+link.dataset.screenLink)}})}});var initial=location.hash.slice(1);openScreen(['overview','pipeline','datasets','quality'].includes(initial)?initial:'overview');</script></body></html>"""


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
