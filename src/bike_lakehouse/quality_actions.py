"""Turn a lakehouse quality report into an ordered remediation plan."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .batch_manifest import utc_timestamp, write_json_atomic


ACTION_CATALOG: dict[str, tuple[str, str]] = {
    "bronze_partition_freshness": (
        "P0",
        "补齐最新摄取分区并重新执行受控回填。",
    ),
    "bronze_partition_continuity": (
        "P0",
        "按缺失日期生成修复清单，完成回填后重新校验连续性。",
    ),
    "silver_row_reconciliation": (
        "P0",
        "核对 Bronze 到 Silver 的合法、拒绝和重复分流数量。",
    ),
    "gold_trip_reconciliation": (
        "P0",
        "重算受影响 Gold 分区并复核行程聚合口径。",
    ),
    "silver_rejected_ratio": (
        "P1",
        "查看拒绝原因分布，修正数据契约或隔离异常来源。",
    ),
    "silver_duplicate_ratio": (
        "P1",
        "检查事件主键冲突与上游重复投递，重跑去重分区。",
    ),
    "station_one_current_version": (
        "P1",
        "重建站点 SCD2 并确认每个站点只有一个当前版本。",
    ),
    "station_contiguous_intervals": (
        "P1",
        "检查站点快照顺序并修复 SCD2 生效区间。",
    ),
    "popular_route_rank_contract": (
        "P2",
        "重算热门路线并检查窗口排序与排名上限。",
    ),
    "cohort_retention_contract": (
        "P2",
        "复核 cohort 日期关联及留存率计算边界。",
    ),
}


def build_remediation_plan(report: dict[str, Any]) -> dict[str, Any]:
    """Build a stable, user-facing action queue from failed quality checks."""
    checks = report.get("checks")
    if not isinstance(checks, list):
        raise ValueError("quality report checks must be a list")

    actions: list[dict[str, Any]] = []
    for check in checks:
        if not isinstance(check, dict) or not isinstance(check.get("name"), str):
            raise ValueError("every quality check must contain a valid name")
        if check.get("status") != "FAIL":
            continue
        name = check["name"]
        priority, recommendation = ACTION_CATALOG.get(
            name,
            ("P2", "检查该质量项的证据字段，修复后重新生成质量报告。"),
        )
        evidence = {
            key: value
            for key, value in check.items()
            if key not in {"name", "status"}
        }
        actions.append(
            {
                "check_name": name,
                "priority": priority,
                "recommendation": recommendation,
                "evidence": evidence,
            }
        )

    failed_manifests = int(
        report.get("manifest_summary", {}).get("status_counts", {}).get("FAILED", 0)
    )
    if failed_manifests:
        actions.append(
            {
                "check_name": "failed_batch_manifests",
                "priority": "P1",
                "recommendation": "检查失败批次日志，从最后成功阶段恢复执行。",
                "evidence": {"failed_manifest_count": failed_manifests},
            }
        )

    priority_order = {"P0": 0, "P1": 1, "P2": 2}
    actions.sort(key=lambda item: (priority_order[item["priority"]], item["check_name"]))
    return {
        "generated_at_utc": utc_timestamp(),
        "source_quality_status": report.get("overall_status", "UNKNOWN"),
        "action_count": len(actions),
        "ready_for_release": not actions,
        "actions": actions,
    }


def build_remediation_plan_file(report_path: Path, output_path: Path) -> dict[str, Any]:
    report = json.loads(Path(report_path).read_text(encoding="utf-8"))
    plan = build_remediation_plan(report)
    write_json_atomic(Path(output_path).resolve(), plan)
    return plan
