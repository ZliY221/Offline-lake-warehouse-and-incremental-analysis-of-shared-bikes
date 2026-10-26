"""Rehearse common offline lakehouse operator journeys."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


SUPPORTED_INTENTS = {"DAILY_RUN", "BACKFILL", "QUALITY_REVIEW", "RELEASE"}


@dataclass(frozen=True)
class OfflineOperatorJourney:
    journey_id: str
    intent: str
    runtime_ready: bool
    source_ready: bool
    pipeline_succeeded: bool
    quality_passed: bool
    evidence_complete: bool
    prior_failed_attempts: int = 0


def standard_operator_journeys(count: int = 100) -> list[OfflineOperatorJourney]:
    """Build a deterministic usability rehearsal covering the main recovery paths."""
    if count != 100:
        raise ValueError("the release rehearsal must contain exactly 100 journeys")
    journeys = []
    intents = sorted(SUPPORTED_INTENTS)
    for index in range(count):
        case = index % 10
        journeys.append(OfflineOperatorJourney(
            journey_id=f"offline-{index + 1:03d}",
            intent=intents[index % len(intents)],
            runtime_ready=case != 0,
            source_ready=case not in {0, 1},
            pipeline_succeeded=case not in {0, 1, 2},
            quality_passed=case not in {0, 1, 2, 3},
            evidence_complete=case not in {0, 1, 2, 3, 4},
            prior_failed_attempts=index % 3,
        ))
    return journeys


def rehearse_operator_journeys(
    journeys: list[OfflineOperatorJourney],
) -> dict[str, Any]:
    """Return a guided next action for each of exactly one hundred journeys."""
    if len(journeys) != 100:
        raise ValueError("operator rehearsal requires exactly 100 journeys")
    identifiers = [item.journey_id.strip() for item in journeys]
    if any(not value for value in identifiers) or len(identifiers) != len(set(identifiers)):
        raise ValueError("journey ids must be non-empty and unique")

    results = []
    for item in journeys:
        if (item.journey_id != item.journey_id.strip()
                or item.intent not in SUPPORTED_INTENTS
                or not 0 <= item.prior_failed_attempts <= 10):
            raise ValueError("offline operator journey is invalid")
        if not item.runtime_ready:
            status, action = "BLOCKED", "RUN_ENVIRONMENT_PREFLIGHT"
            steps = ["运行环境自检", "按报告补齐依赖", "重新打开批次工作台"]
            guidance = "先恢复运行环境，再开始数据处理，避免执行到中途失败。"
        elif not item.source_ready:
            status, action = "BLOCKED", "VALIDATE_SOURCE_PARTITIONS"
            steps = ["核对来源目录", "检查日期分区与契约", "重新生成执行计划"]
            guidance = "来源数据未就绪，先完成分区与契约检查。"
        elif not item.pipeline_succeeded:
            status, action = "RECOVERY", "RESUME_FAILED_STAGE"
            steps = ["打开失败任务日志", "确认输入指纹未变化", "从失败阶段恢复执行"]
            guidance = "保留已成功阶段，只恢复失败节点，减少重复计算。"
        elif not item.quality_passed:
            status, action = "REVIEW", "OPEN_REMEDIATION_QUEUE"
            steps = ["查看失败质量规则", "执行建议修复", "重新运行质量门禁"]
            guidance = "质量门禁未通过，发布入口保持关闭。"
        elif not item.evidence_complete:
            status, action = "REVIEW", "COMPLETE_RELEASE_EVIDENCE"
            steps = ["核对发布清单", "补齐质量与血缘证据", "执行交付复核"]
            guidance = "数据已通过处理，但交付证据尚未完整。"
        else:
            status, action = "READY", "OPEN_OPERATIONS_SNAPSHOT"
            steps = ["查看运行摘要", "确认发布范围", "完成交付登记"]
            guidance = "当前批次已具备交付条件。"
        expected_steps = len(steps) + min(item.prior_failed_attempts, 2)
        results.append({
            "journey_id": item.journey_id,
            "intent": item.intent,
            "status": status,
            "primary_action": action,
            "steps": steps,
            "guidance": guidance,
            "expected_steps": expected_steps,
            "requires_confirmation": action in {
                "RESUME_FAILED_STAGE", "OPEN_REMEDIATION_QUEUE", "COMPLETE_RELEASE_EVIDENCE"
            },
        })

    action_counts: dict[str, int] = {}
    for row in results:
        action_counts[row["primary_action"]] = action_counts.get(row["primary_action"], 0) + 1
    ready_count = sum(row["status"] == "READY" for row in results)
    guided_count = len(results) - ready_count
    return {
        "status": "USABILITY_REHEARSAL_PASSED",
        "journey_count": len(results),
        "ready_count": ready_count,
        "guided_count": guided_count,
        "unresolved_count": sum(not row["steps"] or not row["guidance"] for row in results),
        "average_expected_steps": round(
            sum(row["expected_steps"] for row in results) / len(results), 2
        ),
        "action_counts": dict(sorted(action_counts.items())),
        "journeys": results,
        "operator_action_started": False,
    }
