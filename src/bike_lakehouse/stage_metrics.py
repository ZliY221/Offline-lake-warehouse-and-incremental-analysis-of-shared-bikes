"""Capture reproducible Spark UI REST metrics for a controlled local workload."""

from __future__ import annotations

from datetime import UTC, datetime
import json
from pathlib import Path
import platform
import statistics
from typing import Any
from urllib.parse import quote
from urllib.request import urlopen

from .batch_manifest import write_json_atomic


def fetch_json(url: str, *, timeout_seconds: float = 10.0) -> Any:
    with urlopen(url, timeout=timeout_seconds) as response:  # noqa: S310 - local Spark UI
        return json.load(response)


def _metric(metrics: dict[str, Any], *path: str) -> int:
    current: Any = metrics
    for part in path:
        if not isinstance(current, dict):
            return 0
        current = current.get(part, 0)
    return int(current or 0)


def _operation_name(raw_name: object) -> str:
    return str(raw_name or "").split(" at ", maxsplit=1)[0]


def summarize_tasks(tasks: list[dict[str, Any]]) -> dict[str, object]:
    normalized = []
    for task in tasks:
        metrics = task.get("taskMetrics") or {}
        normalized.append(
            {
                "task_id": int(task.get("taskId", 0)),
                "index": int(task.get("index", 0)),
                "attempt": int(task.get("attempt", 0)),
                "status": str(task.get("status", "UNKNOWN")),
                "executor_run_time_ms": _metric(metrics, "executorRunTime"),
                "executor_cpu_time_ns": _metric(metrics, "executorCpuTime"),
                "jvm_gc_time_ms": _metric(metrics, "jvmGcTime"),
                "memory_bytes_spilled": _metric(metrics, "memoryBytesSpilled"),
                "disk_bytes_spilled": _metric(metrics, "diskBytesSpilled"),
                "shuffle_read_bytes": (
                    _metric(metrics, "shuffleReadMetrics", "remoteBytesRead")
                    + _metric(metrics, "shuffleReadMetrics", "localBytesRead")
                ),
                "shuffle_write_bytes": _metric(
                    metrics, "shuffleWriteMetrics", "bytesWritten"
                ),
            }
        )
    run_times = [row["executor_run_time_ms"] for row in normalized]
    median_run_time = statistics.median(run_times) if run_times else 0
    return {
        "task_count": len(normalized),
        "executor_run_time_ms": {
            "min": min(run_times, default=0),
            "median": median_run_time,
            "max": max(run_times, default=0),
            "max_to_median_ratio": (
                round(max(run_times) / median_run_time, 3)
                if run_times and median_run_time
                else 0
            ),
        },
        "tasks": sorted(normalized, key=lambda row: (row["index"], row["attempt"])),
    }


def _stage_record(stage: dict[str, Any]) -> dict[str, object]:
    return {
        "stage_id": int(stage["stageId"]),
        "attempt_id": int(stage.get("attemptId", 0)),
        "name": _operation_name(stage.get("name")),
        "status": str(stage.get("status", "UNKNOWN")),
        "num_tasks": int(stage.get("numTasks", 0)),
        "num_complete_tasks": int(stage.get("numCompleteTasks", 0)),
        "executor_run_time_ms": int(stage.get("executorRunTime", 0)),
        "executor_cpu_time_ns": int(stage.get("executorCpuTime", 0)),
        "jvm_gc_time_ms": int(stage.get("jvmGcTime", 0)),
        "input_bytes": int(stage.get("inputBytes", 0)),
        "output_bytes": int(stage.get("outputBytes", 0)),
        "shuffle_read_bytes": int(stage.get("shuffleReadBytes", 0)),
        "shuffle_read_records": int(stage.get("shuffleReadRecords", 0)),
        "shuffle_write_bytes": int(stage.get("shuffleWriteBytes", 0)),
        "shuffle_write_records": int(stage.get("shuffleWriteRecords", 0)),
        "memory_bytes_spilled": int(stage.get("memoryBytesSpilled", 0)),
        "disk_bytes_spilled": int(stage.get("diskBytesSpilled", 0)),
    }


def collect_stage_metrics(
    spark: Any,
    *,
    row_count: int = 50_000,
    shuffle_partitions: int = 8,
) -> dict[str, object]:
    if row_count <= 0:
        raise ValueError("row_count must be greater than zero")
    if shuffle_partitions <= 0:
        raise ValueError("shuffle_partitions must be greater than zero")
    from pyspark.sql import functions as F

    web_url = spark.sparkContext.uiWebUrl
    if not web_url:
        raise RuntimeError("Spark UI must be enabled before collecting REST metrics")
    job_group = "stage-metrics-evidence"
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.shuffle.partitions", str(shuffle_partitions))
    spark.sparkContext.setJobGroup(
        job_group,
        "Controlled repartition and aggregation for Spark UI evidence",
    )
    try:
        result = (
            spark.range(row_count)
            .select(
                (F.col("id") % 32).alias("station_key"),
                F.sha2(F.col("id").cast("string"), 256).alias("payload"),
            )
            .repartition(shuffle_partitions, "station_key")
            .groupBy("station_key")
            .agg(
                F.count(F.lit(1)).alias("row_count"),
                F.sum(F.length("payload")).alias("payload_characters"),
            )
            .collect()
        )
    finally:
        spark.sparkContext.setLocalProperty("spark.jobGroup.id", None)
        spark.sparkContext.setLocalProperty("spark.job.description", None)

    application_id = spark.sparkContext.applicationId
    base_url = f"{web_url}/api/v1/applications/{quote(application_id, safe='')}"
    jobs = fetch_json(f"{base_url}/jobs")
    matching_jobs = [job for job in jobs if job.get("jobGroup") == job_group]
    if not matching_jobs:
        raise RuntimeError(f"Spark UI did not retain job group {job_group}")
    stage_ids = sorted(
        {int(stage_id) for job in matching_jobs for stage_id in job.get("stageIds", [])}
    )
    all_stages = fetch_json(f"{base_url}/stages")
    raw_stages = [
        stage
        for stage in all_stages
        if int(stage.get("stageId", -1)) in stage_ids
        and str(stage.get("status")) == "COMPLETE"
    ]
    stages = [_stage_record(stage) for stage in raw_stages]
    if not stages:
        raise RuntimeError("Spark UI returned no completed stages for the workload")
    busiest_stage = max(
        stages,
        key=lambda stage: stage["shuffle_read_bytes"] + stage["shuffle_write_bytes"],
    )
    task_url = (
        f"{base_url}/stages/{busiest_stage['stage_id']}/"
        f"{busiest_stage['attempt_id']}/taskList?length=10000"
    )
    task_summary = summarize_tasks(fetch_json(task_url))
    totals = {
        key: sum(int(stage[key]) for stage in stages)
        for key in (
            "num_tasks",
            "executor_run_time_ms",
            "executor_cpu_time_ns",
            "jvm_gc_time_ms",
            "input_bytes",
            "output_bytes",
            "shuffle_read_bytes",
            "shuffle_read_records",
            "shuffle_write_bytes",
            "shuffle_write_records",
            "memory_bytes_spilled",
            "disk_bytes_spilled",
        )
    }
    result_rows = sorted(
        (row.asDict(recursive=True) for row in result),
        key=lambda row: row["station_key"],
    )
    return {
        "report_version": "1.0",
        "generated_at_utc": datetime.now(UTC).isoformat(timespec="seconds").replace(
            "+00:00", "Z"
        ),
        "environment": {
            "platform": platform.platform(),
            "python_version": platform.python_version(),
            "spark_version": spark.version,
            "spark_master": spark.sparkContext.master,
            "application_id": application_id,
        },
        "input": {
            "row_count": row_count,
            "key_count": 32,
            "shuffle_partitions": shuffle_partitions,
        },
        "claims_boundary": (
            "These are local Spark UI REST metrics for one synthetic workload. "
            "They characterize stages and tasks; they are not production capacity or SLA evidence."
        ),
        "collection_source": "live Spark UI REST API /api/v1 before Spark shutdown",
        "result": {
            "group_count": len(result_rows),
            "total_rows": sum(int(row["row_count"]) for row in result_rows),
            "total_payload_characters": sum(
                int(row["payload_characters"]) for row in result_rows
            ),
        },
        "jobs": [
            {
                "job_id": int(job["jobId"]),
                "name": _operation_name(job.get("name")),
                "status": str(job.get("status", "UNKNOWN")),
                "stage_ids": [int(stage_id) for stage_id in job.get("stageIds", [])],
                "num_tasks": int(job.get("numTasks", 0)),
                "num_completed_tasks": int(job.get("numCompletedTasks", 0)),
            }
            for job in matching_jobs
        ],
        "stages": sorted(stages, key=lambda stage: stage["stage_id"]),
        "stage_totals": totals,
        "busiest_shuffle_stage": {
            "stage_id": busiest_stage["stage_id"],
            "attempt_id": busiest_stage["attempt_id"],
            "task_summary": task_summary,
        },
    }


def render_markdown(report: dict[str, object]) -> str:
    totals = report["stage_totals"]
    lines = [
        "# Spark UI stage and task metrics",
        "",
        f"Generated: `{report['generated_at_utc']}`",
        "",
        str(report["claims_boundary"]),
        "",
        "## Workload result",
        "",
        "```json",
        json.dumps(report["result"], indent=2, sort_keys=True),
        "```",
        "",
        "## Stage totals",
        "",
        "```json",
        json.dumps(totals, indent=2, sort_keys=True),
        "```",
        "",
        "## Completed stages",
        "",
        "| Stage | Tasks | Run time (ms) | Shuffle read | Shuffle write | Spill |",
        "| ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for stage in report["stages"]:
        spill = stage["memory_bytes_spilled"] + stage["disk_bytes_spilled"]
        lines.append(
            f"| {stage['stage_id']} | {stage['num_tasks']} | "
            f"{stage['executor_run_time_ms']} | {stage['shuffle_read_bytes']} | "
            f"{stage['shuffle_write_bytes']} | {spill} |"
        )
    lines.extend(
        [
            "",
            "## Busiest shuffle stage task distribution",
            "",
            "```json",
            json.dumps(
                report["busiest_shuffle_stage"], indent=2, sort_keys=True
            ),
            "```",
            "",
        ]
    )
    return "\n".join(lines)


def write_stage_metrics(
    report: dict[str, object], *, json_path: Path, markdown_path: Path
) -> None:
    write_json_atomic(Path(json_path).resolve(), report)
    markdown_path = Path(markdown_path).resolve()
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.write_text(render_markdown(report), encoding="utf-8")
