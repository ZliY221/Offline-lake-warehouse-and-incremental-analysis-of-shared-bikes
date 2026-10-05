"""Reproducible Spark physical-plan comparison without performance claims."""

from __future__ import annotations

from contextlib import redirect_stdout
from datetime import UTC, datetime
from io import StringIO
import json
from pathlib import Path
import platform
import re
from typing import Any

from .batch_manifest import write_json_atomic


PLAN_TOKENS = (
    "SortMergeJoin",
    "BroadcastHashJoin",
    "BroadcastExchange",
    "Exchange",
    "AdaptiveSparkPlan",
    "AQEShuffleRead",
    "coalesced",
)


def capture_formatted_plan(dataframe: Any) -> str:
    output = StringIO()
    with redirect_stdout(output):
        dataframe.explain(mode="formatted")
    return output.getvalue().strip()


def normalize_plan(plan: str) -> str:
    normalized = re.sub(r"#\d+[L]?", "#<id>", plan)
    normalized = re.sub(r"plan_id=\d+", "plan_id=<id>", normalized)
    return normalized


def plan_features(plan: str) -> dict[str, int]:
    return {token: plan.count(token) for token in PLAN_TOKENS}


def _result_rows(dataframe: Any) -> list[dict[str, object]]:
    return [row.asDict(recursive=True) for row in dataframe.orderBy("district").collect()]


def build_plan_analysis(spark: Any, *, fact_rows: int = 1_000) -> dict[str, object]:
    if fact_rows <= 0:
        raise ValueError("fact_rows must be greater than zero")
    from pyspark.sql import functions as F

    dimension_rows = [
        (f"ST-{index:03d}", f"district-{index % 3}")
        for index in range(1, 9)
    ]
    dimension = spark.createDataFrame(dimension_rows, "station_id string, district string")
    facts = spark.range(fact_rows).select(
        F.format_string("ST-%03d", (F.col("id") % 8) + 1).alias("station_id"),
        ((F.col("id") % 17) + 1).cast("long").alias("trip_minutes"),
    )

    config_keys = (
        "spark.sql.adaptive.enabled",
        "spark.sql.adaptive.coalescePartitions.enabled",
        "spark.sql.autoBroadcastJoinThreshold",
        "spark.sql.shuffle.partitions",
    )
    original = {key: spark.conf.get(key) for key in config_keys}
    try:
        spark.conf.set("spark.sql.adaptive.enabled", "false")
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
        spark.conf.set("spark.sql.shuffle.partitions", "8")
        baseline = (
            facts.join(dimension, "station_id", "inner")
            .groupBy("district")
            .agg(
                F.count(F.lit(1)).alias("trip_count"),
                F.sum("trip_minutes").alias("total_trip_minutes"),
            )
        )
        baseline_rows = _result_rows(baseline)
        baseline_plan = normalize_plan(capture_formatted_plan(baseline))

        broadcasted = (
            facts.join(F.broadcast(dimension), "station_id", "inner")
            .groupBy("district")
            .agg(
                F.count(F.lit(1)).alias("trip_count"),
                F.sum("trip_minutes").alias("total_trip_minutes"),
            )
        )
        broadcast_rows = _result_rows(broadcasted)
        broadcast_plan = normalize_plan(capture_formatted_plan(broadcasted))
        if broadcast_rows != baseline_rows:
            raise ValueError("Join strategies produced different aggregate results")

        spark.conf.set("spark.sql.adaptive.enabled", "true")
        spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
        spark.conf.set("spark.sql.shuffle.partitions", "16")
        adaptive = (
            facts.repartition(16, "station_id")
            .join(dimension, "station_id", "inner")
            .groupBy("district")
            .agg(F.count(F.lit(1)).alias("trip_count"))
        )
        adaptive_rows = _result_rows(adaptive)
        adaptive_plan = normalize_plan(capture_formatted_plan(adaptive))
    finally:
        for key, value in original.items():
            spark.conf.set(key, value)

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
        },
        "input": {
            "fact_rows": fact_rows,
            "dimension_rows": len(dimension_rows),
            "station_keys": 8,
        },
        "claims_boundary": (
            "This report proves plan selection and result equivalence on local synthetic data; "
            "it is not a throughput benchmark or production tuning result."
        ),
        "baseline_sort_merge": {
            "settings": {
                "adaptive_enabled": False,
                "auto_broadcast_join_threshold": -1,
                "shuffle_partitions": 8,
            },
            "result": baseline_rows,
            "features": plan_features(baseline_plan),
            "formatted_plan": baseline_plan,
        },
        "explicit_broadcast": {
            "settings": {
                "adaptive_enabled": False,
                "broadcast_hint": True,
                "shuffle_partitions": 8,
            },
            "result": broadcast_rows,
            "features": plan_features(broadcast_plan),
            "formatted_plan": broadcast_plan,
        },
        "adaptive_aggregation": {
            "settings": {
                "adaptive_enabled": True,
                "coalesce_partitions_enabled": True,
                "auto_broadcast_join_threshold": -1,
                "initial_shuffle_partitions": 16,
            },
            "result": adaptive_rows,
            "features": plan_features(adaptive_plan),
            "formatted_plan": adaptive_plan,
        },
    }


def render_markdown(report: dict[str, object]) -> str:
    sections = [
        ("Baseline Sort-Merge Join", report["baseline_sort_merge"]),
        ("Explicit Broadcast Hash Join", report["explicit_broadcast"]),
        ("Adaptive aggregation", report["adaptive_aggregation"]),
    ]
    lines = [
        "# Spark physical-plan evidence",
        "",
        f"Generated: `{report['generated_at_utc']}`",
        "",
        str(report["claims_boundary"]),
        "",
    ]
    for title, raw_section in sections:
        section = raw_section
        lines.extend(
            [
                f"## {title}",
                "",
                "Features: `" + json.dumps(section["features"], sort_keys=True) + "`",
                "",
                "```text",
                str(section["formatted_plan"]),
                "```",
                "",
            ]
        )
    return "\n".join(lines)


def write_plan_evidence(
    report: dict[str, object],
    *,
    json_path: Path,
    markdown_path: Path,
) -> None:
    write_json_atomic(Path(json_path).resolve(), report)
    markdown_path = Path(markdown_path).resolve()
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.write_text(render_markdown(report), encoding="utf-8")
