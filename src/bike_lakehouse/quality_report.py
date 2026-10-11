"""Cross-layer data quality report for the local lakehouse."""

from __future__ import annotations

from collections import Counter
import json
from pathlib import Path
from typing import Any

from .batch_manifest import utc_timestamp, write_json_atomic


def _require_directory(path: Path, label: str) -> Path:
    resolved = Path(path).resolve()
    if not resolved.is_dir():
        raise ValueError(f"{label} dataset does not exist: {resolved}")
    return resolved


def _check(name: str, passed: bool, **evidence: Any) -> dict[str, Any]:
    return {"name": name, "status": "PASS" if passed else "FAIL", **evidence}


def _manifest_summary(*directories: Path) -> dict[str, Any]:
    records: list[dict[str, Any]] = []
    for directory in directories:
        resolved = Path(directory).resolve()
        if not resolved.is_dir():
            continue
        for path in resolved.glob("*.json"):
            records.append(json.loads(path.read_text(encoding="utf-8")))
    statuses = Counter(str(record.get("status", "UNKNOWN")) for record in records)
    return {
        "record_count": len(records),
        "status_counts": dict(sorted(statuses.items())),
    }


def build_quality_report(
    spark: Any,
    *,
    bronze_path: Path,
    silver_valid_path: Path,
    silver_rejected_path: Path,
    silver_duplicate_path: Path,
    station_dimension_path: Path,
    station_rejected_path: Path,
    gold_daily_metrics_path: Path,
    gold_popular_routes_path: Path,
    gold_cohort_retention_path: Path,
    bronze_manifest_path: Path,
    backfill_manifest_path: Path,
    output_path: Path,
    route_limit: int = 3,
    max_rejected_ratio: float = 0.05,
    max_duplicate_ratio: float = 0.05,
) -> dict[str, Any]:
    """Build and persist a machine-readable report with cross-layer invariants."""
    from pyspark.sql import Window, functions as F

    for name, value in (
        ("max_rejected_ratio", max_rejected_ratio),
        ("max_duplicate_ratio", max_duplicate_ratio),
    ):
        if isinstance(value, bool) or not 0 <= value <= 1:
            raise ValueError(f"{name} must be between 0 and 1")

    bronze = spark.read.parquet(str(_require_directory(bronze_path, "Bronze trip")))
    valid = spark.read.parquet(str(_require_directory(silver_valid_path, "Silver valid")))
    rejected = spark.read.parquet(
        str(_require_directory(silver_rejected_path, "Silver rejected"))
    )
    duplicates = spark.read.parquet(
        str(_require_directory(silver_duplicate_path, "Silver duplicate"))
    )
    dimension = spark.read.parquet(
        str(_require_directory(station_dimension_path, "Station dimension"))
    )
    station_rejected = spark.read.parquet(
        str(_require_directory(station_rejected_path, "Station rejected"))
    )
    daily = spark.read.parquet(
        str(_require_directory(gold_daily_metrics_path, "Gold daily metrics"))
    )
    routes = spark.read.parquet(
        str(_require_directory(gold_popular_routes_path, "Gold popular routes"))
    )
    retention = spark.read.parquet(
        str(_require_directory(gold_cohort_retention_path, "Gold cohort retention"))
    )

    counts = {
        "bronze_rows": bronze.count(),
        "silver_valid_rows": valid.count(),
        "silver_rejected_rows": rejected.count(),
        "silver_duplicate_rows": duplicates.count(),
        "station_dimension_rows": dimension.count(),
        "station_rejected_rows": station_rejected.count(),
        "gold_daily_metric_rows": daily.count(),
        "gold_popular_route_rows": routes.count(),
        "gold_cohort_retention_rows": retention.count(),
    }
    bronze_partitions = {
        row["ingestion_date"].isoformat(): row["count"]
        for row in bronze.groupBy("ingestion_date").count().collect()
    }
    rejected_error_counts = {
        row["error"]: row["count"]
        for row in rejected.select(F.explode("validation_errors").alias("error"))
        .groupBy("error")
        .count()
        .collect()
    }
    duplicate_kind_counts = {
        row["duplicate_kind"]: row["count"]
        for row in duplicates.groupBy("duplicate_kind").count().collect()
    }

    silver_total = (
        counts["silver_valid_rows"]
        + counts["silver_rejected_rows"]
        + counts["silver_duplicate_rows"]
    )
    denominator = counts["bronze_rows"] or 1
    rejected_ratio = counts["silver_rejected_rows"] / denominator
    duplicate_ratio = counts["silver_duplicate_rows"] / denominator
    current_version_violations = (
        dimension.groupBy("station_id")
        .agg(F.sum(F.col("is_current").cast("int")).alias("current_count"))
        .where(F.col("current_count") != 1)
        .count()
    )
    version_order = Window.partitionBy("station_id").orderBy("valid_from")
    intervals = dimension.withColumn(
        "next_valid_from", F.lead("valid_from").over(version_order)
    )
    interval_violations = intervals.where(
        (
            F.col("next_valid_from").isNotNull()
            & (
                F.col("valid_to").isNull()
                | (F.col("valid_to") != F.col("next_valid_from"))
            )
        )
        | (F.col("next_valid_from").isNull() & F.col("valid_to").isNotNull())
        | (F.col("is_current") != F.col("valid_to").isNull())
    ).count()
    gold_trip_count = int(
        daily.agg(F.coalesce(F.sum("trip_count"), F.lit(0)).alias("count")).first()[
            "count"
        ]
    )
    invalid_route_ranks = routes.where(
        (F.col("route_rank") < 1) | (F.col("route_rank") > route_limit)
    ).count()
    duplicate_route_ranks = (
        routes.groupBy("business_date", "district", "route_rank")
        .count()
        .where(F.col("count") > 1)
        .count()
    )
    retention_violations = retention.where(
        (F.col("days_since_cohort") < 0)
        | (F.col("retained_riders") > F.col("cohort_size"))
        | (F.col("retention_rate") < F.lit(0))
        | (F.col("retention_rate") > F.lit(1))
        | (
            (F.col("days_since_cohort") == 0)
            & (
                (F.col("retained_riders") != F.col("cohort_size"))
                | (F.col("retention_rate") != F.lit(1))
            )
        )
    ).count()
    checks = [
        _check(
            "silver_row_reconciliation",
            counts["bronze_rows"] == silver_total,
            bronze_rows=counts["bronze_rows"],
            reconciled_silver_rows=silver_total,
        ),
        _check(
            "silver_rejected_ratio",
            rejected_ratio <= max_rejected_ratio,
            observed_ratio=round(rejected_ratio, 6),
            maximum_ratio=max_rejected_ratio,
            rejected_rows=counts["silver_rejected_rows"],
            bronze_rows=counts["bronze_rows"],
        ),
        _check(
            "silver_duplicate_ratio",
            duplicate_ratio <= max_duplicate_ratio,
            observed_ratio=round(duplicate_ratio, 6),
            maximum_ratio=max_duplicate_ratio,
            duplicate_rows=counts["silver_duplicate_rows"],
            bronze_rows=counts["bronze_rows"],
        ),
        _check(
            "station_one_current_version",
            current_version_violations == 0,
            violation_count=current_version_violations,
        ),
        _check(
            "station_contiguous_intervals",
            interval_violations == 0,
            violation_count=interval_violations,
        ),
        _check(
            "gold_trip_reconciliation",
            counts["silver_valid_rows"] == gold_trip_count,
            silver_valid_rows=counts["silver_valid_rows"],
            gold_aggregated_trip_count=gold_trip_count,
        ),
        _check(
            "popular_route_rank_contract",
            invalid_route_ranks == 0 and duplicate_route_ranks == 0,
            invalid_rank_count=invalid_route_ranks,
            duplicate_rank_count=duplicate_route_ranks,
            configured_route_limit=route_limit,
        ),
        _check(
            "cohort_retention_contract",
            retention_violations == 0,
            violation_count=retention_violations,
        ),
    ]
    manifests = _manifest_summary(bronze_manifest_path, backfill_manifest_path)
    failed_manifests = manifests["status_counts"].get("FAILED", 0)
    if any(check["status"] == "FAIL" for check in checks):
        overall_status = "FAIL"
    elif failed_manifests:
        overall_status = "WARN"
    else:
        overall_status = "PASS"
    report = {
        "generated_at_utc": utc_timestamp(),
        "overall_status": overall_status,
        "dataset_counts": counts,
        "bronze_partition_rows": dict(sorted(bronze_partitions.items())),
        "silver_rejected_error_counts": dict(sorted(rejected_error_counts.items())),
        "silver_duplicate_kind_counts": dict(sorted(duplicate_kind_counts.items())),
        "manifest_summary": manifests,
        "checks": checks,
    }
    write_json_atomic(Path(output_path).resolve(), report)
    return report
