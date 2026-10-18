"""Read-only impact planning for a controlled date backfill."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any

from .backfill import _validate_source_dates
from .batch_manifest import fingerprint_file
from .contracts import trip_source_schema
from .gold import cohort_dates_for_riders, silver_rider_keys_for_date
from .silver import validate_partition_isolated_backfill


@dataclass(frozen=True)
class BackfillImpactPlan:
    plan_id: str
    target_date: str
    source_file_name: str
    source_sha256: str
    source_bytes: int
    source_rows: int
    source_distinct_trip_ids: int
    source_distinct_riders: int
    current_bronze_rows: int
    current_silver_rows: int
    affected_rider_count: int
    affected_bronze_partitions: tuple[str, ...]
    affected_silver_partitions: tuple[str, ...]
    affected_gold_daily_metric_partitions: tuple[str, ...]
    affected_gold_popular_route_partitions: tuple[str, ...]
    potential_gold_cohort_partitions: tuple[str, ...]
    bronze_files_scanned: int
    silver_files_scanned: int
    read_only: bool = True

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def _parquet_file_count(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(1 for item in path.rglob("*.parquet") if item.is_file())


def _partition_row_count(spark: Any, path: Path, predicate: str) -> int:
    if not path.exists() or not any(path.rglob("*.parquet")):
        return 0
    return spark.read.parquet(str(path)).where(predicate).count()


def plan_date_backfill(
    spark: Any,
    *,
    input_path: Path,
    target_date: date,
    bronze_path: Path,
    silver_valid_path: Path,
) -> BackfillImpactPlan:
    """Inspect a replacement date without writing manifests or data partitions."""
    from pyspark.sql import functions as F

    input_path = Path(input_path).resolve()
    bronze_path = Path(bronze_path).resolve()
    silver_valid_path = Path(silver_valid_path).resolve()
    if not input_path.is_file():
        raise ValueError(f"Backfill input file does not exist: {input_path}")

    _validate_source_dates(spark, input_path, target_date)
    validate_partition_isolated_backfill(
        spark,
        bronze_path=bronze_path,
        input_path=input_path,
        target_date=target_date,
    )

    source_sha256, source_bytes = fingerprint_file(input_path)
    source = spark.read.schema(trip_source_schema()).json(str(input_path)).cache()
    try:
        source_rows = source.count()
        source_distinct_trip_ids = source.where(F.col("trip_id").isNotNull()).select(
            "trip_id"
        ).distinct().count()
        source_riders = source.where(F.col("rider_key").isNotNull()).select(
            "rider_key"
        ).distinct()
        source_distinct_riders = source_riders.count()

        current_target_riders = silver_rider_keys_for_date(
            spark,
            silver_trip_path=silver_valid_path,
            target_date=target_date,
        )
        affected_riders = current_target_riders.unionByName(source_riders).distinct().cache()
        try:
            affected_rider_count = affected_riders.count()
            current_cohorts = cohort_dates_for_riders(
                spark,
                silver_trip_path=silver_valid_path,
                rider_keys=affected_riders,
            )
            later_candidates: set[date] = set()
            if silver_valid_path.exists() and any(silver_valid_path.rglob("*.parquet")):
                later_candidates = {
                    row["next_activity_date"]
                    for row in (
                        spark.read.parquet(str(silver_valid_path))
                        .join(affected_riders, "rider_key", "inner")
                        .where(F.col("business_date") > F.lit(target_date.isoformat()).cast("date"))
                        .groupBy("rider_key")
                        .agg(F.min("business_date").alias("next_activity_date"))
                        .select("next_activity_date")
                        .distinct()
                        .collect()
                    )
                    if row["next_activity_date"] is not None
                }
        finally:
            affected_riders.unpersist()
    finally:
        source.unpersist()

    potential_cohorts = sorted(current_cohorts | later_candidates | {target_date})
    day = target_date.isoformat()
    return BackfillImpactPlan(
        plan_id=f"trip-backfill-plan-{target_date:%Y%m%d}-{source_sha256[:16]}",
        target_date=day,
        source_file_name=input_path.name,
        source_sha256=source_sha256,
        source_bytes=source_bytes,
        source_rows=source_rows,
        source_distinct_trip_ids=source_distinct_trip_ids,
        source_distinct_riders=source_distinct_riders,
        current_bronze_rows=_partition_row_count(
            spark, bronze_path, f"ingestion_date = DATE '{day}'"
        ),
        current_silver_rows=_partition_row_count(
            spark, silver_valid_path, f"ingestion_date = DATE '{day}'"
        ),
        affected_rider_count=affected_rider_count,
        affected_bronze_partitions=(day,),
        affected_silver_partitions=(day,),
        affected_gold_daily_metric_partitions=(day,),
        affected_gold_popular_route_partitions=(day,),
        potential_gold_cohort_partitions=tuple(item.isoformat() for item in potential_cohorts),
        bronze_files_scanned=_parquet_file_count(bronze_path),
        silver_files_scanned=_parquet_file_count(silver_valid_path),
    )
