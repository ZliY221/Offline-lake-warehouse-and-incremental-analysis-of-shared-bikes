"""Controlled single-date backfill across Bronze, Silver, and Gold."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any

from .batch_manifest import BatchManifest, fingerprint_file
from .bronze import ingest_trip_bronze
from .contracts import trip_source_schema
from .gold import build_gold_analytics
from .silver import build_trip_silver


@dataclass(frozen=True)
class BackfillResult:
    backfill_id: str
    target_date: str
    bronze_partition_rows: int
    silver_valid_rows: int
    silver_rejected_rows: int
    silver_duplicate_rows: int
    gold_daily_metric_rows: int
    gold_popular_route_rows: int

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def _validate_source_dates(spark: Any, input_path: Path, target_date: date) -> None:
    from pyspark.sql import functions as F

    source_dates = (
        spark.read.schema(trip_source_schema())
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(str(input_path))
        .where(F.col("started_at").isNotNull())
        .select(F.to_date("started_at").alias("business_date"))
        .distinct()
        .collect()
    )
    unexpected = sorted(
        row["business_date"].isoformat()
        for row in source_dates
        if row["business_date"] != target_date
    )
    if unexpected:
        raise ValueError(
            f"Backfill source contains dates outside {target_date.isoformat()}: {unexpected}"
        )


def run_date_backfill(
    spark: Any,
    *,
    input_path: Path,
    target_date: date,
    bronze_path: Path,
    bronze_manifest_path: Path,
    silver_valid_path: Path,
    silver_rejected_path: Path,
    silver_duplicate_path: Path,
    station_dimension_path: Path,
    gold_daily_metrics_path: Path,
    gold_popular_routes_path: Path,
    backfill_manifest_path: Path,
) -> BackfillResult:
    """Replace one Bronze partition, then fully rebuild dependent Silver and Gold."""
    input_path = Path(input_path).resolve()
    if not input_path.is_file():
        raise ValueError(f"Backfill input file does not exist: {input_path}")
    source_sha256, source_bytes = fingerprint_file(input_path)
    backfill_id = f"trip-backfill-{target_date:%Y%m%d}-{source_sha256[:16]}"
    manifest = BatchManifest(
        backfill_manifest_path,
        backfill_id,
        {
            "pipeline": "trip_date_backfill",
            "target_date": target_date.isoformat(),
            "source_file_name": input_path.name,
            "source_sha256": source_sha256,
            "source_bytes": source_bytes,
            "affected_bronze_partitions": [target_date.isoformat()],
            "silver_rebuild_scope": "FULL_DATASET",
            "gold_rebuild_scope": "FULL_DATASET",
        },
    )
    try:
        _validate_source_dates(spark, input_path, target_date)
        bronze = ingest_trip_bronze(
            spark,
            input_path=input_path,
            output_path=bronze_path,
            ingestion_date=target_date,
            manifest_path=bronze_manifest_path,
        )
        silver = build_trip_silver(
            spark,
            bronze_path=bronze_path,
            valid_path=silver_valid_path,
            rejected_path=silver_rejected_path,
            duplicate_path=silver_duplicate_path,
        )
        gold = build_gold_analytics(
            spark,
            silver_trip_path=silver_valid_path,
            station_dimension_path=station_dimension_path,
            daily_metrics_path=gold_daily_metrics_path,
            popular_routes_path=gold_popular_routes_path,
        )
        result = BackfillResult(
            backfill_id=backfill_id,
            target_date=target_date.isoformat(),
            bronze_partition_rows=bronze.output_rows_in_partition,
            silver_valid_rows=silver.valid_rows,
            silver_rejected_rows=silver.rejected_rows,
            silver_duplicate_rows=silver.duplicate_rows,
            gold_daily_metric_rows=gold.daily_metric_rows,
            gold_popular_route_rows=gold.popular_route_rows,
        )
        manifest.complete(
            input_rows=bronze.input_rows,
            corrupt_rows=bronze.corrupt_rows,
            output_rows_in_partition=bronze.output_rows_in_partition,
            bronze_result=bronze.to_dict(),
            silver_result=silver.to_dict(),
            gold_result=gold.to_dict(),
        )
    except Exception as error:
        manifest.fail(error)
        raise
    return result
