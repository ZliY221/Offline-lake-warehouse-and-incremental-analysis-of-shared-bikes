"""Silver trip validation, deterministic deduplication, and rejected records."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any

from .contracts import trip_source_schema
from .generator import BIKE_TYPES, RIDER_TYPES, STATIONS
from .partition_io import replace_date_partition


BUSINESS_COLUMNS = [
    "trip_id",
    "rider_key",
    "started_at",
    "ended_at",
    "start_station_id",
    "end_station_id",
    "rider_type",
    "bike_type",
    "distance_km",
]


@dataclass(frozen=True)
class SilverBuildResult:
    input_rows: int
    valid_rows: int
    rejected_rows: int
    duplicate_rows: int
    exact_duplicate_rows: int
    conflicting_duplicate_rows: int

    def to_dict(self) -> dict[str, int]:
        return asdict(self)


def _with_validation(dataframe: Any) -> Any:
    from pyspark.sql import functions as F

    required = [
        "trip_id",
        "rider_key",
        "started_at",
        "ended_at",
        "start_station_id",
        "end_station_id",
        "rider_type",
        "bike_type",
        "distance_km",
    ]
    missing_required = F.lit(False)
    for column in required:
        missing_required = missing_required | F.col(column).isNull()

    allowed_stations = [station[0] for station in STATIONS]
    validation_errors = F.array(
        F.when(F.col("_corrupt_record").isNotNull(), F.lit("MALFORMED_RECORD")),
        F.when(missing_required, F.lit("MISSING_REQUIRED_FIELD")),
        F.when(
            F.col("trip_id").isNotNull() & ~F.col("trip_id").startswith("trip_"),
            F.lit("INVALID_TRIP_ID"),
        ),
        F.when(
            F.col("rider_key").isNotNull()
            & ~F.col("rider_key").rlike(r"^rider_[0-9a-f]{16}$"),
            F.lit("INVALID_RIDER_KEY"),
        ),
        F.when(
            F.col("started_at").isNotNull()
            & F.col("ended_at").isNotNull()
            & (F.col("ended_at") <= F.col("started_at")),
            F.lit("INVALID_TIME_ORDER"),
        ),
        F.when(
            F.col("started_at").isNotNull()
            & F.col("ended_at").isNotNull()
            & (F.col("ended_at").cast("long") - F.col("started_at").cast("long") > 14_400),
            F.lit("DURATION_EXCEEDS_LIMIT"),
        ),
        F.when(
            F.col("start_station_id").isNotNull()
            & ~F.col("start_station_id").isin(allowed_stations),
            F.lit("INVALID_START_STATION"),
        ),
        F.when(
            F.col("end_station_id").isNotNull()
            & ~F.col("end_station_id").isin(allowed_stations),
            F.lit("INVALID_END_STATION"),
        ),
        F.when(
            F.col("start_station_id").isNotNull()
            & F.col("end_station_id").isNotNull()
            & (F.col("start_station_id") == F.col("end_station_id")),
            F.lit("SAME_START_END_STATION"),
        ),
        F.when(
            F.col("rider_type").isNotNull() & ~F.col("rider_type").isin(list(RIDER_TYPES)),
            F.lit("INVALID_RIDER_TYPE"),
        ),
        F.when(
            F.col("bike_type").isNotNull() & ~F.col("bike_type").isin(list(BIKE_TYPES)),
            F.lit("INVALID_BIKE_TYPE"),
        ),
        F.when(
            F.col("distance_km").isNotNull()
            & ((F.col("distance_km") <= F.lit(0)) | (F.col("distance_km") > F.lit(100))),
            F.lit("INVALID_DISTANCE"),
        ),
    )
    canonical_json = F.to_json(F.struct(*[F.col(column) for column in BUSINESS_COLUMNS]))
    fingerprint_input = F.coalesce(F.col("_corrupt_record"), canonical_json)
    return (
        dataframe.withColumn(
            "validation_errors",
            F.filter(validation_errors, lambda error: error.isNotNull()),
        )
        .withColumn("record_fingerprint", F.sha2(fingerprint_input, 256))
        .withColumn(
            "corrupt_payload_bytes",
            F.when(
                F.col("_corrupt_record").isNotNull(),
                F.length(F.encode(F.col("_corrupt_record"), "UTF-8")),
            ),
        )
    )


def _write_dataset(
    dataframe: Any,
    output_path: Path,
    partition_column: str,
    row_count: int,
) -> None:
    writer = dataframe.write.mode("overwrite")
    if row_count > 0:
        writer = writer.partitionBy(partition_column)
    writer.parquet(str(Path(output_path).resolve()))


def _classify_validated(validated: Any) -> tuple[Any, Any, Any]:
    from pyspark.sql import Window, functions as F

    rejected = (
        validated.where(F.size("validation_errors") > 0)
        .drop("_corrupt_record")
        .withColumnRenamed("record_fingerprint", "source_record_sha256")
    )
    candidates = validated.where(F.size("validation_errors") == 0).drop(
        "validation_errors", "_corrupt_record", "corrupt_payload_bytes"
    )
    trip_stats = candidates.groupBy("trip_id").agg(
        F.count(F.lit(1)).alias("trip_id_row_count"),
        F.countDistinct("record_fingerprint").alias("trip_id_fingerprint_count"),
    )
    classified = candidates.join(trip_stats, "trip_id", "inner")
    conflicts = classified.where(F.col("trip_id_fingerprint_count") > 1).withColumn(
        "duplicate_kind", F.lit("conflicting_duplicate")
    )
    exact_groups = classified.where(F.col("trip_id_fingerprint_count") == 1)
    order = Window.partitionBy("trip_id").orderBy(
        F.col("ingestion_date"), F.col("source_file"), F.col("record_fingerprint")
    )
    ranked = exact_groups.withColumn("duplicate_rank", F.row_number().over(order))
    exact_duplicates = ranked.where(F.col("duplicate_rank") > 1).withColumn(
        "duplicate_kind", F.lit("exact_duplicate")
    )
    duplicate_columns = BUSINESS_COLUMNS + [
        "source_file",
        "ingestion_date",
        "record_fingerprint",
        "trip_id_row_count",
        "trip_id_fingerprint_count",
        "duplicate_kind",
    ]
    duplicates = conflicts.select(*duplicate_columns).unionByName(
        exact_duplicates.select(*duplicate_columns)
    )
    valid = (
        ranked.where(F.col("duplicate_rank") == 1)
        .drop(
            "duplicate_rank",
            "trip_id_row_count",
            "trip_id_fingerprint_count",
        )
        .withColumn("business_date", F.to_date("started_at"))
        .withColumn(
            "trip_duration_minutes",
            (
                (F.col("ended_at").cast("long") - F.col("started_at").cast("long"))
                / F.lit(60)
            ).cast("decimal(10,2)"),
        )
    )
    return valid, rejected, duplicates


def _measure_silver(
    valid: Any,
    rejected: Any,
    duplicates: Any,
    input_rows: int,
) -> SilverBuildResult:
    from pyspark.sql import functions as F

    rejected_rows = rejected.count()
    duplicate_rows = duplicates.count()
    return SilverBuildResult(
        input_rows=input_rows,
        valid_rows=valid.count(),
        rejected_rows=rejected_rows,
        duplicate_rows=duplicate_rows,
        exact_duplicate_rows=duplicates.where(
            F.col("duplicate_kind") == "exact_duplicate"
        ).count(),
        conflicting_duplicate_rows=duplicates.where(
            F.col("duplicate_kind") == "conflicting_duplicate"
        ).count(),
    )


def _valid_candidate_partitions(validated: Any) -> Any:
    from pyspark.sql import functions as F

    return validated.where(F.size("validation_errors") == 0).select(
        "trip_id", "ingestion_date"
    )


def _assert_partition_isolation(candidates: Any) -> None:
    from pyspark.sql import functions as F

    has_cross_partition_id = (
        candidates.groupBy("trip_id")
        .agg(F.countDistinct("ingestion_date").alias("partition_count"))
        .where(F.col("partition_count") > 1)
        .limit(1)
        .count()
    )
    if has_cross_partition_id:
        raise ValueError(
            "Incremental backfill requires each valid trip_id to stay within one "
            "ingestion_date partition; the Bronze dataset violates this invariant"
        )


def validate_partition_isolated_backfill(
    spark: Any,
    *,
    bronze_path: Path,
    input_path: Path,
    target_date: date,
) -> None:
    """Reject local recomputation when a valid trip ID crosses ingestion partitions."""
    from pyspark.sql import functions as F

    bronze_path = Path(bronze_path).resolve()
    if not bronze_path.is_dir():
        return
    existing_validated = _with_validation(spark.read.parquet(str(bronze_path)))
    existing_candidates = _valid_candidate_partitions(existing_validated)
    _assert_partition_isolation(existing_candidates)

    source = (
        spark.read.schema(trip_source_schema())
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(str(Path(input_path).resolve()))
        .withColumn("ingestion_date", F.lit(target_date.isoformat()).cast("date"))
        .withColumn("source_file", F.input_file_name())
    )
    source_ids = _with_validation(source).where(F.size("validation_errors") == 0).select(
        "trip_id"
    ).distinct()
    other_partition_ids = existing_candidates.where(
        F.col("ingestion_date") != F.lit(target_date.isoformat()).cast("date")
    ).select("trip_id").distinct()
    if source_ids.join(other_partition_ids, "trip_id", "inner").limit(1).count():
        raise ValueError(
            "Incremental backfill input contains a valid trip_id already present in another "
            "ingestion_date partition"
        )


def build_trip_silver(
    spark: Any,
    *,
    bronze_path: Path,
    valid_path: Path,
    rejected_path: Path,
    duplicate_path: Path,
) -> SilverBuildResult:
    """Rebuild Silver outputs from the full Bronze trip dataset."""
    bronze_path = Path(bronze_path).resolve()
    if not bronze_path.is_dir():
        raise ValueError(f"Bronze trip dataset does not exist: {bronze_path}")

    validated = _with_validation(spark.read.parquet(str(bronze_path))).cache()
    try:
        input_rows = validated.count()
        valid, rejected, duplicates = _classify_validated(validated)
        result = _measure_silver(valid, rejected, duplicates, input_rows)
        _write_dataset(valid, valid_path, "ingestion_date", result.valid_rows)
        _write_dataset(rejected, rejected_path, "ingestion_date", result.rejected_rows)
        _write_dataset(duplicates, duplicate_path, "ingestion_date", result.duplicate_rows)
    finally:
        validated.unpersist()

    return result


def build_trip_silver_partition(
    spark: Any,
    *,
    bronze_path: Path,
    target_date: date,
    valid_path: Path,
    rejected_path: Path,
    duplicate_path: Path,
) -> SilverBuildResult:
    """Recompute exactly one isolated ingestion-date partition in all Silver outputs."""
    from pyspark.sql import functions as F

    bronze_path = Path(bronze_path).resolve()
    if not bronze_path.is_dir():
        raise ValueError(f"Bronze trip dataset does not exist: {bronze_path}")
    target = F.lit(target_date.isoformat()).cast("date")
    all_validated = _with_validation(spark.read.parquet(str(bronze_path))).cache()
    try:
        _assert_partition_isolation(_valid_candidate_partitions(all_validated))
        validated = all_validated.where(F.col("ingestion_date") == target)
        input_rows = validated.count()
        valid, rejected, duplicates = _classify_validated(validated)
        result = _measure_silver(valid, rejected, duplicates, input_rows)
        replace_date_partition(valid, valid_path, "ingestion_date", target_date)
        replace_date_partition(rejected, rejected_path, "ingestion_date", target_date)
        replace_date_partition(duplicates, duplicate_path, "ingestion_date", target_date)
    finally:
        all_validated.unpersist()
    return result


def valid_rider_keys_for_bronze_partition(
    spark: Any,
    *,
    bronze_path: Path,
    target_date: date,
) -> Any:
    """Preview rider keys that will survive validation and deduplication in one partition."""
    from pyspark.sql import functions as F

    bronze_path = Path(bronze_path).resolve()
    if not bronze_path.is_dir():
        raise ValueError(f"Bronze trip dataset does not exist: {bronze_path}")
    all_validated = _with_validation(spark.read.parquet(str(bronze_path)))
    _assert_partition_isolation(_valid_candidate_partitions(all_validated))
    target = F.lit(target_date.isoformat()).cast("date")
    valid, _, _ = _classify_validated(
        all_validated.where(F.col("ingestion_date") == target)
    )
    return valid.select("rider_key").distinct()
