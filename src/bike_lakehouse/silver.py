"""Silver trip validation, deterministic deduplication, and rejected records."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from .generator import BIKE_TYPES, RIDER_TYPES, STATIONS


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


def build_trip_silver(
    spark: Any,
    *,
    bronze_path: Path,
    valid_path: Path,
    rejected_path: Path,
    duplicate_path: Path,
) -> SilverBuildResult:
    """Rebuild Silver outputs from the full Bronze trip dataset."""
    from pyspark.sql import Window, functions as F

    bronze_path = Path(bronze_path).resolve()
    if not bronze_path.is_dir():
        raise ValueError(f"Bronze trip dataset does not exist: {bronze_path}")

    validated = _with_validation(spark.read.parquet(str(bronze_path))).cache()
    try:
        input_rows = validated.count()
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
        conflicts = (
            classified.where(F.col("trip_id_fingerprint_count") > 1)
            .withColumn("duplicate_kind", F.lit("conflicting_duplicate"))
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

        rejected_rows = rejected.count()
        duplicate_rows = duplicates.count()
        exact_duplicate_rows = duplicates.where(
            F.col("duplicate_kind") == "exact_duplicate"
        ).count()
        conflicting_duplicate_rows = duplicates.where(
            F.col("duplicate_kind") == "conflicting_duplicate"
        ).count()
        valid_rows = valid.count()

        _write_dataset(valid, valid_path, "ingestion_date", valid_rows)
        _write_dataset(rejected, rejected_path, "ingestion_date", rejected_rows)
        _write_dataset(duplicates, duplicate_path, "ingestion_date", duplicate_rows)
    finally:
        validated.unpersist()

    return SilverBuildResult(
        input_rows=input_rows,
        valid_rows=valid_rows,
        rejected_rows=rejected_rows,
        duplicate_rows=duplicate_rows,
        exact_duplicate_rows=exact_duplicate_rows,
        conflicting_duplicate_rows=conflicting_duplicate_rows,
    )
