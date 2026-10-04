"""Build a validated station SCD2 dimension from dated full snapshots."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable

from .contracts import station_source_schema


STATION_BUSINESS_COLUMNS = [
    "snapshot_date",
    "station_id",
    "station_name",
    "district",
    "capacity",
    "active",
]
TRACKED_ATTRIBUTES = ["station_name", "district", "capacity", "active"]


@dataclass(frozen=True)
class StationDimensionResult:
    input_rows: int
    valid_snapshot_rows: int
    rejected_rows: int
    exact_duplicate_rows: int
    conflicting_snapshot_rows: int
    dimension_versions: int
    current_versions: int

    def to_dict(self) -> dict[str, int]:
        return asdict(self)


def _read_and_validate(spark: Any, input_paths: Iterable[Path]) -> Any:
    from pyspark.sql import functions as F

    resolved = [str(Path(path).resolve()) for path in input_paths]
    if not resolved:
        raise ValueError("At least one station snapshot path is required")
    for path in resolved:
        if not Path(path).is_file():
            raise ValueError(f"Station snapshot does not exist: {path}")
    source = (
        spark.read.schema(station_source_schema())
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .json(resolved)
        .withColumn("source_file", F.input_file_name())
    )
    errors = F.array(
        F.when(F.col("_corrupt_record").isNotNull(), F.lit("MALFORMED_RECORD")),
        F.when(F.col("snapshot_date").isNull(), F.lit("MISSING_SNAPSHOT_DATE")),
        F.when(F.col("station_id").isNull(), F.lit("MISSING_STATION_ID")),
        F.when(
            F.col("station_id").isNotNull() & ~F.col("station_id").rlike(r"^ST-[0-9]{3}$"),
            F.lit("INVALID_STATION_ID"),
        ),
        F.when(
            F.col("station_name").isNull() | (F.trim(F.col("station_name")) == ""),
            F.lit("INVALID_STATION_NAME"),
        ),
        F.when(
            F.col("district").isNull() | (F.trim(F.col("district")) == ""),
            F.lit("INVALID_DISTRICT"),
        ),
        F.when(
            F.col("capacity").isNull() | (F.col("capacity") <= 0),
            F.lit("INVALID_CAPACITY"),
        ),
        F.when(F.col("active").isNull(), F.lit("MISSING_ACTIVE_FLAG")),
    )
    canonical_json = F.to_json(F.struct(*[F.col(c) for c in STATION_BUSINESS_COLUMNS]))
    fingerprint_input = F.coalesce(F.col("_corrupt_record"), canonical_json)
    return (
        source.withColumn(
            "validation_errors",
            F.filter(errors, lambda error: error.isNotNull()),
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


def _write_dataset(dataframe: Any, output_path: Path) -> None:
    # The dimension is intentionally small and unpartitioned. Partitioning by a
    # Boolean current flag would turn that column into a path-derived string on
    # some Spark readers, weakening the output contract.
    dataframe.write.mode("overwrite").parquet(str(Path(output_path).resolve()))


def build_station_dimension(
    spark: Any,
    *,
    input_paths: Iterable[Path],
    dimension_path: Path,
    rejected_path: Path,
) -> StationDimensionResult:
    """Full rebuild of non-overlapping, end-exclusive SCD2 station history."""
    from pyspark.sql import Window, functions as F

    validated = _read_and_validate(spark, input_paths).cache()
    try:
        input_rows = validated.count()
        base_rejected = validated.where(F.size("validation_errors") > 0)
        candidates = validated.where(F.size("validation_errors") == 0)
        snapshot_stats = candidates.groupBy("station_id", "snapshot_date").agg(
            F.count(F.lit(1)).alias("snapshot_row_count"),
            F.countDistinct("record_fingerprint").alias("snapshot_fingerprint_count"),
        )
        classified = candidates.join(
            snapshot_stats,
            ["station_id", "snapshot_date"],
            "inner",
        )
        conflicts = (
            classified.where(F.col("snapshot_fingerprint_count") > 1)
            .drop("snapshot_row_count", "snapshot_fingerprint_count")
            .withColumn(
                "validation_errors",
                F.array(F.lit("CONFLICTING_STATION_SNAPSHOT")),
            )
        )
        non_conflicting = classified.where(F.col("snapshot_fingerprint_count") == 1)
        exact_duplicate_rows = (
            snapshot_stats.where(F.col("snapshot_fingerprint_count") == 1).agg(
                F.coalesce(
                    F.sum(F.col("snapshot_row_count") - F.lit(1)),
                    F.lit(0),
                ).alias("count")
            ).first()["count"]
        )
        dedup_order = Window.partitionBy("station_id", "snapshot_date").orderBy(
            "source_file", "record_fingerprint"
        )
        clean = (
            non_conflicting.withColumn("snapshot_rank", F.row_number().over(dedup_order))
            .where(F.col("snapshot_rank") == 1)
            .drop(
                "snapshot_rank",
                "snapshot_row_count",
                "snapshot_fingerprint_count",
                "validation_errors",
                "_corrupt_record",
                "corrupt_payload_bytes",
            )
        )
        rejects = (
            base_rejected.unionByName(conflicts)
            .drop("_corrupt_record")
            .withColumnRenamed("record_fingerprint", "source_record_sha256")
        )

        history_order = Window.partitionBy("station_id").orderBy("snapshot_date")
        with_previous = clean.withColumn("snapshot_sequence", F.row_number().over(history_order))
        for attribute in TRACKED_ATTRIBUTES:
            previous = f"previous_{attribute}"
            with_previous = with_previous.withColumn(previous, F.lag(attribute).over(history_order))
        changed = F.col("snapshot_sequence") == 1
        for attribute in TRACKED_ATTRIBUTES:
            previous = f"previous_{attribute}"
            changed = changed | ~F.col(attribute).eqNullSafe(F.col(previous))
        versions = with_previous.withColumn("is_changed_snapshot", changed).where(
            "is_changed_snapshot"
        )
        version_order = Window.partitionBy("station_id").orderBy("snapshot_date")
        dimension = (
            versions.withColumn("valid_from", F.col("snapshot_date"))
            .withColumn("valid_to", F.lead("snapshot_date").over(version_order))
            .withColumn("is_current", F.col("valid_to").isNull())
            .select(
                "station_id",
                *TRACKED_ATTRIBUTES,
                "valid_from",
                "valid_to",
                "is_current",
                "source_file",
                "record_fingerprint",
            )
        )

        valid_snapshot_rows = clean.count()
        rejected_rows = rejects.count()
        conflicting_snapshot_rows = conflicts.count()
        dimension_versions = dimension.count()
        current_versions = dimension.where("is_current").count()
        _write_dataset(dimension, dimension_path)
        _write_dataset(rejects, rejected_path)
    finally:
        validated.unpersist()

    return StationDimensionResult(
        input_rows=input_rows,
        valid_snapshot_rows=valid_snapshot_rows,
        rejected_rows=rejected_rows,
        exact_duplicate_rows=int(exact_duplicate_rows),
        conflicting_snapshot_rows=conflicting_snapshot_rows,
        dimension_versions=dimension_versions,
        current_versions=current_versions,
    )
