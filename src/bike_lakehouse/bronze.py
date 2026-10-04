"""Bronze ingestion with explicit schema and dynamic partition overwrite."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any

from .batch_manifest import BatchManifest, fingerprint_file
from .contracts import trip_source_schema


@dataclass(frozen=True)
class BronzeLoadResult:
    batch_id: str
    ingestion_date: str
    source_sha256: str
    input_rows: int
    output_rows_in_partition: int
    corrupt_rows: int

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


def ingest_trip_bronze(
    spark: Any,
    *,
    input_path: Path,
    output_path: Path,
    ingestion_date: date,
    manifest_path: Path | None = None,
) -> BronzeLoadResult:
    from pyspark.sql import functions as F

    input_path = Path(input_path).resolve()
    output_path = Path(output_path).resolve()
    if not input_path.is_file():
        raise ValueError(f"input file does not exist: {input_path}")
    source_sha256, source_bytes = fingerprint_file(input_path)
    batch_id = f"trip-bronze-{ingestion_date:%Y%m%d}-{source_sha256[:16]}"
    manifest = (
        BatchManifest(
            manifest_path,
            batch_id,
            {
                "pipeline": "trip_bronze",
                "ingestion_date": ingestion_date.isoformat(),
                "source_file_name": input_path.name,
                "source_sha256": source_sha256,
                "source_bytes": source_bytes,
            },
        )
        if manifest_path is not None
        else None
    )
    try:
        dataframe = (
            spark.read.schema(trip_source_schema())
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .json(str(input_path))
            .withColumn("ingestion_date", F.lit(ingestion_date.isoformat()).cast("date"))
            .withColumn("source_file", F.input_file_name())
        )
        dataframe.cache()
        try:
            input_rows = dataframe.count()
            corrupt_rows = dataframe.where(F.col("_corrupt_record").isNotNull()).count()
            (
                dataframe.write.option("partitionOverwriteMode", "dynamic")
                .mode("overwrite")
                .partitionBy("ingestion_date")
                .parquet(str(output_path))
            )
        finally:
            dataframe.unpersist()
        partition_rows = (
            spark.read.parquet(str(output_path))
            .where(F.col("ingestion_date") == F.lit(ingestion_date.isoformat()).cast("date"))
            .count()
        )
        if manifest is not None:
            manifest.complete(
                input_rows=input_rows,
                corrupt_rows=corrupt_rows,
                output_rows_in_partition=partition_rows,
            )
    except Exception as error:
        if manifest is not None:
            manifest.fail(error)
        raise
    return BronzeLoadResult(
        batch_id=batch_id,
        ingestion_date=ingestion_date.isoformat(),
        source_sha256=source_sha256,
        input_rows=input_rows,
        output_rows_in_partition=partition_rows,
        corrupt_rows=corrupt_rows,
    )
