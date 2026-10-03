"""Bronze ingestion with explicit schema and dynamic partition overwrite."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any

from .contracts import trip_source_schema


@dataclass(frozen=True)
class BronzeLoadResult:
    ingestion_date: str
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
) -> BronzeLoadResult:
    from pyspark.sql import functions as F

    input_path = Path(input_path).resolve()
    output_path = Path(output_path).resolve()
    if not input_path.is_file():
        raise ValueError(f"input file does not exist: {input_path}")
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
    return BronzeLoadResult(
        ingestion_date=ingestion_date.isoformat(),
        input_rows=input_rows,
        output_rows_in_partition=partition_rows,
        corrupt_rows=corrupt_rows,
    )
