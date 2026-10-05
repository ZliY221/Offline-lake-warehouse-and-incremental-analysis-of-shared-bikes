"""Small, deterministic local Spark session factory."""

from __future__ import annotations

from pathlib import Path


def create_local_spark(
    app_name: str,
    warehouse_directory: Path | None = None,
    *,
    ui_enabled: bool = False,
):
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[2]")
        .appName(app_name)
        .config("spark.ui.enabled", str(ui_enabled).lower())
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    )
    if warehouse_directory is not None:
        builder = builder.config(
            "spark.sql.warehouse.dir", warehouse_directory.resolve().as_uri()
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
