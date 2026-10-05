"""Gold daily metrics and ranked routes with point-in-time station joins."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any

from .partition_io import replace_date_partition


@dataclass(frozen=True)
class GoldBuildResult:
    input_trip_rows: int
    enriched_trip_rows: int
    daily_metric_rows: int
    popular_route_rows: int
    cohort_retention_rows: int
    cohort_input_trip_rows: int

    def to_dict(self) -> dict[str, int]:
        return asdict(self)


def _require_dataset(path: Path, label: str) -> Path:
    resolved = Path(path).resolve()
    if not resolved.is_dir():
        raise ValueError(f"{label} dataset does not exist: {resolved}")
    return resolved


def _write_partitioned(
    dataframe: Any,
    output_path: Path,
    row_count: int,
    partition_column: str,
) -> None:
    writer = dataframe.write.mode("overwrite")
    if row_count > 0:
        writer = writer.partitionBy(partition_column)
    writer.parquet(str(Path(output_path).resolve()))


def _join_station_version(trips: Any, station_dimension: Any, prefix: str) -> Any:
    from pyspark.sql import functions as F

    trip_alias = "trip"
    dimension_alias = f"{prefix}_dimension"
    station_column = f"{prefix}_station_id"
    joined = trips.alias(trip_alias).join(
        station_dimension.alias(dimension_alias),
        (F.col(f"{trip_alias}.{station_column}") == F.col(f"{dimension_alias}.station_id"))
        & (F.col(f"{trip_alias}.business_date") >= F.col(f"{dimension_alias}.valid_from"))
        & (
            F.col(f"{dimension_alias}.valid_to").isNull()
            | (F.col(f"{trip_alias}.business_date") < F.col(f"{dimension_alias}.valid_to"))
        ),
        "left",
    )
    return joined.select(
        f"{trip_alias}.*",
        F.col(f"{dimension_alias}.station_id").alias(f"_{prefix}_dimension_station_id"),
        F.col(f"{dimension_alias}.station_name").alias(f"{prefix}_station_name"),
        F.col(f"{dimension_alias}.district").alias(f"{prefix}_district"),
    )


def _require_complete_station_joins(enriched: Any) -> Any:
    from pyspark.sql import functions as F

    unmatched_start = enriched.where(F.col("_start_dimension_station_id").isNull()).count()
    unmatched_end = enriched.where(F.col("_end_dimension_station_id").isNull()).count()
    if unmatched_start or unmatched_end:
        raise ValueError(
            "Point-in-time station lookup failed for "
            f"{unmatched_start} start and {unmatched_end} end station rows"
        )
    return enriched.drop(
        "_start_dimension_station_id",
        "_end_dimension_station_id",
    )


def _build_daily_metrics(enriched: Any) -> Any:
    from pyspark.sql import functions as F

    return (
        enriched.groupBy(
            "business_date",
            "start_district",
            "rider_type",
            "bike_type",
        )
        .agg(
            F.count(F.lit(1)).alias("trip_count"),
            F.sum("trip_duration_minutes")
            .cast("decimal(14,2)")
            .alias("total_trip_duration_minutes"),
            F.avg("trip_duration_minutes")
            .cast("decimal(12,2)")
            .alias("average_trip_duration_minutes"),
            F.sum("distance_km").cast("decimal(14,2)").alias("total_distance_km"),
        )
        .withColumnRenamed("start_district", "district")
    )


def _build_popular_routes(enriched: Any, route_limit: int) -> Any:
    from pyspark.sql import Window, functions as F

    route_counts = enriched.groupBy(
        "business_date",
        "start_district",
        "start_station_id",
        "start_station_name",
        "end_station_id",
        "end_station_name",
    ).agg(
        F.count(F.lit(1)).alias("route_trip_count"),
        F.sum("distance_km").cast("decimal(14,2)").alias("route_distance_km"),
    )
    rank_window = Window.partitionBy("business_date", "start_district").orderBy(
        F.desc("route_trip_count"),
        F.asc("start_station_id"),
        F.asc("end_station_id"),
    )
    return (
        route_counts.withColumn("route_rank", F.row_number().over(rank_window))
        .where(F.col("route_rank") <= route_limit)
        .withColumnRenamed("start_district", "district")
    )


def _build_cohort_retention(trips: Any) -> Any:
    from pyspark.sql import functions as F

    rider_activity = trips.select("rider_key", "business_date").distinct()
    rider_cohorts = rider_activity.groupBy("rider_key").agg(
        F.min("business_date").alias("cohort_date")
    )
    cohort_sizes = rider_cohorts.groupBy("cohort_date").agg(
        F.count(F.lit(1)).alias("cohort_size")
    )
    retained = (
        rider_activity.join(rider_cohorts, "rider_key", "inner")
        .groupBy("cohort_date", F.col("business_date").alias("activity_date"))
        .agg(F.countDistinct("rider_key").alias("retained_riders"))
        .withColumn(
            "days_since_cohort",
            F.datediff("activity_date", "cohort_date"),
        )
    )
    return (
        retained.join(cohort_sizes, "cohort_date", "inner")
        .withColumn(
            "retention_rate",
            (F.col("retained_riders") / F.col("cohort_size")).cast("decimal(8,4)"),
        )
        .select(
            "cohort_date",
            "activity_date",
            "days_since_cohort",
            "cohort_size",
            "retained_riders",
            "retention_rate",
        )
    )


def build_gold_analytics(
    spark: Any,
    *,
    silver_trip_path: Path,
    station_dimension_path: Path,
    daily_metrics_path: Path,
    popular_routes_path: Path,
    cohort_retention_path: Path,
    route_limit: int = 3,
) -> GoldBuildResult:
    """Rebuild Gold outputs after end-exclusive point-in-time dimension joins."""
    if route_limit <= 0:
        raise ValueError("route_limit must be greater than zero")
    silver_path = _require_dataset(silver_trip_path, "Silver trip")
    dimension_path = _require_dataset(station_dimension_path, "Station dimension")

    trips = spark.read.parquet(str(silver_path))
    station_dimension = spark.read.parquet(str(dimension_path)).select(
        "station_id",
        "station_name",
        "district",
        "valid_from",
        "valid_to",
    )
    input_trip_rows = trips.count()
    with_start = _join_station_version(trips, station_dimension, "start")
    cached_enriched = _join_station_version(with_start, station_dimension, "end").cache()
    try:
        enriched = _require_complete_station_joins(cached_enriched)
        enriched_trip_rows = enriched.count()
        daily_metrics = _build_daily_metrics(enriched)
        popular_routes = _build_popular_routes(enriched, route_limit)
        cohort_retention = _build_cohort_retention(trips)

        daily_metric_rows = daily_metrics.count()
        popular_route_rows = popular_routes.count()
        cohort_retention_rows = cohort_retention.count()
        _write_partitioned(daily_metrics, daily_metrics_path, daily_metric_rows, "business_date")
        _write_partitioned(
            popular_routes,
            popular_routes_path,
            popular_route_rows,
            "business_date",
        )
        _write_partitioned(
            cohort_retention,
            cohort_retention_path,
            cohort_retention_rows,
            "cohort_date",
        )
    finally:
        cached_enriched.unpersist()

    return GoldBuildResult(
        input_trip_rows=input_trip_rows,
        enriched_trip_rows=enriched_trip_rows,
        daily_metric_rows=daily_metric_rows,
        popular_route_rows=popular_route_rows,
        cohort_retention_rows=cohort_retention_rows,
        cohort_input_trip_rows=input_trip_rows,
    )


def build_gold_analytics_partition(
    spark: Any,
    *,
    target_date: date,
    silver_trip_path: Path,
    station_dimension_path: Path,
    daily_metrics_path: Path,
    popular_routes_path: Path,
    cohort_retention_path: Path,
    route_limit: int = 3,
) -> GoldBuildResult:
    """Replace one daily/route partition and fully rebuild dependency-wide cohorts."""
    from pyspark.sql import functions as F

    if route_limit <= 0:
        raise ValueError("route_limit must be greater than zero")
    silver_path = _require_dataset(silver_trip_path, "Silver trip")
    dimension_path = _require_dataset(station_dimension_path, "Station dimension")
    all_trips = spark.read.parquet(str(silver_path)).cache()
    try:
        cohort_input_trip_rows = all_trips.count()
        target = F.lit(target_date.isoformat()).cast("date")
        trips = all_trips.where(F.col("business_date") == target)
        station_dimension = spark.read.parquet(str(dimension_path)).select(
            "station_id",
            "station_name",
            "district",
            "valid_from",
            "valid_to",
        )
        input_trip_rows = trips.count()
        with_start = _join_station_version(trips, station_dimension, "start")
        cached_enriched = _join_station_version(with_start, station_dimension, "end").cache()
        try:
            enriched = _require_complete_station_joins(cached_enriched)
            enriched_trip_rows = enriched.count()
            daily_metrics = _build_daily_metrics(enriched)
            popular_routes = _build_popular_routes(enriched, route_limit)
            daily_metric_rows = daily_metrics.count()
            popular_route_rows = popular_routes.count()
            replace_date_partition(
                daily_metrics,
                daily_metrics_path,
                "business_date",
                target_date,
            )
            replace_date_partition(
                popular_routes,
                popular_routes_path,
                "business_date",
                target_date,
            )
        finally:
            cached_enriched.unpersist()

        cohort_retention = _build_cohort_retention(all_trips)
        cohort_retention_rows = cohort_retention.count()
        _write_partitioned(
            cohort_retention,
            cohort_retention_path,
            cohort_retention_rows,
            "cohort_date",
        )
    finally:
        all_trips.unpersist()

    return GoldBuildResult(
        input_trip_rows=input_trip_rows,
        enriched_trip_rows=enriched_trip_rows,
        daily_metric_rows=daily_metric_rows,
        popular_route_rows=popular_route_rows,
        cohort_retention_rows=cohort_retention_rows,
        cohort_input_trip_rows=cohort_input_trip_rows,
    )
