"""Explicit Spark schemas for source and Bronze datasets."""

from __future__ import annotations


def trip_source_schema():
    from pyspark.sql.types import (
        DecimalType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            StructField("trip_id", StringType(), True),
            StructField("rider_key", StringType(), True),
            StructField("started_at", TimestampType(), True),
            StructField("ended_at", TimestampType(), True),
            StructField("start_station_id", StringType(), True),
            StructField("end_station_id", StringType(), True),
            StructField("rider_type", StringType(), True),
            StructField("bike_type", StringType(), True),
            StructField("distance_km", DecimalType(8, 2), True),
            StructField("_corrupt_record", StringType(), True),
        ]
    )


def station_source_schema():
    from pyspark.sql.types import (
        BooleanType,
        DateType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    return StructType(
        [
            StructField("snapshot_date", DateType(), True),
            StructField("station_id", StringType(), True),
            StructField("station_name", StringType(), True),
            StructField("district", StringType(), True),
            StructField("capacity", IntegerType(), True),
            StructField("active", BooleanType(), True),
            StructField("_corrupt_record", StringType(), True),
        ]
    )
