from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path
import tempfile
import unittest

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    StringType,
    StructField,
    StructType,
)

from bike_lakehouse.gold import build_gold_analytics
from bike_lakehouse.spark import create_local_spark


TRIP_SCHEMA = StructType(
    [
        StructField("trip_id", StringType(), False),
        StructField("rider_key", StringType(), False),
        StructField("start_station_id", StringType(), False),
        StructField("end_station_id", StringType(), False),
        StructField("rider_type", StringType(), False),
        StructField("bike_type", StringType(), False),
        StructField("distance_km", DecimalType(8, 2), False),
        StructField("business_date", DateType(), False),
        StructField("trip_duration_minutes", DecimalType(10, 2), False),
    ]
)

DIMENSION_SCHEMA = StructType(
    [
        StructField("station_id", StringType(), False),
        StructField("station_name", StringType(), False),
        StructField("district", StringType(), False),
        StructField("valid_from", DateType(), False),
        StructField("valid_to", DateType(), True),
        StructField("is_current", BooleanType(), False),
    ]
)


def trip(
    trip_id: str,
    rider_key: str,
    business_date: date,
    start: str,
    end: str,
    rider: str,
    bike: str,
    duration: str,
    distance: str,
) -> tuple[object, ...]:
    return (
        trip_id,
        rider_key,
        start,
        end,
        rider,
        bike,
        Decimal(distance),
        business_date,
        Decimal(duration),
    )


class GoldBuildTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark_temp = tempfile.TemporaryDirectory()
        cls.spark = create_local_spark(
            "bike-trip-gold-tests",
            Path(cls.spark_temp.name) / "warehouse",
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()
        cls.spark_temp.cleanup()

    def _write_dimensions(self, path: Path) -> None:
        rows = [
            ("ST-001", "一号站旧名", "甲区", date(2026, 10, 1), date(2026, 10, 2), False),
            ("ST-001", "一号站新名", "乙区", date(2026, 10, 2), None, True),
            ("ST-002", "二号站", "乙区", date(2026, 10, 1), None, True),
            ("ST-003", "三号站", "甲区", date(2026, 10, 1), None, True),
        ]
        self.spark.createDataFrame(rows, DIMENSION_SCHEMA).write.mode("overwrite").parquet(
            str(path)
        )

    def test_metrics_and_routes_use_point_in_time_station_attributes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            silver = root / "silver"
            dimension = root / "dimension"
            daily = root / "daily"
            routes = root / "routes"
            rows = [
                trip(
                    "t1", "rider_0000000000000001", date(2026, 10, 1), "ST-001", "ST-002", "member", "classic", "10", "2"
                ),
                trip(
                    "t2", "rider_0000000000000002", date(2026, 10, 1), "ST-001", "ST-002", "member", "classic", "20", "4"
                ),
                trip(
                    "t3", "rider_0000000000000003", date(2026, 10, 1), "ST-001", "ST-003", "casual", "electric", "30", "6"
                ),
                trip(
                    "t4", "rider_0000000000000001", date(2026, 10, 2), "ST-001", "ST-002", "member", "classic", "40", "8"
                ),
                trip(
                    "t5", "rider_0000000000000004", date(2026, 10, 2), "ST-002", "ST-001", "member", "classic", "50", "10"
                ),
            ]
            self.spark.createDataFrame(rows, TRIP_SCHEMA).write.mode("overwrite").parquet(
                str(silver)
            )
            self._write_dimensions(dimension)

            result = build_gold_analytics(
                self.spark,
                silver_trip_path=silver,
                station_dimension_path=dimension,
                daily_metrics_path=daily,
                popular_routes_path=routes,
                cohort_retention_path=root / "retention",
                route_limit=2,
            )

            self.assertEqual(result.input_trip_rows, 5)
            self.assertEqual(result.enriched_trip_rows, 5)
            self.assertEqual(result.daily_metric_rows, 3)
            self.assertEqual(result.popular_route_rows, 4)
            self.assertEqual(result.cohort_retention_rows, 3)
            daily_frame = self.spark.read.parquet(str(daily))
            day_one_member = daily_frame.where(
                "business_date = DATE '2026-10-01' AND district = '甲区' "
                "AND rider_type = 'member' AND bike_type = 'classic'"
            ).first()
            self.assertEqual(day_one_member["trip_count"], 2)
            self.assertEqual(
                day_one_member["total_trip_duration_minutes"], Decimal("30.00")
            )
            self.assertEqual(
                day_one_member["average_trip_duration_minutes"], Decimal("15.00")
            )
            self.assertEqual(day_one_member["total_distance_km"], Decimal("6.00"))
            day_two = daily_frame.where("business_date = DATE '2026-10-02'").first()
            self.assertEqual(day_two["district"], "乙区")
            self.assertEqual(day_two["trip_count"], 2)
            self.assertEqual(day_two["total_trip_duration_minutes"], Decimal("90.00"))
            route_rows = (
                self.spark.read.parquet(str(routes))
                .where("business_date = DATE '2026-10-01' AND district = '甲区'")
                .orderBy("route_rank")
                .collect()
            )
            self.assertEqual(
                [
                    (row["end_station_id"], row["route_trip_count"], row["route_rank"])
                    for row in route_rows
                ],
                [("ST-002", 2, 1), ("ST-003", 1, 2)],
            )
            self.assertEqual(route_rows[0]["start_station_name"], "一号站旧名")
            retention = (
                self.spark.read.parquet(str(root / "retention"))
                .orderBy("cohort_date", "days_since_cohort")
                .collect()
            )
            self.assertEqual(
                [
                    (
                        row["cohort_date"],
                        row["days_since_cohort"],
                        row["cohort_size"],
                        row["retained_riders"],
                        row["retention_rate"],
                    )
                    for row in retention
                ],
                [
                    (date(2026, 10, 1), 0, 3, 3, Decimal("1.0000")),
                    (date(2026, 10, 1), 1, 3, 1, Decimal("0.3333")),
                    (date(2026, 10, 2), 0, 1, 1, Decimal("1.0000")),
                ],
            )

    def test_missing_point_in_time_dimension_fails_instead_of_dropping_trips(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            silver = root / "silver"
            dimension = root / "dimension"
            self.spark.createDataFrame(
                [
                    trip(
                        "missing",
                        "rider_0000000000000001",
                        date(2026, 9, 30),
                        "ST-001",
                        "ST-002",
                        "member",
                        "classic",
                        "10",
                        "2",
                    )
                ],
                TRIP_SCHEMA,
            ).write.mode("overwrite").parquet(str(silver))
            self._write_dimensions(dimension)

            with self.assertRaisesRegex(ValueError, "Point-in-time station lookup failed"):
                build_gold_analytics(
                    self.spark,
                    silver_trip_path=silver,
                    station_dimension_path=dimension,
                    daily_metrics_path=root / "daily",
                    popular_routes_path=root / "routes",
                    cohort_retention_path=root / "retention",
                )


if __name__ == "__main__":
    unittest.main()
