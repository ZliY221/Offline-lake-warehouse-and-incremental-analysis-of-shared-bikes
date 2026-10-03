from __future__ import annotations

from datetime import date
import json
from pathlib import Path
import tempfile
import unittest

from pyspark.sql import functions as F

from bike_lakehouse.bronze import ingest_trip_bronze
from bike_lakehouse.generator import generate_trips
from bike_lakehouse.silver import build_trip_silver
from bike_lakehouse.spark import create_local_spark


def write_mixed_source(path: Path) -> None:
    rows = generate_trips(9)
    valid = rows[0]
    exact_duplicate = dict(valid)
    conflict_one = dict(rows[1])
    conflict_two = dict(conflict_one)
    conflict_two["distance_km"] = "9.99"
    invalid_time = dict(rows[2])
    invalid_time["ended_at"] = invalid_time["started_at"]
    invalid_station = dict(rows[3])
    invalid_station["start_station_id"] = "ST-999"
    same_station = dict(rows[4])
    same_station["end_station_id"] = same_station["start_station_id"]
    invalid_rider = dict(rows[5])
    invalid_rider["rider_type"] = "unknown"
    invalid_bike = dict(rows[6])
    invalid_bike["bike_type"] = "scooter"
    invalid_distance = dict(rows[7])
    invalid_distance["distance_km"] = "0.00"
    missing_id = dict(rows[8])
    missing_id["trip_id"] = None
    mixed = [
        valid,
        exact_duplicate,
        conflict_one,
        conflict_two,
        invalid_time,
        invalid_station,
        same_station,
        invalid_rider,
        invalid_bike,
        invalid_distance,
        missing_id,
    ]
    serialized = [json.dumps(row, ensure_ascii=False, sort_keys=True) for row in mixed]
    serialized.append("{not-valid-json}")
    path.write_text("\n".join(serialized) + "\n", encoding="utf-8")


class SilverBuildTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark_temp = tempfile.TemporaryDirectory()
        cls.spark = create_local_spark(
            "bike-trip-silver-tests",
            Path(cls.spark_temp.name) / "warehouse",
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()
        cls.spark_temp.cleanup()

    def test_validation_deduplication_and_derived_fields(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "mixed.ndjson"
            bronze = root / "bronze"
            valid = root / "valid"
            rejected = root / "rejected"
            duplicates = root / "duplicates"
            write_mixed_source(source)
            ingest_trip_bronze(
                self.spark,
                input_path=source,
                output_path=bronze,
                ingestion_date=date(2026, 10, 1),
            )

            result = build_trip_silver(
                self.spark,
                bronze_path=bronze,
                valid_path=valid,
                rejected_path=rejected,
                duplicate_path=duplicates,
            )

            self.assertEqual(result.input_rows, 12)
            self.assertEqual(result.valid_rows, 1)
            self.assertEqual(result.rejected_rows, 8)
            self.assertEqual(result.duplicate_rows, 3)
            self.assertEqual(result.exact_duplicate_rows, 1)
            self.assertEqual(result.conflicting_duplicate_rows, 2)

            valid_frame = self.spark.read.parquet(str(valid))
            self.assertEqual(valid_frame.schema["business_date"].dataType.simpleString(), "date")
            self.assertEqual(
                valid_frame.schema["trip_duration_minutes"].dataType.simpleString(),
                "decimal(10,2)",
            )
            self.assertGreater(valid_frame.first()["trip_duration_minutes"], 0)

            rejected_frame = self.spark.read.parquet(str(rejected))
            error_codes = {
                error
                for row in rejected_frame.select("validation_errors").collect()
                for error in row["validation_errors"]
            }
            self.assertTrue(
                {
                    "MALFORMED_RECORD",
                    "MISSING_REQUIRED_FIELD",
                    "INVALID_TIME_ORDER",
                    "INVALID_START_STATION",
                    "SAME_START_END_STATION",
                    "INVALID_RIDER_TYPE",
                    "INVALID_BIKE_TYPE",
                    "INVALID_DISTANCE",
                }.issubset(error_codes)
            )
            self.assertNotIn("_corrupt_record", rejected_frame.columns)
            corrupt = rejected_frame.where(F.col("corrupt_payload_bytes").isNotNull()).first()
            self.assertEqual(len(corrupt["source_record_sha256"]), 64)
            self.assertGreater(corrupt["corrupt_payload_bytes"], 0)

            duplicate_kinds = {
                row["duplicate_kind"]: row["count"]
                for row in self.spark.read.parquet(str(duplicates))
                .groupBy("duplicate_kind")
                .count()
                .collect()
            }
            self.assertEqual(
                duplicate_kinds,
                {"exact_duplicate": 1, "conflicting_duplicate": 2},
            )

    def test_full_rebuild_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "mixed.ndjson"
            bronze = root / "bronze"
            outputs = {
                "valid_path": root / "valid",
                "rejected_path": root / "rejected",
                "duplicate_path": root / "duplicates",
            }
            write_mixed_source(source)
            ingest_trip_bronze(
                self.spark,
                input_path=source,
                output_path=bronze,
                ingestion_date=date(2026, 10, 1),
            )
            first = build_trip_silver(self.spark, bronze_path=bronze, **outputs)
            second = build_trip_silver(self.spark, bronze_path=bronze, **outputs)
            self.assertEqual(first, second)
            self.assertEqual(self.spark.read.parquet(str(outputs["valid_path"])).count(), 1)
            self.assertEqual(self.spark.read.parquet(str(outputs["rejected_path"])).count(), 8)
            self.assertEqual(self.spark.read.parquet(str(outputs["duplicate_path"])).count(), 3)

    def test_empty_quality_outputs_keep_readable_schemas(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "valid.ndjson"
            bronze = root / "bronze"
            valid = root / "valid"
            rejected = root / "rejected"
            duplicates = root / "duplicates"
            source.write_text(
                json.dumps(generate_trips(1)[0], ensure_ascii=False, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            ingest_trip_bronze(
                self.spark,
                input_path=source,
                output_path=bronze,
                ingestion_date=date(2026, 10, 1),
            )
            result = build_trip_silver(
                self.spark,
                bronze_path=bronze,
                valid_path=valid,
                rejected_path=rejected,
                duplicate_path=duplicates,
            )
            self.assertEqual(result.valid_rows, 1)
            self.assertEqual(result.rejected_rows, 0)
            self.assertEqual(result.duplicate_rows, 0)
            self.assertEqual(self.spark.read.parquet(str(rejected)).count(), 0)
            self.assertIn("validation_errors", self.spark.read.parquet(str(rejected)).columns)
            self.assertEqual(self.spark.read.parquet(str(duplicates)).count(), 0)
            self.assertIn("duplicate_kind", self.spark.read.parquet(str(duplicates)).columns)


if __name__ == "__main__":
    unittest.main()
