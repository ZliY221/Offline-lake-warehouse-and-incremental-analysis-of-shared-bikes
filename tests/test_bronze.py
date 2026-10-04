from __future__ import annotations

from datetime import UTC, date, datetime
import json
from pathlib import Path
import tempfile
import unittest

from pyspark.sql import functions as F

from bike_lakehouse.bronze import ingest_trip_bronze
from bike_lakehouse.generator import generate_trips
from bike_lakehouse.spark import create_local_spark


def write_events(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text(
        "\n".join(json.dumps(row, ensure_ascii=False, sort_keys=True) for row in rows) + "\n",
        encoding="utf-8",
    )


class BronzeIngestionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark_temp = tempfile.TemporaryDirectory()
        cls.spark = create_local_spark(
            "bike-trip-bronze-tests",
            Path(cls.spark_temp.name) / "warehouse",
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()
        cls.spark_temp.cleanup()

    def test_explicit_schema_and_decimal_are_preserved_in_parquet(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "trips.ndjson"
            output = root / "bronze"
            manifest = root / "manifest"
            write_events(source, generate_trips(5))

            result = ingest_trip_bronze(
                self.spark,
                input_path=source,
                output_path=output,
                ingestion_date=date(2026, 10, 1),
                manifest_path=manifest,
            )

            self.assertEqual(result.input_rows, 5)
            self.assertEqual(result.output_rows_in_partition, 5)
            self.assertEqual(result.corrupt_rows, 0)
            self.assertEqual(len(result.source_sha256), 64)
            manifest_files = list(manifest.glob("*.json"))
            self.assertEqual(len(manifest_files), 1)
            manifest_record = json.loads(manifest_files[0].read_text(encoding="utf-8"))
            self.assertEqual(manifest_record["batch_id"], result.batch_id)
            self.assertEqual(manifest_record["status"], "SUCCEEDED")
            self.assertEqual(manifest_record["attempt_count"], 1)
            self.assertEqual(manifest_record["input_rows"], 5)
            self.assertEqual(manifest_record["output_rows_in_partition"], 5)
            self.assertEqual(manifest_record["source_sha256"], result.source_sha256)
            dataframe = self.spark.read.parquet(str(output))
            self.assertEqual(dataframe.schema["distance_km"].dataType.simpleString(), "decimal(8,2)")
            self.assertEqual(dataframe.schema["started_at"].dataType.simpleString(), "timestamp")
            self.assertEqual(
                dataframe.select(F.date_format("ingestion_date", "yyyy-MM-dd")).first()[0],
                "2026-10-01",
            )

    def test_dynamic_overwrite_is_idempotent_and_preserves_other_dates(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "bronze"
            manifest = root / "manifest"
            day_one = root / "day-one.ndjson"
            day_two = root / "day-two.ndjson"
            write_events(day_one, generate_trips(5, seed=2027))
            write_events(
                day_two,
                generate_trips(
                    4,
                    seed=2028,
                    start_time=datetime(2026, 10, 2, 6, 0, tzinfo=UTC),
                ),
            )
            ingest_trip_bronze(
                self.spark,
                input_path=day_one,
                output_path=output,
                ingestion_date=date(2026, 10, 1),
                manifest_path=manifest,
            )
            ingest_trip_bronze(
                self.spark,
                input_path=day_two,
                output_path=output,
                ingestion_date=date(2026, 10, 2),
                manifest_path=manifest,
            )
            ingest_trip_bronze(
                self.spark,
                input_path=day_one,
                output_path=output,
                ingestion_date=date(2026, 10, 1),
                manifest_path=manifest,
            )

            counts = {
                row["day"]: row["count"]
                for row in self.spark.read.parquet(str(output))
                .select(F.date_format("ingestion_date", "yyyy-MM-dd").alias("day"))
                .groupBy("day")
                .count()
                .collect()
            }
            self.assertEqual(counts, {"2026-10-01": 5, "2026-10-02": 4})
            manifest_records = [
                json.loads(path.read_text(encoding="utf-8"))
                for path in sorted(manifest.glob("*.json"))
            ]
            self.assertEqual(len(manifest_records), 2)
            attempts_by_date = {
                record["ingestion_date"]: record["attempt_count"]
                for record in manifest_records
            }
            self.assertEqual(attempts_by_date, {"2026-10-01": 2, "2026-10-02": 1})


if __name__ == "__main__":
    unittest.main()
