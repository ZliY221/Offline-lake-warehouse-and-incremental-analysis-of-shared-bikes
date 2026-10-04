from __future__ import annotations

from datetime import UTC, date, datetime
import json
from pathlib import Path
import tempfile
import unittest

from pyspark.sql import functions as F

from bike_lakehouse.backfill import run_date_backfill
from bike_lakehouse.bronze import ingest_trip_bronze
from bike_lakehouse.generator import generate_station_snapshot, generate_trips
from bike_lakehouse.quality_report import build_quality_report
from bike_lakehouse.spark import create_local_spark
from bike_lakehouse.station_dimension import build_station_dimension


def write_rows(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text(
        "\n".join(json.dumps(row, ensure_ascii=False, sort_keys=True) for row in rows) + "\n",
        encoding="utf-8",
    )


class DateBackfillTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark_temp = tempfile.TemporaryDirectory()
        cls.spark = create_local_spark(
            "bike-trip-backfill-tests",
            Path(cls.spark_temp.name) / "warehouse",
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()
        cls.spark_temp.cleanup()

    def test_one_partition_is_replaced_and_dependents_are_rebuilt(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            day_one = root / "day-one.ndjson"
            day_two = root / "day-two.ndjson"
            stations = root / "stations.ndjson"
            write_rows(day_one, generate_trips(3, seed=11))
            write_rows(
                day_two,
                generate_trips(
                    4,
                    seed=12,
                    start_time=datetime(2026, 10, 2, 6, 0, tzinfo=UTC),
                ),
            )
            write_rows(stations, generate_station_snapshot(date(2026, 10, 1)))
            bronze = root / "bronze"
            bronze_manifest = root / "bronze-manifest"
            dimension = root / "dimension"
            station_rejected = root / "station-rejected"
            build_station_dimension(
                self.spark,
                input_paths=[stations],
                dimension_path=dimension,
                rejected_path=station_rejected,
            )
            ingest_trip_bronze(
                self.spark,
                input_path=day_two,
                output_path=bronze,
                ingestion_date=date(2026, 10, 2),
                manifest_path=bronze_manifest,
            )

            result = run_date_backfill(
                self.spark,
                input_path=day_one,
                target_date=date(2026, 10, 1),
                bronze_path=bronze,
                bronze_manifest_path=bronze_manifest,
                silver_valid_path=root / "silver-valid",
                silver_rejected_path=root / "silver-rejected",
                silver_duplicate_path=root / "silver-duplicates",
                station_dimension_path=dimension,
                gold_daily_metrics_path=root / "gold-daily",
                gold_popular_routes_path=root / "gold-routes",
                gold_cohort_retention_path=root / "gold-retention",
                backfill_manifest_path=root / "backfill-manifest",
            )

            self.assertEqual(result.bronze_partition_rows, 3)
            self.assertEqual(result.silver_valid_rows, 7)
            counts = {
                row["day"]: row["count"]
                for row in self.spark.read.parquet(str(bronze))
                .select(F.date_format("ingestion_date", "yyyy-MM-dd").alias("day"))
                .groupBy("day")
                .count()
                .collect()
            }
            self.assertEqual(counts, {"2026-10-01": 3, "2026-10-02": 4})
            report_path = next((root / "backfill-manifest").glob("*.json"))
            report = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual(report["status"], "SUCCEEDED")
            self.assertEqual(report["affected_bronze_partitions"], ["2026-10-01"])
            self.assertEqual(report["silver_rebuild_scope"], "FULL_DATASET")
            self.assertEqual(report["gold_result"]["input_trip_rows"], 7)
            quality = build_quality_report(
                self.spark,
                bronze_path=bronze,
                silver_valid_path=root / "silver-valid",
                silver_rejected_path=root / "silver-rejected",
                silver_duplicate_path=root / "silver-duplicates",
                station_dimension_path=dimension,
                station_rejected_path=station_rejected,
                gold_daily_metrics_path=root / "gold-daily",
                gold_popular_routes_path=root / "gold-routes",
                gold_cohort_retention_path=root / "gold-retention",
                bronze_manifest_path=bronze_manifest,
                backfill_manifest_path=root / "backfill-manifest",
                output_path=root / "quality-report.json",
            )
            self.assertEqual(quality["overall_status"], "PASS")
            self.assertTrue(all(check["status"] == "PASS" for check in quality["checks"]))
            self.assertEqual(quality["dataset_counts"]["bronze_rows"], 7)
            self.assertEqual(quality["dataset_counts"]["silver_valid_rows"], 7)

    def test_source_date_mismatch_is_rejected_and_recorded(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "wrong-date.ndjson"
            write_rows(
                source,
                generate_trips(
                    1,
                    start_time=datetime(2026, 10, 2, 6, 0, tzinfo=UTC),
                ),
            )
            with self.assertRaisesRegex(ValueError, "outside 2026-10-01"):
                run_date_backfill(
                    self.spark,
                    input_path=source,
                    target_date=date(2026, 10, 1),
                    bronze_path=root / "bronze",
                    bronze_manifest_path=root / "bronze-manifest",
                    silver_valid_path=root / "silver-valid",
                    silver_rejected_path=root / "silver-rejected",
                    silver_duplicate_path=root / "silver-duplicates",
                    station_dimension_path=root / "dimension",
                    gold_daily_metrics_path=root / "gold-daily",
                    gold_popular_routes_path=root / "gold-routes",
                    gold_cohort_retention_path=root / "gold-retention",
                    backfill_manifest_path=root / "backfill-manifest",
                )
            report_path = next((root / "backfill-manifest").glob("*.json"))
            report = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual(report["status"], "FAILED")
            self.assertEqual(report["error_type"], "ValueError")


if __name__ == "__main__":
    unittest.main()
