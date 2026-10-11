from __future__ import annotations

from datetime import UTC, date, datetime
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from pyspark.sql import functions as F

from bike_lakehouse.backfill import run_date_backfill
from bike_lakehouse.bronze import ingest_trip_bronze
from bike_lakehouse.generator import generate_station_snapshot, generate_trips
from bike_lakehouse.gold import build_gold_analytics
from bike_lakehouse.quality_report import build_quality_report
from bike_lakehouse.silver import build_trip_silver, build_trip_silver_partition
from bike_lakehouse.spark import create_local_spark
from bike_lakehouse.station_dimension import build_station_dimension


def write_rows(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text(
        "\n".join(json.dumps(row, ensure_ascii=False, sort_keys=True) for row in rows) + "\n",
        encoding="utf-8",
    )


def partition_fingerprints(path: Path, partition: str) -> list[tuple[str, str]]:
    partition_path = path / partition
    return [
        (
            file.name,
            hashlib.sha256(file.read_bytes()).hexdigest(),
        )
        for file in sorted(partition_path.glob("*.parquet"))
    ]


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

    def test_target_partitions_are_replaced_while_siblings_stay_byte_identical(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            original_day_one = root / "original-day-one.ndjson"
            day_one = root / "day-one.ndjson"
            day_two = root / "day-two.ndjson"
            stations = root / "stations.ndjson"
            original_day_one_rows = generate_trips(2, seed=10)
            write_rows(original_day_one, original_day_one_rows)
            day_two_rows = generate_trips(
                4,
                seed=12,
                start_time=datetime(2026, 10, 2, 6, 0, tzinfo=UTC),
            )
            replacement_rows = generate_trips(3, seed=11)
            replacement_rows[0]["rider_key"] = day_two_rows[0]["rider_key"]
            write_rows(day_one, replacement_rows)
            write_rows(day_two, day_two_rows)
            expected_affected_rider_count = len(
                {
                    row["rider_key"]
                    for row in original_day_one_rows + replacement_rows
                }
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
                input_path=original_day_one,
                output_path=bronze,
                ingestion_date=date(2026, 10, 1),
                manifest_path=bronze_manifest,
            )
            ingest_trip_bronze(
                self.spark,
                input_path=day_two,
                output_path=bronze,
                ingestion_date=date(2026, 10, 2),
                manifest_path=bronze_manifest,
            )
            silver_paths = {
                "valid_path": root / "silver-valid",
                "rejected_path": root / "silver-rejected",
                "duplicate_path": root / "silver-duplicates",
            }
            gold_paths = {
                "daily_metrics_path": root / "gold-daily",
                "popular_routes_path": root / "gold-routes",
                "cohort_retention_path": root / "gold-retention",
            }
            build_trip_silver(self.spark, bronze_path=bronze, **silver_paths)
            build_gold_analytics(
                self.spark,
                silver_trip_path=silver_paths["valid_path"],
                station_dimension_path=dimension,
                **gold_paths,
            )
            unchanged_before = {
                "silver": partition_fingerprints(
                    silver_paths["valid_path"], "ingestion_date=2026-10-02"
                ),
                "daily": partition_fingerprints(
                    gold_paths["daily_metrics_path"], "business_date=2026-10-02"
                ),
                "routes": partition_fingerprints(
                    gold_paths["popular_routes_path"], "business_date=2026-10-02"
                ),
            }
            self.assertTrue(all(unchanged_before.values()))
            affected_cohort_before = partition_fingerprints(
                gold_paths["cohort_retention_path"], "cohort_date=2026-10-02"
            )
            self.assertTrue(affected_cohort_before)

            result = run_date_backfill(
                self.spark,
                input_path=day_one,
                target_date=date(2026, 10, 1),
                bronze_path=bronze,
                bronze_manifest_path=bronze_manifest,
                silver_valid_path=silver_paths["valid_path"],
                silver_rejected_path=silver_paths["rejected_path"],
                silver_duplicate_path=silver_paths["duplicate_path"],
                station_dimension_path=dimension,
                gold_daily_metrics_path=gold_paths["daily_metrics_path"],
                gold_popular_routes_path=gold_paths["popular_routes_path"],
                gold_cohort_retention_path=gold_paths["cohort_retention_path"],
                backfill_manifest_path=root / "backfill-manifest",
            )

            self.assertEqual(result.bronze_partition_rows, 3)
            self.assertEqual(result.silver_valid_rows, 3)
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
            self.assertEqual(report["affected_silver_partitions"], ["2026-10-01"])
            self.assertEqual(report["silver_rebuild_scope"], "TARGET_INGESTION_DATE")
            self.assertEqual(
                report["gold_daily_metrics_rebuild_scope"], "TARGET_BUSINESS_DATE"
            )
            self.assertEqual(
                report["gold_cohort_retention_rebuild_scope"], "AFFECTED_COHORT_DATES"
            )
            self.assertEqual(
                report["gold_cohort_retention_compute_scope"], "FULL_DATASET_SCAN"
            )
            self.assertEqual(report["affected_rider_count"], expected_affected_rider_count)
            self.assertEqual(
                report["affected_gold_cohort_retention_partitions"],
                ["2026-10-01", "2026-10-02"],
            )
            self.assertEqual(report["cross_partition_trip_id_policy"], "REJECT_BEFORE_WRITE")
            self.assertEqual(report["gold_result"]["input_trip_rows"], 3)
            self.assertEqual(report["gold_result"]["cohort_input_trip_rows"], 7)
            affected_cohort_rows = (
                self.spark.read.parquet(str(gold_paths["cohort_retention_path"]))
                .where("cohort_date IN (DATE '2026-10-01', DATE '2026-10-02')")
                .count()
            )
            self.assertEqual(
                report["gold_result"]["cohort_retention_rows"],
                affected_cohort_rows,
            )
            unchanged_after = {
                "silver": partition_fingerprints(
                    silver_paths["valid_path"], "ingestion_date=2026-10-02"
                ),
                "daily": partition_fingerprints(
                    gold_paths["daily_metrics_path"], "business_date=2026-10-02"
                ),
                "routes": partition_fingerprints(
                    gold_paths["popular_routes_path"], "business_date=2026-10-02"
                ),
            }
            self.assertEqual(unchanged_after, unchanged_before)
            affected_cohort_after = partition_fingerprints(
                gold_paths["cohort_retention_path"], "cohort_date=2026-10-02"
            )
            self.assertNotEqual(affected_cohort_after, affected_cohort_before)
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
            checks = {check["name"]: check for check in quality["checks"]}
            self.assertEqual(8, len(checks))
            self.assertEqual(0.0, checks["silver_rejected_ratio"]["observed_ratio"])
            self.assertEqual(0.05, checks["silver_rejected_ratio"]["maximum_ratio"])
            self.assertEqual(0.0, checks["silver_duplicate_ratio"]["observed_ratio"])
            self.assertEqual(quality["dataset_counts"]["bronze_rows"], 7)
            self.assertEqual(quality["dataset_counts"]["silver_valid_rows"], 7)

    def test_cross_partition_trip_id_collision_fails_before_bronze_write(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            old_day_one = root / "old-day-one.ndjson"
            day_two = root / "day-two.ndjson"
            replacement = root / "replacement.ndjson"
            old_rows = generate_trips(1, seed=21)
            day_two_rows = generate_trips(
                1,
                seed=22,
                start_time=datetime(2026, 10, 2, 6, 0, tzinfo=UTC),
            )
            replacement_rows = generate_trips(1, seed=23)
            replacement_rows[0]["trip_id"] = day_two_rows[0]["trip_id"]
            write_rows(old_day_one, old_rows)
            write_rows(day_two, day_two_rows)
            write_rows(replacement, replacement_rows)
            bronze = root / "bronze"
            for input_path, ingestion_date in (
                (old_day_one, date(2026, 10, 1)),
                (day_two, date(2026, 10, 2)),
            ):
                ingest_trip_bronze(
                    self.spark,
                    input_path=input_path,
                    output_path=bronze,
                    ingestion_date=ingestion_date,
                )
            before = [
                row["trip_id"]
                for row in self.spark.read.parquet(str(bronze))
                .where("ingestion_date = DATE '2026-10-01'")
                .select("trip_id")
                .collect()
            ]

            with self.assertRaisesRegex(ValueError, "another ingestion_date partition"):
                run_date_backfill(
                    self.spark,
                    input_path=replacement,
                    target_date=date(2026, 10, 1),
                    bronze_path=bronze,
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
            after = [
                row["trip_id"]
                for row in self.spark.read.parquet(str(bronze))
                .where("ingestion_date = DATE '2026-10-01'")
                .select("trip_id")
                .collect()
            ]
            self.assertEqual(after, before)
            report_path = next((root / "backfill-manifest").glob("*.json"))
            report = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual(report["status"], "FAILED")
            self.assertEqual(report["error_type"], "ValueError")

            ingest_trip_bronze(
                self.spark,
                input_path=replacement,
                output_path=bronze,
                ingestion_date=date(2026, 10, 1),
            )
            with self.assertRaisesRegex(ValueError, "Bronze dataset violates this invariant"):
                build_trip_silver_partition(
                    self.spark,
                    bronze_path=bronze,
                    target_date=date(2026, 10, 1),
                    valid_path=root / "unsafe-silver-valid",
                    rejected_path=root / "unsafe-silver-rejected",
                    duplicate_path=root / "unsafe-silver-duplicates",
                )

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
