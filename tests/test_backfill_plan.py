from __future__ import annotations

from datetime import UTC, date, datetime
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from bike_lakehouse.backfill_plan import plan_date_backfill
from bike_lakehouse.bronze import ingest_trip_bronze
from bike_lakehouse.generator import generate_trips
from bike_lakehouse.silver import build_trip_silver
from bike_lakehouse.spark import create_local_spark


def write_rows(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text(
        "\n".join(json.dumps(row, sort_keys=True) for row in rows) + "\n",
        encoding="utf-8",
    )


def tree_fingerprint(root: Path) -> dict[str, str]:
    return {
        str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


class BackfillImpactPlanTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark_temp = tempfile.TemporaryDirectory()
        cls.spark = create_local_spark(
            "bike-trip-backfill-plan-tests",
            Path(cls.spark_temp.name) / "warehouse",
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()
        cls.spark_temp.cleanup()

    def test_plan_reports_conservative_scope_without_writing_data(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            day_one = root / "day-one.ndjson"
            day_two = root / "day-two.ndjson"
            replacement = root / "replacement.ndjson"
            day_one_rows = generate_trips(3, seed=201)
            day_two_rows = generate_trips(
                2,
                seed=202,
                start_time=datetime(2026, 10, 2, 6, 0, tzinfo=UTC),
            )
            replacement_rows = generate_trips(2, seed=203)
            replacement_rows[0]["rider_key"] = day_two_rows[0]["rider_key"]
            write_rows(day_one, day_one_rows)
            write_rows(day_two, day_two_rows)
            write_rows(replacement, replacement_rows)

            bronze = root / "bronze"
            manifest = root / "bronze-manifest"
            ingest_trip_bronze(
                self.spark,
                input_path=day_one,
                output_path=bronze,
                ingestion_date=date(2026, 10, 1),
                manifest_path=manifest,
            )
            ingest_trip_bronze(
                self.spark,
                input_path=day_two,
                output_path=bronze,
                ingestion_date=date(2026, 10, 2),
                manifest_path=manifest,
            )
            silver = root / "silver-valid"
            build_trip_silver(
                self.spark,
                bronze_path=bronze,
                valid_path=silver,
                rejected_path=root / "silver-rejected",
                duplicate_path=root / "silver-duplicates",
            )
            before = tree_fingerprint(root)

            plan = plan_date_backfill(
                self.spark,
                input_path=replacement,
                target_date=date(2026, 10, 1),
                bronze_path=bronze,
                silver_valid_path=silver,
            )

            self.assertTrue(plan.read_only)
            self.assertEqual(plan.source_rows, 2)
            self.assertEqual(plan.source_distinct_trip_ids, 2)
            self.assertEqual(plan.current_bronze_rows, 3)
            self.assertEqual(plan.current_silver_rows, 3)
            self.assertEqual(plan.affected_bronze_partitions, ("2026-10-01",))
            self.assertIn("2026-10-01", plan.potential_gold_cohort_partitions)
            self.assertIn("2026-10-02", plan.potential_gold_cohort_partitions)
            self.assertGreater(plan.bronze_files_scanned, 0)
            self.assertGreater(plan.silver_files_scanned, 0)
            self.assertEqual(tree_fingerprint(root), before)

    def test_plan_rejects_cross_partition_collision(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            day_two = root / "day-two.ndjson"
            replacement = root / "replacement.ndjson"
            day_two_rows = generate_trips(
                1,
                seed=204,
                start_time=datetime(2026, 10, 2, 6, 0, tzinfo=UTC),
            )
            replacement_rows = generate_trips(1, seed=205)
            replacement_rows[0]["trip_id"] = day_two_rows[0]["trip_id"]
            write_rows(day_two, day_two_rows)
            write_rows(replacement, replacement_rows)
            bronze = root / "bronze"
            ingest_trip_bronze(
                self.spark,
                input_path=day_two,
                output_path=bronze,
                ingestion_date=date(2026, 10, 2),
            )

            with self.assertRaisesRegex(ValueError, "another ingestion_date partition"):
                plan_date_backfill(
                    self.spark,
                    input_path=replacement,
                    target_date=date(2026, 10, 1),
                    bronze_path=bronze,
                    silver_valid_path=root / "missing-silver",
                )


if __name__ == "__main__":
    unittest.main()
