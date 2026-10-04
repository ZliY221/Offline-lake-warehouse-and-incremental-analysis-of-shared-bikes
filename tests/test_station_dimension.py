from __future__ import annotations

from datetime import date
import json
from pathlib import Path
import tempfile
import unittest

from bike_lakehouse.generator import generate_station_snapshot
from bike_lakehouse.spark import create_local_spark
from bike_lakehouse.station_dimension import build_station_dimension


def write_rows(path: Path, rows: list[dict[str, object]], malformed: bool = False) -> None:
    lines = [json.dumps(row, ensure_ascii=False, sort_keys=True) for row in rows]
    if malformed:
        lines.append("{broken-json}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


class StationDimensionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark_temp = tempfile.TemporaryDirectory()
        cls.spark = create_local_spark(
            "bike-station-dimension-tests",
            Path(cls.spark_temp.name) / "warehouse",
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()
        cls.spark_temp.cleanup()

    def test_attribute_changes_create_non_overlapping_scd2_versions(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            day_one = generate_station_snapshot(date(2026, 10, 1))
            day_two = generate_station_snapshot(date(2026, 10, 2))
            day_two[2]["capacity"] = 48
            day_two[4]["station_name"] = "科技馆东门"
            day_two[7]["active"] = False
            first = root / "stations-1.ndjson"
            second = root / "stations-2.ndjson"
            write_rows(first, day_one)
            write_rows(second, day_two)
            dimension = root / "dimension"
            rejected = root / "rejected"

            result = build_station_dimension(
                self.spark,
                input_paths=[first, second],
                dimension_path=dimension,
                rejected_path=rejected,
            )

            self.assertEqual(result.input_rows, 16)
            self.assertEqual(result.valid_snapshot_rows, 16)
            self.assertEqual(result.rejected_rows, 0)
            self.assertEqual(result.dimension_versions, 11)
            self.assertEqual(result.current_versions, 8)
            frame = self.spark.read.parquet(str(dimension))
            station_three = frame.where("station_id = 'ST-003'").orderBy("valid_from").collect()
            self.assertEqual(len(station_three), 2)
            self.assertEqual(station_three[0]["capacity"], 42)
            self.assertEqual(station_three[0]["valid_from"], date(2026, 10, 1))
            self.assertEqual(station_three[0]["valid_to"], date(2026, 10, 2))
            self.assertFalse(station_three[0]["is_current"])
            self.assertEqual(station_three[1]["capacity"], 48)
            self.assertIsNone(station_three[1]["valid_to"])
            self.assertTrue(station_three[1]["is_current"])
            self.assertEqual(self.spark.read.parquet(str(rejected)).count(), 0)

    def test_duplicate_and_invalid_snapshots_are_handled_deterministically(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            rows = generate_station_snapshot(date(2026, 10, 1))
            rows.append(dict(rows[0]))
            conflict = dict(rows[1])
            conflict["capacity"] = 99
            rows.append(conflict)
            invalid = dict(rows[2])
            invalid["capacity"] = 0
            rows.append(invalid)
            source = root / "stations.ndjson"
            write_rows(source, rows, malformed=True)
            dimension = root / "dimension"
            rejected = root / "rejected"

            first = build_station_dimension(
                self.spark,
                input_paths=[source],
                dimension_path=dimension,
                rejected_path=rejected,
            )
            second = build_station_dimension(
                self.spark,
                input_paths=[source],
                dimension_path=dimension,
                rejected_path=rejected,
            )

            self.assertEqual(first, second)
            self.assertEqual(first.input_rows, 12)
            self.assertEqual(first.exact_duplicate_rows, 1)
            self.assertEqual(first.conflicting_snapshot_rows, 2)
            self.assertEqual(first.rejected_rows, 4)
            self.assertEqual(first.valid_snapshot_rows, 7)
            self.assertEqual(first.dimension_versions, 7)
            rejected_frame = self.spark.read.parquet(str(rejected))
            error_codes = {
                error
                for row in rejected_frame.select("validation_errors").collect()
                for error in row["validation_errors"]
            }
            self.assertTrue(
                {"CONFLICTING_STATION_SNAPSHOT", "INVALID_CAPACITY", "MALFORMED_RECORD"}
                .issubset(error_codes)
            )
            self.assertNotIn("_corrupt_record", rejected_frame.columns)


if __name__ == "__main__":
    unittest.main()
