from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
import json
import unittest

from bike_lakehouse.generator import generate_station_snapshot, generate_trips
from bike_lakehouse.validation import validate_station, validate_trip


class GeneratorTests(unittest.TestCase):
    def test_generation_is_deterministic_and_ids_are_unique(self) -> None:
        first = generate_trips(30, seed=2027)
        second = generate_trips(30, seed=2027)
        self.assertEqual(first, second)
        self.assertEqual(len({row["trip_id"] for row in first}), 30)

    def test_generated_records_pass_contracts(self) -> None:
        trips = generate_trips(20)
        stations = generate_station_snapshot(date(2026, 10, 1))
        self.assertTrue(all(validate_trip(row) == [] for row in trips))
        self.assertTrue(all(validate_station(row) == [] for row in stations))

    def test_trip_times_stations_and_decimal_distance_are_valid(self) -> None:
        for row in generate_trips(40):
            started = datetime.fromisoformat(row["started_at"].replace("Z", "+00:00"))
            ended = datetime.fromisoformat(row["ended_at"].replace("Z", "+00:00"))
            self.assertGreater(ended, started)
            self.assertNotEqual(row["start_station_id"], row["end_station_id"])
            distance = Decimal(row["distance_km"])
            self.assertGreater(distance, 0)
            self.assertEqual(distance.as_tuple().exponent, -2)

    def test_no_direct_personal_fields_are_generated(self) -> None:
        serialized = json.dumps(generate_trips(20), ensure_ascii=False).lower()
        for forbidden in ("name", "phone", "email", "address", "id_card", "latitude", "longitude"):
            self.assertNotIn(forbidden, serialized)

    def test_invalid_count_and_naive_time_are_rejected(self) -> None:
        with self.assertRaises(ValueError):
            generate_trips(-1)
        with self.assertRaises(ValueError):
            generate_trips(1, start_time=datetime(2026, 10, 1))
        self.assertEqual(
            generate_trips(0, start_time=datetime(2026, 10, 1, tzinfo=UTC)),
            [],
        )


if __name__ == "__main__":
    unittest.main()
