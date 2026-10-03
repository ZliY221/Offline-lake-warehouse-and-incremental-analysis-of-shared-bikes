"""Generate deterministic NDJSON source batches."""

from __future__ import annotations

import argparse
from datetime import UTC, date, datetime, timedelta
import json
from pathlib import Path

from .generator import generate_station_snapshot, generate_trips
from .validation import validate_station, validate_trip


def _date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("date must use YYYY-MM-DD") from error


def _write(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as stream:
        for row in rows:
            stream.write(json.dumps(row, ensure_ascii=False, sort_keys=True))
            stream.write("\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate bike lakehouse source data")
    parser.add_argument("--date", type=_date, default=date(2026, 10, 1))
    parser.add_argument("--count", type=int, default=20)
    parser.add_argument("--seed", type=int, default=2027)
    parser.add_argument("--trips-output", type=Path, required=True)
    parser.add_argument("--stations-output", type=Path, required=True)
    args = parser.parse_args()
    start_time = datetime.combine(args.date, datetime.min.time(), tzinfo=UTC) + timedelta(hours=6)
    trips = generate_trips(args.count, seed=args.seed, start_time=start_time)
    stations = generate_station_snapshot(args.date)
    violations = [errors for row in trips if (errors := validate_trip(row))]
    violations.extend(errors for row in stations if (errors := validate_station(row)))
    if violations:
        raise ValueError(f"generated records violate their contracts: {violations}")
    _write(args.trips_output, trips)
    _write(args.stations_output, stations)
    print(f"generated {len(trips)} trips and {len(stations)} stations for {args.date}")


if __name__ == "__main__":
    main()
