"""Deterministic, privacy-safe bike trip and station snapshot generation."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
import random
import uuid


RIDER_TYPES = ("member", "casual")
BIKE_TYPES = ("classic", "electric")
STATIONS = (
    ("ST-001", "滨河公园", "西市区", 28),
    ("ST-002", "辽河广场", "站前区", 36),
    ("ST-003", "大学园区", "西市区", 42),
    ("ST-004", "客运中心", "站前区", 32),
    ("ST-005", "科技馆", "西市区", 24),
    ("ST-006", "体育场", "站前区", 30),
    ("ST-007", "沿海产业基地", "沿海产业基地", 40),
    ("ST-008", "民兴河公园", "西市区", 26),
)


def _utc_text(value: datetime) -> str:
    return value.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def generate_trips(
    count: int,
    *,
    seed: int = 2027,
    start_time: datetime | None = None,
) -> list[dict[str, object]]:
    if count < 0:
        raise ValueError("count must be zero or greater")
    base_time = start_time or datetime(2026, 10, 1, 6, 0, tzinfo=UTC)
    if base_time.tzinfo is None:
        raise ValueError("start_time must include timezone information")
    rng = random.Random(seed)
    rider_rng = random.Random(seed ^ 0x5A17)
    namespace = uuid.uuid5(uuid.NAMESPACE_URL, f"bike-trip-lakehouse:{seed}")
    rider_namespace = uuid.uuid5(namespace, "synthetic-riders")
    rider_pool_size = max(1, count // 5)
    rider_keys = [
        f"rider_{uuid.uuid5(rider_namespace, str(index)).hex[:16]}"
        for index in range(rider_pool_size)
    ]
    station_ids = [station[0] for station in STATIONS]
    trips: list[dict[str, object]] = []
    for index in range(count):
        started_at = base_time + timedelta(seconds=index * 73)
        duration_seconds = rng.randint(4 * 60, 48 * 60)
        ended_at = started_at + timedelta(seconds=duration_seconds)
        start_station_id, end_station_id = rng.sample(station_ids, 2)
        distance = Decimal(rng.randint(45, 1250)) / Decimal("100")
        trip_uuid = uuid.uuid5(namespace, f"{seed}:{index}")
        trips.append(
            {
                "trip_id": f"trip_{trip_uuid.hex}",
                "rider_key": rider_rng.choice(rider_keys),
                "started_at": _utc_text(started_at),
                "ended_at": _utc_text(ended_at),
                "start_station_id": start_station_id,
                "end_station_id": end_station_id,
                "rider_type": rng.choice(RIDER_TYPES),
                "bike_type": rng.choice(BIKE_TYPES),
                "distance_km": f"{distance:.2f}",
            }
        )
    return trips


def generate_station_snapshot(snapshot_date: date) -> list[dict[str, object]]:
    return [
        {
            "snapshot_date": snapshot_date.isoformat(),
            "station_id": station_id,
            "station_name": station_name,
            "district": district,
            "capacity": capacity,
            "active": True,
        }
        for station_id, station_name, district, capacity in STATIONS
    ]
