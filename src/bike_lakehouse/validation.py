"""Dependency-free business validation for generated source records."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation
import re
from typing import Any

from .generator import BIKE_TYPES, RIDER_TYPES, STATIONS


TRIP_KEYS = {
    "trip_id",
    "rider_key",
    "started_at",
    "ended_at",
    "start_station_id",
    "end_station_id",
    "rider_type",
    "bike_type",
    "distance_km",
}
STATION_KEYS = {
    "snapshot_date",
    "station_id",
    "station_name",
    "district",
    "capacity",
    "active",
}
STATION_IDS = {station[0] for station in STATIONS}


def _timestamp(value: Any, field: str, errors: list[str]) -> datetime | None:
    if not isinstance(value, str) or not value.endswith("Z"):
        errors.append(f"{field} must be a UTC timestamp ending with Z")
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        errors.append(f"{field} must be valid ISO 8601")
        return None


def validate_trip(record: Any) -> list[str]:
    errors: list[str] = []
    if not isinstance(record, dict):
        return ["trip must be an object"]
    if set(record) != TRIP_KEYS:
        errors.append("trip fields do not match the v1 contract")
    if not isinstance(record.get("trip_id"), str) or not record["trip_id"].startswith("trip_"):
        errors.append("trip_id must start with trip_")
    rider_key = record.get("rider_key")
    if not isinstance(rider_key, str) or re.fullmatch(r"rider_[0-9a-f]{16}", rider_key) is None:
        errors.append("rider_key must be a synthetic 16-hex identifier")
    started_at = _timestamp(record.get("started_at"), "started_at", errors)
    ended_at = _timestamp(record.get("ended_at"), "ended_at", errors)
    if started_at and ended_at and ended_at <= started_at:
        errors.append("ended_at must be later than started_at")
    start_station = record.get("start_station_id")
    end_station = record.get("end_station_id")
    if start_station not in STATION_IDS:
        errors.append("start_station_id is not supported")
    if end_station not in STATION_IDS:
        errors.append("end_station_id is not supported")
    if start_station == end_station:
        errors.append("start and end stations must differ")
    if record.get("rider_type") not in RIDER_TYPES:
        errors.append("rider_type is not supported")
    if record.get("bike_type") not in BIKE_TYPES:
        errors.append("bike_type is not supported")
    distance = record.get("distance_km")
    try:
        parsed_distance = Decimal(distance) if isinstance(distance, str) else None
    except InvalidOperation:
        parsed_distance = None
    if parsed_distance is None or parsed_distance <= 0 or parsed_distance.as_tuple().exponent != -2:
        errors.append("distance_km must be a positive decimal string with two places")
    return errors


def validate_station(record: Any) -> list[str]:
    errors: list[str] = []
    if not isinstance(record, dict):
        return ["station must be an object"]
    if set(record) != STATION_KEYS:
        errors.append("station fields do not match the v1 contract")
    try:
        date.fromisoformat(str(record.get("snapshot_date")))
    except ValueError:
        errors.append("snapshot_date must be an ISO date")
    if record.get("station_id") not in STATION_IDS:
        errors.append("station_id is not supported")
    if not isinstance(record.get("station_name"), str) or not record["station_name"]:
        errors.append("station_name must be non-empty")
    if not isinstance(record.get("district"), str) or not record["district"]:
        errors.append("district must be non-empty")
    capacity = record.get("capacity")
    if not isinstance(capacity, int) or isinstance(capacity, bool) or capacity <= 0:
        errors.append("capacity must be a positive integer")
    if not isinstance(record.get("active"), bool):
        errors.append("active must be a boolean")
    return errors
