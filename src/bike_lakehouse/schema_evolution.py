"""Compatibility checks for evolving trip source contracts."""

from __future__ import annotations

from dataclasses import asdict, dataclass


@dataclass(frozen=True)
class ContractCompatibility:
    compatible: bool
    missing_required: tuple[str, ...]
    incompatible_types: tuple[str, ...]
    added_fields: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


TRIP_CONTRACT: dict[str, tuple[str, bool]] = {
    "trip_id": ("string", True), "rider_key": ("string", True),
    "started_at": ("timestamp", True), "ended_at": ("timestamp", True),
    "start_station_id": ("string", True), "end_station_id": ("string", True),
    "rider_type": ("string", False), "bike_type": ("string", False),
    "distance_km": ("decimal", False),
}


def evaluate_trip_contract(fields: dict[str, str]) -> ContractCompatibility:
    """Compare normalized source fields with the supported trip contract."""
    normalized = {name: value.lower().split("(", 1)[0] for name, value in fields.items()}
    missing = tuple(name for name, (_, required) in TRIP_CONTRACT.items() if required and name not in normalized)
    incompatible = tuple(name for name, (expected, _) in TRIP_CONTRACT.items() if name in normalized and normalized[name] != expected)
    added = tuple(sorted(set(normalized) - set(TRIP_CONTRACT)))
    return ContractCompatibility(not missing and not incompatible, missing, incompatible, added)
