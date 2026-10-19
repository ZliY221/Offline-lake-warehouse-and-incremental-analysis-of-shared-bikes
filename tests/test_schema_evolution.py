from bike_lakehouse.schema_evolution import evaluate_trip_contract


def test_contract_accepts_optional_and_added_fields():
    result = evaluate_trip_contract({"trip_id": "string", "rider_key": "string", "started_at": "timestamp", "ended_at": "timestamp", "start_station_id": "string", "end_station_id": "string", "source_region": "string"})
    assert result.compatible is True
    assert result.added_fields == ("source_region",)


def test_contract_reports_breaking_changes():
    result = evaluate_trip_contract({"trip_id": "integer", "rider_key": "string", "started_at": "timestamp", "ended_at": "timestamp", "start_station_id": "string"})
    assert result.compatible is False
    assert result.missing_required == ("end_station_id",)
    assert result.incompatible_types == ("trip_id",)
