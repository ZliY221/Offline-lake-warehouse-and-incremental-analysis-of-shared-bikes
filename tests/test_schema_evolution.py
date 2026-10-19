import unittest

from bike_lakehouse.schema_evolution import evaluate_trip_contract


class SchemaEvolutionTests(unittest.TestCase):
    def test_contract_accepts_optional_and_added_fields(self):
        result = evaluate_trip_contract({"trip_id": "string", "rider_key": "string", "started_at": "timestamp", "ended_at": "timestamp", "start_station_id": "string", "end_station_id": "string", "source_region": "string"})
        self.assertTrue(result.compatible)
        self.assertEqual(result.added_fields, ("source_region",))

    def test_contract_reports_breaking_changes(self):
        result = evaluate_trip_contract({"trip_id": "integer", "rider_key": "string", "started_at": "timestamp", "ended_at": "timestamp", "start_station_id": "string"})
        self.assertFalse(result.compatible)
        self.assertEqual(result.missing_required, ("end_station_id",))
        self.assertEqual(result.incompatible_types, ("trip_id",))
