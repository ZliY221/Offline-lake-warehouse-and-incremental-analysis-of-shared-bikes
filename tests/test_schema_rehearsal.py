import unittest

from bike_lakehouse.schema_rehearsal import (
    DatasetSchemaChange,
    SchemaConsumerContract,
    rehearse_schema_changes,
)


class SchemaRehearsalTests(unittest.TestCase):
    def test_blocks_removal_required_by_consumer(self):
        result = rehearse_schema_changes([
            DatasetSchemaChange("silver", ("trip_id", "station_id"), ("trip_id",)),
            DatasetSchemaChange("gold", ("trip_id",), ("trip_id",), ("silver",)),
        ], [SchemaConsumerContract("station-report", "silver", ("station_id",))])
        self.assertEqual("BLOCKED", result["status"])
        self.assertEqual("BLOCKED", result["datasets"][0]["status"])
        self.assertEqual(["station_id"], result["datasets"][0]["impacted_consumers"][0]["missing_fields"])
        self.assertFalse(result["schema_changes_applied"])

    def test_removed_field_with_dependent_dataset_requires_review(self):
        result = rehearse_schema_changes([
            DatasetSchemaChange("silver", ("trip_id", "legacy"), ("trip_id",)),
            DatasetSchemaChange("gold", ("trip_id",), ("trip_id",), ("silver",)),
        ], [])
        self.assertEqual("REVIEW", result["status"])
        self.assertEqual(["gold"], result["datasets"][0]["dependent_datasets"])

    def test_additive_change_is_ready(self):
        result = rehearse_schema_changes([
            DatasetSchemaChange("gold", ("trip_id",), ("trip_id", "duration_minutes")),
        ], [SchemaConsumerContract("dashboard", "gold", ("trip_id",))])
        self.assertEqual("READY", result["status"])
        self.assertEqual(["duration_minutes"], result["datasets"][0]["added_fields"])

    def test_unknown_dependency_is_rejected(self):
        with self.assertRaises(ValueError):
            rehearse_schema_changes([
                DatasetSchemaChange("gold", ("trip_id",), ("trip_id",), ("missing",)),
            ], [])
