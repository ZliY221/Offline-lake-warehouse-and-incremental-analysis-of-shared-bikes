import unittest

from bike_lakehouse.data_release import build_release


class DataReleaseTests(unittest.TestCase):
    def test_release_is_deterministic_and_sorted(self):
        release = build_release(dataset="trips_gold", partitions=["d=2", "d=1", "d=1"],
                                row_count=40, schema_version="3", source_fingerprint="abc",
                                quality_passed=True)
        self.assertEqual(("d=1", "d=2"), release.partitions)
        self.assertEqual("READY", release.status)
        self.assertEqual(release.release_id, build_release(dataset="trips_gold", partitions=["d=1", "d=2"], row_count=40, schema_version="3", source_fingerprint="abc", quality_passed=True).release_id)

    def test_failed_quality_blocks_release(self):
        release = build_release(dataset="trips", partitions=["d=1"], row_count=0,
                                schema_version="3", source_fingerprint="abc",
                                quality_passed=False)
        self.assertEqual("BLOCKED", release.status)

