import unittest

from bike_lakehouse.data_release import build_release, build_restore_point, compare_releases, delivery_sla, recovery_drill, release_audit, release_handoff, retention_decision


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

    def test_restore_point_only_executes_for_ready_release(self):
        release = build_release(dataset="trips", partitions=["d=1"], row_count=10,
                                schema_version="3", source_fingerprint="abc",
                                quality_passed=True)
        point = build_restore_point(release)
        self.assertTrue(point.executable)
        self.assertEqual(release.release_id, point.release_id)

    def test_retention_preserves_minimum_recovery_floor(self):
        self.assertEqual("KEEP_MINIMUM", retention_decision(age_days=200, retained_releases=3)["action"])
        self.assertEqual("ARCHIVE", retention_decision(age_days=200, retained_releases=8)["action"])

    def test_audit_flags_blocked_and_duplicate_releases(self):
        release = build_release(dataset="trips", partitions=["d=1"], row_count=10,
                                schema_version="3", source_fingerprint="abc",
                                quality_passed=False)
        audit = release_audit([release, release])
        self.assertEqual("REVIEW", audit["audit_status"])
        self.assertEqual([release.release_id], audit["duplicate_release_ids"])

    def test_release_comparison_flags_schema_and_removed_partitions(self):
        old = build_release(dataset="trips", partitions=["d=1", "d=2"], row_count=20, schema_version="3", source_fingerprint="a", quality_passed=True)
        new = build_release(dataset="trips", partitions=["d=2", "d=3"], row_count=25, schema_version="4", source_fingerprint="b", quality_passed=True)
        diff = compare_releases(old, new)
        self.assertEqual(["d=3"], diff["added_partitions"])
        self.assertTrue(diff["requires_review"])

    def test_recovery_drill_requires_all_partitions_and_fingerprint(self):
        release = build_release(dataset="trips", partitions=["d=1", "d=2"], row_count=20, schema_version="3", source_fingerprint="a", quality_passed=True)
        result = recovery_drill(build_restore_point(release), restored_partitions=["d=1"], fingerprint_matches=True)
        self.assertEqual("REVIEW", result["status"])
        self.assertEqual(["d=2"], result["missing_partitions"])

    def test_delivery_sla_reports_late_seconds(self):
        result = delivery_sla(ready_at_epoch=100, due_at_epoch=200, delivered_at_epoch=230)
        self.assertEqual("DELIVERED_LATE", result["status"])
        self.assertEqual(30, result["delay_seconds"])

    def test_handoff_blocks_missing_evidence(self):
        release = build_release(dataset="trips", partitions=["d=1"], row_count=10, schema_version="3", source_fingerprint="a", quality_passed=True)
        result = release_handoff(release, owner="data-team", consumer="reporting", evidence_links=[])
        self.assertEqual("BLOCKED", result["handoff_status"])
        self.assertIn("delivery evidence is missing", result["blockers"])

