from __future__ import annotations

import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from bike_lakehouse.batch_manifest import BatchManifest, fingerprint_file


class BatchManifestTests(unittest.TestCase):
    def test_fingerprint_and_state_transitions_are_auditable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.ndjson"
            source.write_bytes(b'{"trip_id":"one"}\n')
            digest, byte_count = fingerprint_file(source)
            self.assertEqual(digest, hashlib.sha256(source.read_bytes()).hexdigest())
            self.assertEqual(byte_count, source.stat().st_size)

            batch_id = "trip-bronze-20261001-test"
            first = BatchManifest(
                root / "manifest",
                batch_id,
                {"pipeline": "trip_bronze", "source_sha256": digest},
            )
            running = json.loads(first.path.read_text(encoding="utf-8"))
            self.assertEqual(running["status"], "RUNNING")
            first.fail(RuntimeError("simulated failure"))
            failed = json.loads(first.path.read_text(encoding="utf-8"))
            self.assertEqual(failed["status"], "FAILED")
            self.assertEqual(failed["error_type"], "RuntimeError")

            second = BatchManifest(
                root / "manifest",
                batch_id,
                {"pipeline": "trip_bronze", "source_sha256": digest},
            )
            second.complete(input_rows=1, corrupt_rows=0, output_rows_in_partition=1)
            succeeded = json.loads(second.path.read_text(encoding="utf-8"))
            self.assertEqual(succeeded["status"], "SUCCEEDED")
            self.assertEqual(succeeded["attempt_count"], 2)
            self.assertEqual(succeeded["input_rows"], 1)
            self.assertIsNone(succeeded["error_type"])


if __name__ == "__main__":
    unittest.main()
