from __future__ import annotations

import hashlib
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from bike_lakehouse.repair_plan import build_source_catalog


class RepairSourceCatalogTests(unittest.TestCase):
    def test_catalog_is_sorted_and_contains_fingerprints_without_rows(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            later = root / "trips-2026-10-03.ndjson"
            earlier = root / "trips-2026-10-02.ndjson"
            later.write_text('{"trip_id":"later"}\n', encoding="utf-8", newline="\n")
            earlier.write_text('{"trip_id":"earlier"}\n', encoding="utf-8", newline="\n")
            catalog = build_source_catalog(root)
            self.assertEqual(2, catalog["source_count"])
            self.assertEqual(
                ["2026-10-02", "2026-10-03"],
                [row["ingestion_date"] for row in catalog["sources"]],
            )
            self.assertEqual(
                hashlib.sha256(earlier.read_bytes()).hexdigest(),
                catalog["sources"][0]["source_sha256"],
            )
            self.assertNotIn("trip_id", str(catalog))

    def test_empty_directory_produces_an_explicit_empty_catalog(self) -> None:
        with TemporaryDirectory() as directory:
            catalog = build_source_catalog(Path(directory))
            self.assertEqual(
                {"catalog_version": "1.0", "status": "READY", "source_count": 0, "sources": []},
                catalog,
            )

    def test_unexpected_names_and_missing_directories_are_rejected(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "repair-input.ndjson").write_text("{}\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "unexpected repair source name"):
                build_source_catalog(root)
            with self.assertRaisesRegex(ValueError, "does not exist"):
                build_source_catalog(root / "missing")


if __name__ == "__main__":
    unittest.main()
