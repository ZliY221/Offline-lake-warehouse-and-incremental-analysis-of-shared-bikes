"""Assess source-partition readiness from a JSON input file."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .batch_manifest import write_json_atomic
from .partition_readiness import PartitionReadinessInput, assess_partition_readiness


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", type=Path, default=Path("build/readiness/partitions.json"))
    parser.add_argument("--minimum-completeness", type=float, default=0.98)
    parser.add_argument("--maximum-lateness-minutes", type=int, default=30)
    args = parser.parse_args()
    payload = json.loads(args.input.read_text(encoding="utf-8"))
    partitions = [PartitionReadinessInput(**item) for item in payload]
    report = assess_partition_readiness(
        partitions,
        minimum_completeness=args.minimum_completeness,
        maximum_lateness_minutes=args.maximum_lateness_minutes,
    )
    write_json_atomic(args.output.resolve(), report)
    print(f"{report['status']}: {report['partition_count']} partitions")


if __name__ == "__main__":
    main()
