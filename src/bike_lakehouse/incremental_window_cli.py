"""Plan incremental load windows from a JSON candidate file."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .batch_manifest import write_json_atomic
from .incremental_window import IncrementalLoadCandidate, plan_incremental_load_windows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", type=Path, default=Path("build/readiness/load-windows.json"))
    parser.add_argument("--parallel-slots", type=int, default=2)
    parser.add_argument("--window-minutes", type=int, default=120)
    args = parser.parse_args()
    payload = json.loads(args.input.read_text(encoding="utf-8"))
    report = plan_incremental_load_windows(
        [IncrementalLoadCandidate(**item) for item in payload],
        parallel_slots=args.parallel_slots,
        window_minutes=args.window_minutes,
    )
    write_json_atomic(args.output.resolve(), report)
    print(f"{report['status']}: {report['planned_count']} planned")


if __name__ == "__main__":
    main()
