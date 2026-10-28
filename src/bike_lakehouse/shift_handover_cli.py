"""Build an offline shift handover packet from a JSON work list."""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from .batch_manifest import write_json_atomic
from .shift_handover import HandoverWorkItem, build_shift_handover


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", type=Path, default=Path("build/operations/shift-handover.json"))
    parser.add_argument("--generated-at", default=None)
    parser.add_argument("--focus-limit", type=int, default=12)
    parser.add_argument("--stale-after-minutes", type=int, default=120)
    args = parser.parse_args()
    payload = json.loads(args.input.read_text(encoding="utf-8"))
    report = build_shift_handover(
        [HandoverWorkItem(**item) for item in payload],
        generated_at=args.generated_at or datetime.now(UTC).isoformat(),
        focus_limit=args.focus_limit,
        stale_after_minutes=args.stale_after_minutes,
    )
    write_json_atomic(args.output.resolve(), report)
    print(f"{report['status']}: {report['visible_count']} focus items")


if __name__ == "__main__":
    main()
