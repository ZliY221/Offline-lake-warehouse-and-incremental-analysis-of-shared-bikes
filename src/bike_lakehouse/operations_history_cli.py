"""Create a multi-run operations history bundle from snapshot JSON files."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .operations_history import build_operations_history, write_operations_history


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("snapshots", nargs="+", type=Path)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--output-html", type=Path, required=True)
    parser.add_argument("--recent-window", type=int, default=5)
    parser.add_argument("--readiness-threshold", type=float, default=0.8)
    args = parser.parse_args()
    payloads = [json.loads(path.read_text(encoding="utf-8")) for path in args.snapshots]
    history = build_operations_history(
        payloads,
        recent_window=args.recent_window,
        readiness_threshold=args.readiness_threshold,
    )
    write_operations_history(history, args.output_json, args.output_html)
    print(json.dumps({
        "run_count": history["summary"]["run_count"],
        "trend_status": history["summary"]["trend_status"],
        "output_html": str(args.output_html),
    }, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
