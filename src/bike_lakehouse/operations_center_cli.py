"""Command-line entry point for the offline lakehouse operations center."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .operations_center import build_operations_snapshot, write_operations_bundle


def _read(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Build an offline run operations bundle")
    parser.add_argument("--quality-report", type=Path, required=True)
    parser.add_argument("--orchestration-state", type=Path, required=True)
    parser.add_argument("--remediation-plan", type=Path)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--output-html", type=Path, required=True)
    parser.add_argument("--require-ready", action="store_true")
    args = parser.parse_args()
    remediation = _read(args.remediation_plan) if args.remediation_plan else None
    snapshot = build_operations_snapshot(
        _read(args.quality_report), _read(args.orchestration_state), remediation
    )
    write_operations_bundle(snapshot, args.output_json, args.output_html)
    print(json.dumps({
        "status": snapshot["status"],
        "release_ready": snapshot["release_ready"],
        "output_json": str(args.output_json),
        "output_html": str(args.output_html),
    }, ensure_ascii=False, sort_keys=True))
    if args.require_ready and not snapshot["release_ready"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
