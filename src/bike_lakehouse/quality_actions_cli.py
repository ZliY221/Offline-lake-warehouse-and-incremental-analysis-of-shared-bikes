"""Command-line quality remediation planner."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .quality_actions import build_remediation_plan_file


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a lakehouse quality remediation plan")
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--require-ready", action="store_true")
    args = parser.parse_args()
    plan = build_remediation_plan_file(args.report, args.output)
    print(json.dumps({
        "action_count": plan["action_count"],
        "ready_for_release": plan["ready_for_release"],
        "output": str(args.output),
    }, ensure_ascii=False, sort_keys=True))
    if args.require_ready and not plan["ready_for_release"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
