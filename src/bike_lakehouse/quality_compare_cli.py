"""Command-line quality report comparison."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .quality_compare import compare_quality_report_files


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare two lakehouse quality reports")
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--candidate", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--fail-on-regression", action="store_true")
    args = parser.parse_args()
    result = compare_quality_report_files(args.baseline, args.candidate, args.output)
    print(json.dumps({
        "comparison_status": result["comparison_status"],
        "regression_count": result["regression_count"],
        "recovery_count": result["recovery_count"],
        "output": str(args.output),
    }, ensure_ascii=False, sort_keys=True))
    if args.fail_on_regression and result["regression_count"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
