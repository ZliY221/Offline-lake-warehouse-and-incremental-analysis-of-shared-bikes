from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

from bike_lakehouse.compaction import summarize_compaction_journal


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect a resumable compaction journal")
    parser.add_argument("journal", type=Path)
    parser.add_argument("--pretty", action="store_true", help="indent the JSON status output")
    parser.add_argument(
        "--require-complete",
        action="store_true",
        help="exit with status 2 unless compaction is fully completed",
    )
    args = parser.parse_args()
    summary = summarize_compaction_journal(args.journal)
    print(json.dumps(
        summary,
        ensure_ascii=False,
        indent=2 if args.pretty else None,
        sort_keys=True,
    ))
    if args.require_complete and not summary["automation_ready"]:
        sys.exit(2)


if __name__ == "__main__":
    main()
