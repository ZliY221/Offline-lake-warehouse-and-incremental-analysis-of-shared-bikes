from __future__ import annotations

import argparse
import json
from pathlib import Path

from bike_lakehouse.compaction import summarize_compaction_journal


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect a resumable compaction journal")
    parser.add_argument("journal", type=Path)
    args = parser.parse_args()
    print(json.dumps(summarize_compaction_journal(args.journal), ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
