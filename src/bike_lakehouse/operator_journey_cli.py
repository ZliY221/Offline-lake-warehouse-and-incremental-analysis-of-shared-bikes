"""Generate the standard 100-journey operator usability rehearsal."""

from __future__ import annotations

import argparse
from pathlib import Path

from .batch_manifest import write_json_atomic
from .operator_journey import rehearse_operator_journeys, standard_operator_journeys


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("build/usability/operator-journeys.json"),
    )
    args = parser.parse_args()
    report = rehearse_operator_journeys(standard_operator_journeys())
    write_json_atomic(args.output.resolve(), report)
    print(
        f"{report['status']}: {report['journey_count']} journeys, "
        f"{report['unresolved_count']} unresolved"
    )


if __name__ == "__main__":
    main()
