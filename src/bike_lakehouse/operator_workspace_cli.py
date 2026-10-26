"""Create the standard 100-journey offline operator workspace."""

from __future__ import annotations

import argparse
from pathlib import Path

from .batch_manifest import write_json_atomic
from .operator_journey import standard_operator_journeys
from .operator_workspace import build_operator_workspace, render_operator_workspace_html


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-json", type=Path,
                        default=Path("build/usability/operator-workspace.json"))
    parser.add_argument("--output-html", type=Path,
                        default=Path("build/usability/operator-workspace.html"))
    parser.add_argument("--visible-limit", type=int, default=12)
    args = parser.parse_args()
    workspace = build_operator_workspace(
        standard_operator_journeys(), visible_limit=args.visible_limit
    )
    write_json_atomic(args.output_json.resolve(), workspace)
    args.output_html.parent.mkdir(parents=True, exist_ok=True)
    args.output_html.write_text(render_operator_workspace_html(workspace), encoding="utf-8")
    print(
        f"{workspace['status']}: {workspace['visible_count']} focused, "
        f"{workspace['remaining_count']} queued"
    )


if __name__ == "__main__":
    main()
