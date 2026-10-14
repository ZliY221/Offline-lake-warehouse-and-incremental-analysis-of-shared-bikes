"""Compare two lakehouse quality reports and surface regressions."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .batch_manifest import utc_timestamp, write_json_atomic


def _check_statuses(report: dict[str, Any]) -> dict[str, str]:
    checks = report.get("checks")
    if not isinstance(checks, list):
        raise ValueError("quality report checks must be a list")
    statuses: dict[str, str] = {}
    for check in checks:
        name = str(check.get("name", "")).strip()
        status = str(check.get("status", "")).strip()
        if not name or status not in {"PASS", "FAIL"}:
            raise ValueError("every quality check must contain a valid name and status")
        if name in statuses:
            raise ValueError(f"duplicate quality check: {name}")
        statuses[name] = status
    return statuses


def compare_quality_reports(
    baseline: dict[str, Any], candidate: dict[str, Any]
) -> dict[str, Any]:
    """Return check-level changes and fail when the candidate introduces regressions."""
    baseline_statuses = _check_statuses(baseline)
    candidate_statuses = _check_statuses(candidate)
    names = sorted(set(baseline_statuses) | set(candidate_statuses))
    changes = []
    regressions = []
    recoveries = []
    for name in names:
        previous = baseline_statuses.get(name, "MISSING")
        current = candidate_statuses.get(name, "MISSING")
        classification = "UNCHANGED"
        if previous == "PASS" and current != "PASS":
            classification = "REGRESSION"
            regressions.append(name)
        elif previous != "PASS" and current == "PASS":
            classification = "RECOVERY"
            recoveries.append(name)
        elif previous != current:
            classification = "CHANGED"
        changes.append(
            {"name": name, "baseline_status": previous, "candidate_status": current,
             "classification": classification}
        )
    return {
        "generated_at_utc": utc_timestamp(),
        "comparison_status": "FAIL" if regressions else "PASS",
        "baseline_overall_status": baseline.get("overall_status"),
        "candidate_overall_status": candidate.get("overall_status"),
        "regression_count": len(regressions),
        "recovery_count": len(recoveries),
        "regressions": regressions,
        "recoveries": recoveries,
        "checks": changes,
    }


def compare_quality_report_files(
    baseline_path: Path, candidate_path: Path, output_path: Path
) -> dict[str, Any]:
    baseline = json.loads(Path(baseline_path).read_text(encoding="utf-8"))
    candidate = json.loads(Path(candidate_path).read_text(encoding="utf-8"))
    comparison = compare_quality_reports(baseline, candidate)
    write_json_atomic(Path(output_path).resolve(), comparison)
    return comparison
