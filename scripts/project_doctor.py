#!/usr/bin/env python3
"""Cross-platform preflight checks for the shared-bike lakehouse."""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT = "bike-trip-lakehouse"
MIN_PYTHON = (3, 10)
ROOT = Path(__file__).resolve().parents[1]
REQUIRED_FILES = (
    "pyproject.toml",
    "data/sample/trips.ndjson",
    "data/sample/stations.ndjson",
    "dags/bike_lakehouse_airflow.py",
    "src/bike_lakehouse/pipeline_definition.py",
)


def result(check_id: str, label: str, status: str, detail: str) -> dict[str, str]:
    return {"id": check_id, "label": label, "status": status, "detail": detail}


def command_result(
    check_id: str,
    label: str,
    executable: str,
    args: list[str],
    minimum_major: int | None = None,
) -> dict[str, str]:
    resolved = shutil.which(executable)
    if resolved is None:
        return result(check_id, label, "FAIL", f"command not found: {executable}")
    try:
        completed = subprocess.run(
            [resolved, *args],
            cwd=ROOT,
            capture_output=True,
            timeout=20,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return result(check_id, label, "FAIL", f"command unavailable: {type(exc).__name__}")
    raw_output = completed.stdout + completed.stderr
    encoding = "utf-16le" if b"\x00" in raw_output else "utf-8"
    output = raw_output.decode(encoding, errors="replace").strip().replace("\n", " ")
    output = re.sub(r"\s+", " ", output)[:240]
    if completed.returncode != 0:
        return result(check_id, label, "FAIL", output or f"exit code {completed.returncode}")
    if minimum_major is not None:
        match = re.search(r"(?<!\d)(\d+)(?:\.\d+)+", output)
        if match is None or int(match.group(1)) < minimum_major:
            detail = f"requires major version {minimum_major}+; {output}"
            return result(check_id, label, "FAIL", detail)
    return result(check_id, label, "PASS", output or f"available: {executable}")


def module_result(module: str) -> dict[str, str]:
    available = importlib.util.find_spec(module) is not None
    return result(
        f"module.{module}",
        f"Python module {module}",
        "PASS" if available else "FAIL",
        "available" if available else f"install the project dependency that provides {module}",
    )


def managed_runtime_result(
    check_id: str, label: str, relative: str, setup_command: str
) -> dict[str, str]:
    available = (ROOT / relative).exists()
    return result(
        check_id,
        label,
        "PASS" if available else "FAIL",
        "available in build/" if available else f"run {setup_command}",
    )


def writable_result() -> dict[str, str]:
    build = ROOT / "build"
    try:
        build.mkdir(parents=True, exist_ok=True)
        descriptor, probe = tempfile.mkstemp(prefix="doctor-", dir=build)
        os.close(descriptor)
        Path(probe).unlink()
    except OSError as exc:
        return result("workspace.write", "Build directory", "FAIL", type(exc).__name__)
    return result("workspace.write", "Build directory", "PASS", "build/ is writable")


def evaluate(profile: str) -> dict[str, Any]:
    checks: list[dict[str, str]] = []
    python_ok = sys.version_info[:2] >= MIN_PYTHON
    checks.append(
        result(
            "runtime.python",
            "Python runtime",
            "PASS" if python_ok else "FAIL",
            f"{platform.python_version()} (requires {MIN_PYTHON[0]}.{MIN_PYTHON[1]}+)",
        )
    )
    for relative in REQUIRED_FILES:
        exists = (ROOT / relative).is_file()
        checks.append(
            result(
                f"file.{relative}",
                relative,
                "PASS" if exists else "FAIL",
                "present" if exists else "required project file is missing",
            )
        )
    checks.append(writable_result())
    checks.append(module_result("pyspark"))
    if platform.system() == "Windows":
        wsl_check = command_result(
            "runtime.wsl", "Windows Subsystem for Linux", "wsl.exe", ["--status"]
        )
        if wsl_check["status"] == "PASS":
            wsl_check["detail"] = "available"
        checks.append(wsl_check)
        checks.append(
            managed_runtime_result(
                "runtime.java",
                "Project-local JDK 17",
                "build/tools/jdk17/bin/java",
                "scripts/setup-wsl-tools.ps1",
            )
        )
        checks.append(
            managed_runtime_result(
                "runtime.pyspark",
                "Project-local PySpark",
                "build/tools/python-packages/pyspark",
                "scripts/setup-wsl-tools.ps1",
            )
        )
    else:
        checks.append(command_result("runtime.java", "Java runtime", "java", ["-version"], 17))
    if profile == "airflow":
        if platform.system() == "Windows":
            checks.append(
                managed_runtime_result(
                    "runtime.airflow",
                    "Project-local Airflow",
                    "build/airflow-runtime/venv/bin/airflow",
                    "scripts/setup-airflow.ps1",
                )
            )
        else:
            checks.append(module_result("airflow"))
    failed = sum(item["status"] == "FAIL" for item in checks)
    return {
        "schema_version": "1.0",
        "project": PROJECT,
        "profile": profile,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "platform": platform.platform(),
        "status": "READY" if failed == 0 else "BLOCKED",
        "summary": {"total": len(checks), "passed": len(checks) - failed, "failed": failed},
        "checks": checks,
    }


def write_json(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def write_markdown(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "# Project environment report",
        "",
        f"- Project: `{report['project']}`",
        f"- Profile: `{report['profile']}`",
        f"- Status: **{report['status']}**",
        f"- Checks: {report['summary']['passed']}/{report['summary']['total']} available",
        "",
        "| Check | Status | Detail |",
        "|---|---|---|",
    ]
    for check in report["checks"]:
        detail = check["detail"].replace("|", "\\|")
        lines.append(f"| {check['label']} | {check['status']} | {detail} |")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=("pipeline", "airflow"), default="pipeline")
    parser.add_argument("--json-output", type=Path)
    parser.add_argument("--markdown-output", type=Path)
    args = parser.parse_args()
    report = evaluate(args.profile)
    if args.json_output:
        write_json(args.json_output, report)
    if args.markdown_output:
        write_markdown(args.markdown_output, report)
    for check in report["checks"]:
        print(f"[{check['status']}] {check['label']}: {check['detail']}")
    print(f"Environment status: {report['status']}")
    return 0 if report["status"] == "READY" else 1


if __name__ == "__main__":
    raise SystemExit(main())
