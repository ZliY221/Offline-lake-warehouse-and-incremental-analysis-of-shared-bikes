#!/usr/bin/env python3
"""Audit Git-tracked files before publishing a lakehouse repository."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import subprocess
import sys
from typing import Any


HIGH_RISK_PATTERNS = {
    "private_key": re.compile(
        rb"-----BEGIN (?:RSA |OPENSSH |EC |DSA )?PRIVATE KEY-----"
    ),
    "github_token": re.compile(rb"gh[pousr]_[A-Za-z0-9]{20,}"),
    "aws_access_key": re.compile(rb"AKIA[0-9A-Z]{16}"),
    "openai_style_key": re.compile(rb"sk-[A-Za-z0-9]{20,}"),
}
REVIEW_PATTERNS = {
    "email": re.compile(
        rb"(?<![A-Za-z0-9._%+-])[A-Za-z0-9._%+-]+@"
        rb"[A-Za-z0-9.-]+\.[A-Za-z]{2,}(?![A-Za-z0-9.-])"
    ),
    "mainland_phone": re.compile(rb"(?<!\d)1[3-9]\d{9}(?!\d)"),
    "windows_user_path": re.compile(rb"[A-Za-z]:\\Users\\[^\\\s]+"),
}
NON_PROJECT_PATTERNS = {
    name: re.compile(term.encode("utf-8"), re.IGNORECASE)
    for name, term in {
        "resume_copy": "简历",
        "interview_copy": "面试",
        "job_search_copy": "求职",
        "campus_recruiting_copy": "校招",
        "internship_role_copy": "实习岗位",
        "portfolio_copy": "作品集",
        "learning_unit_copy": "学习单元",
        "learning_plan_copy": "学习计划",
        "exercise_copy": "动手练习",
        "lesson_objective_copy": "本单元目标",
        "course_source_copy": "课程源码",
        "reviewer_copy": "评审者",
    }.items()
}


def tracked_files(repo_root: Path) -> list[Path]:
    result = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=repo_root,
        check=True,
        capture_output=True,
    )
    return [
        repo_root / raw.decode("utf-8", errors="surrogateescape")
        for raw in result.stdout.split(b"\0")
        if raw
    ]


def matching_lines(content: bytes, pattern: re.Pattern[bytes]) -> list[int]:
    return [
        line_number
        for line_number, line in enumerate(content.splitlines(), start=1)
        if pattern.search(line)
    ]


def audit(repo_root: Path, review_size_bytes: int, fail_size_bytes: int) -> dict[str, Any]:
    high_risk: list[dict[str, Any]] = []
    review: list[dict[str, Any]] = []
    non_project: list[dict[str, Any]] = []
    large_files: list[dict[str, Any]] = []
    files = tracked_files(repo_root)
    script_path = Path(__file__).resolve()

    for path in files:
        if not path.is_file():
            continue
        size = path.stat().st_size
        relative = path.relative_to(repo_root).as_posix()
        if size >= review_size_bytes:
            large_files.append(
                {
                    "path": relative,
                    "bytes": size,
                    "status": "FAIL" if size >= fail_size_bytes else "REVIEW",
                }
            )
        if path.resolve() == script_path:
            continue
        content = path.read_bytes()
        if b"\0" in content:
            continue
        for rule, pattern in HIGH_RISK_PATTERNS.items():
            lines = matching_lines(content, pattern)
            if lines:
                high_risk.append({"path": relative, "rule": rule, "lines": lines})
        for rule, pattern in REVIEW_PATTERNS.items():
            lines = matching_lines(content, pattern)
            if lines:
                review.append({"path": relative, "rule": rule, "lines": lines})
        for rule, pattern in NON_PROJECT_PATTERNS.items():
            lines = matching_lines(content, pattern)
            if lines:
                non_project.append(
                    {"path": relative, "rule": rule, "lines": lines}
                )

    failed_large_files = [item for item in large_files if item["status"] == "FAIL"]
    if high_risk or non_project or failed_large_files:
        status = "FAIL"
    elif review or large_files:
        status = "PASS_WITH_REVIEW"
    else:
        status = "PASS"
    return {
        "status": status,
        "repository": repo_root.name,
        "tracked_file_count": len(files),
        "high_risk_matches": high_risk,
        "non_project_matches": non_project,
        "review_matches": review,
        "large_files": large_files,
        "thresholds": {
            "review_file_bytes": review_size_bytes,
            "fail_file_bytes": fail_size_bytes,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Audit Git-tracked files for publication risks"
    )
    parser.add_argument("--repo", type=Path, default=Path.cwd())
    parser.add_argument("--review-size-mb", type=int, default=5)
    parser.add_argument("--fail-size-mb", type=int, default=20)
    args = parser.parse_args()
    if args.review_size_mb <= 0 or args.fail_size_mb <= args.review_size_mb:
        raise SystemExit("size thresholds must be positive and fail > review")
    repo_root = args.repo.resolve()
    report = audit(
        repo_root,
        args.review_size_mb * 1024 * 1024,
        args.fail_size_mb * 1024 * 1024,
    )
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True))
    if report["status"] == "FAIL":
        sys.exit(1)


if __name__ == "__main__":
    main()

