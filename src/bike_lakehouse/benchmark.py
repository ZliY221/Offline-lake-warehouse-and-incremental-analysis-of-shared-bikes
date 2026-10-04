"""Reproducible local end-to-end benchmark with raw per-round evidence."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
import json
import os
from pathlib import Path
import platform
import statistics
import subprocess
from time import perf_counter
from typing import Any

from .batch_manifest import fingerprint_file, utc_timestamp, write_json_atomic
from .bronze import ingest_trip_bronze
from .generator import generate_station_snapshot, generate_trips
from .gold import build_gold_analytics
from .silver import build_trip_silver
from .station_dimension import build_station_dimension


def _write_rows(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as stream:
        for row in rows:
            stream.write(json.dumps(row, ensure_ascii=False, sort_keys=True))
            stream.write("\n")


def _git_commit(repo_root: Path) -> str | None:
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=repo_root,
        check=False,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip() if result.returncode == 0 else None


def _memory_bytes() -> int | None:
    meminfo = Path("/proc/meminfo")
    if not meminfo.is_file():
        return None
    for line in meminfo.read_text(encoding="utf-8").splitlines():
        if line.startswith("MemTotal:"):
            return int(line.split()[1]) * 1024
    return None


def summarize_rounds(rounds: list[dict[str, Any]], input_rows: int) -> dict[str, float]:
    if not rounds:
        raise ValueError("At least one measured round is required")
    stage_names = ["bronze_seconds", "silver_seconds", "gold_seconds", "total_seconds"]
    summary = {
        f"median_{stage}": round(
            statistics.median(float(round_result[stage]) for round_result in rounds),
            3,
        )
        for stage in stage_names
    }
    median_total = summary["median_total_seconds"]
    summary["median_end_to_end_input_rows_per_second"] = round(
        input_rows / median_total,
        2,
    )
    return summary


def run_benchmark(
    spark: Any,
    *,
    repo_root: Path,
    work_path: Path,
    report_path: Path,
    input_rows: int,
    seed: int,
    batch_date: date,
    warmup_rounds: int,
    measured_rounds: int,
) -> dict[str, Any]:
    if input_rows <= 0:
        raise ValueError("input_rows must be greater than zero")
    if warmup_rounds < 0 or measured_rounds <= 0:
        raise ValueError("warmup_rounds must be non-negative and measured_rounds positive")

    work_path = Path(work_path).resolve()
    source = work_path / "input" / "trips.ndjson"
    stations = work_path / "input" / "stations.ndjson"
    start_time = datetime.combine(batch_date, datetime.min.time(), tzinfo=UTC) + timedelta(
        hours=6
    )
    _write_rows(source, generate_trips(input_rows, seed=seed, start_time=start_time))
    _write_rows(stations, generate_station_snapshot(batch_date))
    source_sha256, source_bytes = fingerprint_file(source)

    dimension_path = work_path / "lakehouse" / "dim" / "stations"
    build_station_dimension(
        spark,
        input_paths=[stations],
        dimension_path=dimension_path,
        rejected_path=work_path / "lakehouse" / "dim" / "stations_rejected",
    )

    bronze_path = work_path / "lakehouse" / "bronze" / "trips"
    silver_valid = work_path / "lakehouse" / "silver" / "trips_valid"
    silver_rejected = work_path / "lakehouse" / "silver" / "trips_rejected"
    silver_duplicates = work_path / "lakehouse" / "silver" / "trips_duplicates"
    gold_daily = work_path / "lakehouse" / "gold" / "daily_metrics"
    gold_routes = work_path / "lakehouse" / "gold" / "popular_routes"

    raw_rounds: list[dict[str, Any]] = []
    total_rounds = warmup_rounds + measured_rounds
    for index in range(total_rounds):
        total_started = perf_counter()
        stage_started = perf_counter()
        bronze = ingest_trip_bronze(
            spark,
            input_path=source,
            output_path=bronze_path,
            ingestion_date=batch_date,
        )
        bronze_seconds = perf_counter() - stage_started

        stage_started = perf_counter()
        silver = build_trip_silver(
            spark,
            bronze_path=bronze_path,
            valid_path=silver_valid,
            rejected_path=silver_rejected,
            duplicate_path=silver_duplicates,
        )
        silver_seconds = perf_counter() - stage_started

        stage_started = perf_counter()
        gold = build_gold_analytics(
            spark,
            silver_trip_path=silver_valid,
            station_dimension_path=dimension_path,
            daily_metrics_path=gold_daily,
            popular_routes_path=gold_routes,
        )
        gold_seconds = perf_counter() - stage_started
        total_seconds = perf_counter() - total_started

        if index >= warmup_rounds:
            raw_rounds.append(
                {
                    "round": index - warmup_rounds + 1,
                    "bronze_seconds": round(bronze_seconds, 3),
                    "silver_seconds": round(silver_seconds, 3),
                    "gold_seconds": round(gold_seconds, 3),
                    "total_seconds": round(total_seconds, 3),
                    "bronze_partition_rows": bronze.output_rows_in_partition,
                    "silver_valid_rows": silver.valid_rows,
                    "gold_daily_metric_rows": gold.daily_metric_rows,
                    "gold_popular_route_rows": gold.popular_route_rows,
                }
            )

    report = {
        "generated_at_utc": utc_timestamp(),
        "method": {
            "spark_session": "one persistent local[2] session; startup excluded",
            "dataset": "one deterministic NDJSON file reused in every round",
            "timing": "time.perf_counter wall time; full write and validation actions included",
            "pipeline": "Bronze dynamic overwrite -> Silver full rebuild -> Gold full rebuild",
            "warmup_rounds": warmup_rounds,
            "measured_rounds": measured_rounds,
        },
        "environment": {
            "platform": platform.platform(),
            "python_version": platform.python_version(),
            "java_version": spark._jvm.java.lang.System.getProperty("java.version"),
            "spark_version": spark.version,
            "spark_master": spark.sparkContext.master,
            "logical_cpu_count": os.cpu_count(),
            "memory_bytes": _memory_bytes(),
            "git_commit": _git_commit(Path(repo_root).resolve()),
        },
        "input": {
            "rows": input_rows,
            "bytes": source_bytes,
            "sha256": source_sha256,
            "seed": seed,
            "batch_date": batch_date.isoformat(),
        },
        "raw_rounds": raw_rounds,
        "summary": summarize_rounds(raw_rounds, input_rows),
    }
    write_json_atomic(Path(report_path).resolve(), report)
    return report
