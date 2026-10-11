"""Shared command and dependency definition for the full lakehouse build."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PipelineCommand:
    name: str
    dependencies: tuple[str, ...]
    module: str
    arguments: tuple[str, ...]

    def argv(self, python_executable: str = "python3") -> tuple[str, ...]:
        return (python_executable, "-m", self.module, *self.arguments)


def build_pipeline_commands(run_root: Path) -> tuple[PipelineCommand, ...]:
    """Build the canonical five-stage pipeline for a given isolated run root."""

    lakehouse = run_root / "lakehouse"
    bronze = lakehouse / "bronze" / "trips"
    silver_valid = lakehouse / "silver" / "trips_valid"
    silver_rejected = lakehouse / "silver" / "trips_rejected"
    silver_duplicates = lakehouse / "silver" / "trips_duplicates"
    station_dimension = lakehouse / "dim" / "stations"
    station_rejected = lakehouse / "dim" / "stations_rejected"
    daily_metrics = lakehouse / "gold" / "daily_metrics"
    popular_routes = lakehouse / "gold" / "popular_routes"
    cohort_retention = lakehouse / "gold" / "cohort_retention"
    bronze_manifests = lakehouse / "control" / "bronze_batches"
    backfill_manifests = lakehouse / "control" / "date_backfills"
    quality_report = run_root / "reports" / "data-quality.json"

    def arguments(*values: object) -> tuple[str, ...]:
        return tuple(str(value) for value in values)

    return (
        PipelineCommand(
            "bronze",
            (),
            "bike_lakehouse.bronze_cli",
            arguments(
                "--input",
                "data/sample/trips.ndjson",
                "--output",
                bronze,
                "--ingestion-date",
                "2026-10-01",
                "--manifest",
                bronze_manifests,
            ),
        ),
        PipelineCommand(
            "silver",
            ("bronze",),
            "bike_lakehouse.silver_cli",
            arguments(
                "--bronze",
                bronze,
                "--valid",
                silver_valid,
                "--rejected",
                silver_rejected,
                "--duplicates",
                silver_duplicates,
            ),
        ),
        PipelineCommand(
            "station_dimension",
            (),
            "bike_lakehouse.station_dimension_cli",
            arguments(
                "--input",
                "data/sample/stations.ndjson",
                "--input",
                "data/sample/stations-2026-10-02.ndjson",
                "--dimension",
                station_dimension,
                "--rejected",
                station_rejected,
            ),
        ),
        PipelineCommand(
            "gold",
            ("silver", "station_dimension"),
            "bike_lakehouse.gold_cli",
            arguments(
                "--silver-trips",
                silver_valid,
                "--station-dimension",
                station_dimension,
                "--daily-metrics",
                daily_metrics,
                "--popular-routes",
                popular_routes,
                "--cohort-retention",
                cohort_retention,
            ),
        ),
        PipelineCommand(
            "quality_gate",
            ("gold",),
            "bike_lakehouse.quality_report_cli",
            arguments(
                "--bronze",
                bronze,
                "--silver-valid",
                silver_valid,
                "--silver-rejected",
                silver_rejected,
                "--silver-duplicates",
                silver_duplicates,
                "--station-dimension",
                station_dimension,
                "--station-rejected",
                station_rejected,
                "--gold-daily-metrics",
                daily_metrics,
                "--gold-popular-routes",
                popular_routes,
                "--gold-cohort-retention",
                cohort_retention,
                "--bronze-manifest",
                bronze_manifests,
                "--backfill-manifest",
                backfill_manifests,
                "--output",
                quality_report,
                "--required-latest-ingestion-date",
                "2026-10-01",
                "--fail-on-error",
            ),
        ),
    )
