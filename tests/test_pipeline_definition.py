from __future__ import annotations

from pathlib import Path
import unittest

from bike_lakehouse.managed_pipeline_cli import _tasks
from bike_lakehouse.pipeline_definition import build_pipeline_commands


class PipelineDefinitionTests(unittest.TestCase):
    def test_canonical_graph_has_expected_dependencies_and_quality_gate(self) -> None:
        commands = build_pipeline_commands(Path("build/test-run"))
        self.assertEqual(
            {command.name: command.dependencies for command in commands},
            {
                "bronze": (),
                "silver": ("bronze",),
                "station_dimension": (),
                "gold": ("silver", "station_dimension"),
                "quality_gate": ("gold",),
            },
        )
        quality = commands[-1]
        self.assertEqual(quality.module, "bike_lakehouse.quality_report_cli")
        self.assertIn("--fail-on-error", quality.arguments)
        freshness_index = quality.arguments.index("--required-latest-ingestion-date")
        self.assertEqual("2026-10-01", quality.arguments[freshness_index + 1])

    def test_managed_orchestrator_uses_the_canonical_commands(self) -> None:
        run_root = Path("build/test-run")
        commands = build_pipeline_commands(run_root)
        managed = _tasks(run_root)
        self.assertEqual(
            [(task.name, task.dependencies, task.command) for task in managed],
            [
                (command.name, command.dependencies, command.argv())
                for command in commands
            ],
        )


if __name__ == "__main__":
    unittest.main()
