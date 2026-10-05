from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from bike_lakehouse.orchestration import (
    ExecutionResult,
    PipelineOrchestrator,
    TaskSpec,
    render_orchestration_markdown,
    validate_tasks,
)


def success_result(task: TaskSpec, attempt: int) -> ExecutionResult:
    return ExecutionResult(0, f"logs/{task.name}-{attempt}.log", "a" * 64)


class OrchestrationTests(unittest.TestCase):
    def test_retry_preserves_dependency_order_and_attempt_evidence(self) -> None:
        calls: list[tuple[str, int]] = []

        def executor(task: TaskSpec, attempt: int) -> ExecutionResult:
            calls.append((task.name, attempt))
            exit_code = 75 if task.name == "silver" and attempt == 1 else 0
            return ExecutionResult(
                exit_code,
                f"logs/{task.name}-{attempt}.log",
                str(attempt) * 64,
            )

        with tempfile.TemporaryDirectory() as directory:
            orchestrator = PipelineOrchestrator(
                pipeline_name="test-pipeline",
                run_id="retry-run",
                tasks=[
                    TaskSpec("bronze", (), ("bronze",)),
                    TaskSpec("silver", ("bronze",), ("silver",)),
                    TaskSpec("gold", ("silver",), ("gold",)),
                ],
                state_path=Path(directory) / "state.json",
                executor=executor,
            )
            state = orchestrator.run()
        self.assertEqual(state["status"], "SUCCEEDED")
        self.assertEqual(
            calls,
            [("bronze", 1), ("silver", 1), ("silver", 2), ("gold", 1)],
        )
        self.assertEqual(state["tasks"]["silver"]["attempts"], 2)
        self.assertEqual(
            [attempt["exit_code"] for attempt in state["tasks"]["silver"]["attempt_history"]],
            [75, 0],
        )
        self.assertIn(
            "TASK_RETRY_SCHEDULED",
            [event["event"] for event in state["events"]],
        )
        markdown = render_orchestration_markdown(state)
        self.assertIn("| silver | bronze | SUCCEEDED | 2 |", markdown)

    def test_resume_skips_completed_tasks_and_checks_graph_fingerprint(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            state_path = Path(directory) / "state.json"
            tasks = [TaskSpec("only", (), ("only",))]
            PipelineOrchestrator(
                pipeline_name="test-pipeline",
                run_id="resume-run",
                tasks=tasks,
                state_path=state_path,
                executor=success_result,
            ).run()
            calls: list[str] = []

            def must_not_run(task: TaskSpec, attempt: int) -> ExecutionResult:
                calls.append(task.name)
                return success_result(task, attempt)

            state = PipelineOrchestrator(
                pipeline_name="test-pipeline",
                run_id="resume-run",
                tasks=tasks,
                state_path=state_path,
                executor=must_not_run,
            ).run(resume=True)
        self.assertEqual(calls, [])
        self.assertEqual(state["resume_count"], 1)
        self.assertEqual(state["status"], "SUCCEEDED")
        self.assertEqual(state["events"][-2]["event"], "TASK_RESUME_SKIPPED")

    def test_invalid_dependency_graphs_are_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "missing dependencies"):
            validate_tasks([TaskSpec("silver", ("bronze",), ("silver",))])
        with self.assertRaisesRegex(ValueError, "cycle"):
            validate_tasks(
                [
                    TaskSpec("one", ("two",), ("one",)),
                    TaskSpec("two", ("one",), ("two",)),
                ]
            )


if __name__ == "__main__":
    unittest.main()
