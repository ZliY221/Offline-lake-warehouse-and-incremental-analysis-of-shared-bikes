import unittest

from bike_lakehouse.operator_journey import standard_operator_journeys
from bike_lakehouse.operator_workspace import (
    build_operator_workspace,
    render_operator_workspace_html,
)


class OperatorWorkspaceTests(unittest.TestCase):
    def test_focus_queue_prioritises_blockers_and_bounds_visible_work(self):
        result = build_operator_workspace(standard_operator_journeys(), visible_limit=10)
        self.assertEqual("OPERATOR_WORKSPACE_READY", result["status"])
        self.assertEqual(100, result["journey_count"])
        self.assertEqual(10, result["visible_count"])
        self.assertEqual(90, result["remaining_count"])
        self.assertTrue(all(item["status"] == "BLOCKED" for item in result["focus_items"]))
        self.assertFalse(result["execution_started"])

    def test_workspace_exposes_resume_keys_and_standalone_html(self):
        result = build_operator_workspace(standard_operator_journeys())
        self.assertEqual(len(result["focus_items"]), len({
            item["resume_key"] for item in result["focus_items"]
        }))
        html = render_operator_workspace_html(result)
        self.assertIn("离线运行焦点队列", html)
        self.assertIn("只读计划", html)
        self.assertNotIn("<script", html)

    def test_visible_limit_is_bounded(self):
        with self.assertRaises(ValueError):
            build_operator_workspace(standard_operator_journeys(), visible_limit=0)
