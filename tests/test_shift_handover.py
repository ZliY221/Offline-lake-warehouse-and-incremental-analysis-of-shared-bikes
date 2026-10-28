import unittest

from bike_lakehouse.shift_handover import HandoverWorkItem, build_shift_handover


class ShiftHandoverTests(unittest.TestCase):
    def test_handover_prioritises_blocked_and_stale_work(self) -> None:
        result = build_shift_handover([
            HandoverWorkItem("quality-1", "QUALITY", "OPEN", "data", "2026-10-28T08:30:00+08:00", "q:1", "复核质量规则", 20),
            HandoverWorkItem("source-1", "SOURCE", "BLOCKED", "ingestion", "2026-10-28T06:00:00+08:00", "s:1", "补齐来源分区", 30, True),
            HandoverWorkItem("pipe-1", "PIPELINE", "IN_PROGRESS", "platform", "2026-10-28T09:40:00+08:00", "p:1", "继续失败任务", 15),
            HandoverWorkItem("done-1", "DELIVERY", "RESOLVED", "release", "2026-10-28T09:50:00+08:00", "d:1", "无需操作", 5),
        ], generated_at="2026-10-28T10:00:00+08:00", focus_limit=2, stale_after_minutes=120)
        self.assertEqual("HANDOVER_ATTENTION", result["status"])
        self.assertEqual("source-1", result["focus_items"][0]["item_id"])
        self.assertEqual(3, result["unfinished_count"])
        self.assertEqual(1, result["remaining_count"])
        self.assertFalse(result["work_started"])

    def test_clean_handover_is_ready(self) -> None:
        result = build_shift_handover([
            HandoverWorkItem("delivery-1", "DELIVERY", "OPEN", "release", "2026-10-28T09:30:00+08:00", "d:1", "登记交付", 10),
        ], generated_at="2026-10-28T10:00:00+08:00")
        self.assertEqual("HANDOVER_READY", result["status"])

    def test_duplicate_item_is_rejected(self) -> None:
        item = HandoverWorkItem("same", "PIPELINE", "OPEN", "data", "2026-10-28T09:00:00+08:00", "p:1", "继续任务", 10)
        with self.assertRaises(ValueError):
            build_shift_handover([item, item], generated_at="2026-10-28T10:00:00+08:00")
