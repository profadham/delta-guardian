"""
Unit tests for Delta Guardian Heuristic Engine.
Tests health score computation, threshold flags for OPTIMIZE and VACUUM, and recommendations.
"""

import unittest

from src.engine import HealthGrade, HeuristicEngine, Severity
from src.inspector import TableMetrics


class TestHeuristicEngine(unittest.TestCase):

    def setUp(self):
        self.engine = HeuristicEngine(
            small_file_threshold_pct=30.0,
            tiny_file_threshold_pct=15.0,
            tombstone_ratio_threshold_pct=15.0,
        )

    def test_invalid_table_score(self):
        metrics = TableMetrics(
            table_path="/dummy/path",
            is_valid_delta_table=False,
            current_version=0,
            total_commits=0,
            active_file_count=0,
            active_total_bytes=0,
            active_avg_bytes=0.0,
            active_min_bytes=0,
            active_max_bytes=0,
            small_file_threshold_bytes=16 * 1024 * 1024,
            small_file_count=0,
            small_file_pct=0.0,
            tiny_file_threshold_bytes=1024 * 1024,
            tiny_file_count=0,
            tiny_file_pct=0.0,
            size_distribution=[],
            tombstoned_file_count=0,
            tombstoned_total_bytes=0,
            tombstoned_ratio_pct=0.0,
            tombstoned_files_on_disk_count=0,
            tombstoned_files_on_disk_bytes=0,
            unreferenced_versions=[],
            partitions=[],
        )
        report = self.engine.evaluate(metrics)
        self.assertEqual(report.score, 0)
        self.assertEqual(report.grade, HealthGrade.CRITICAL)
        self.assertFalse(report.requires_optimize)

    def test_healthy_table_score(self):
        # 2 files of 200MB each, no tombstones
        metrics = TableMetrics(
            table_path="/dummy/healthy",
            is_valid_delta_table=True,
            current_version=5,
            total_commits=5,
            active_file_count=2,
            active_total_bytes=400 * 1024 * 1024,
            active_avg_bytes=200.0 * 1024 * 1024,
            active_min_bytes=190 * 1024 * 1024,
            active_max_bytes=210 * 1024 * 1024,
            small_file_threshold_bytes=16 * 1024 * 1024,
            small_file_count=0,
            small_file_pct=0.0,
            tiny_file_threshold_bytes=1024 * 1024,
            tiny_file_count=0,
            tiny_file_pct=0.0,
            size_distribution=[],
            tombstoned_file_count=0,
            tombstoned_total_bytes=0,
            tombstoned_ratio_pct=0.0,
            tombstoned_files_on_disk_count=0,
            tombstoned_files_on_disk_bytes=0,
            unreferenced_versions=[],
            partitions=[],
        )
        report = self.engine.evaluate(metrics)
        self.assertEqual(report.score, 100)
        self.assertEqual(report.grade, HealthGrade.EXCELLENT)
        self.assertFalse(report.requires_optimize)
        self.assertFalse(report.requires_vacuum)

    def test_small_file_problem_flags_optimize(self):
        # 100 files of 50KB each
        metrics = TableMetrics(
            table_path="/dummy/messy",
            is_valid_delta_table=True,
            current_version=100,
            total_commits=100,
            active_file_count=100,
            active_total_bytes=5 * 1024 * 1024,
            active_avg_bytes=50.0 * 1024,
            active_min_bytes=40 * 1024,
            active_max_bytes=60 * 1024,
            small_file_threshold_bytes=16 * 1024 * 1024,
            small_file_count=100,
            small_file_pct=100.0,
            tiny_file_threshold_bytes=1024 * 1024,
            tiny_file_count=100,
            tiny_file_pct=100.0,
            size_distribution=[],
            tombstoned_file_count=0,
            tombstoned_total_bytes=0,
            tombstoned_ratio_pct=0.0,
            tombstoned_files_on_disk_count=0,
            tombstoned_files_on_disk_bytes=0,
            unreferenced_versions=[],
            partitions=[],
        )
        report = self.engine.evaluate(metrics)
        self.assertTrue(report.requires_optimize)
        self.assertFalse(report.requires_vacuum)
        self.assertLess(report.score, 50)
        self.assertEqual(report.grade, HealthGrade.CRITICAL)
        self.assertTrue(any(r.action == "OPTIMIZE" for r in report.recommendations))

    def test_tombstone_accumulation_flags_vacuum(self):
        # 1 active file + 30 unreferenced tombstone files on disk
        metrics = TableMetrics(
            table_path="/dummy/tombstones",
            is_valid_delta_table=True,
            current_version=35,
            total_commits=35,
            active_file_count=1,
            active_total_bytes=50 * 1024 * 1024,
            active_avg_bytes=50.0 * 1024 * 1024,
            active_min_bytes=50 * 1024 * 1024,
            active_max_bytes=50 * 1024 * 1024,
            small_file_threshold_bytes=16 * 1024 * 1024,
            small_file_count=0,
            small_file_pct=0.0,
            tiny_file_threshold_bytes=1024 * 1024,
            tiny_file_count=0,
            tiny_file_pct=0.0,
            size_distribution=[],
            tombstoned_file_count=30,
            tombstoned_total_bytes=50 * 1024 * 1024,
            tombstoned_ratio_pct=50.0,
            tombstoned_files_on_disk_count=30,
            tombstoned_files_on_disk_bytes=50 * 1024 * 1024,
            unreferenced_versions=[10, 20],
            partitions=[],
        )
        report = self.engine.evaluate(metrics)
        self.assertTrue(report.requires_vacuum)
        self.assertTrue(any(r.action == "VACUUM" for r in report.recommendations))


if __name__ == "__main__":
    unittest.main()
