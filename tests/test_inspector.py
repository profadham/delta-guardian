"""
Unit tests for Delta Guardian Inspector.
Tests parsing of _delta_log/*.json commit logs without loading Parquet data.
"""

import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
from deltalake import write_deltalake, DeltaTable

from src.inspector import DeltaLogInspector, format_bytes


class TestDeltaLogInspector(unittest.TestCase):

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(prefix="delta_guard_test_")
        self.table_path = Path(self.test_dir) / "table"

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_invalid_table_detection(self):
        inspector = DeltaLogInspector(str(self.table_path))
        self.assertFalse(inspector.is_delta_table())
        metrics = inspector.inspect()
        self.assertFalse(metrics.is_valid_delta_table)
        self.assertEqual(metrics.active_file_count, 0)
        self.assertEqual(metrics.total_commits, 0)

    def test_single_commit_metrics(self):
        data = pa.table({"col_a": [1, 2, 3], "col_b": ["x", "y", "z"]})
        write_deltalake(str(self.table_path), data, mode="overwrite")

        inspector = DeltaLogInspector(str(self.table_path))
        self.assertTrue(inspector.is_delta_table())

        metrics = inspector.inspect()
        self.assertTrue(metrics.is_valid_delta_table)
        self.assertEqual(metrics.current_version, 0)
        self.assertEqual(metrics.total_commits, 1)
        self.assertEqual(metrics.active_file_count, 1)
        self.assertGreater(metrics.active_total_bytes, 0)
        self.assertEqual(metrics.small_file_count, 1)
        self.assertEqual(metrics.small_file_pct, 100.0)
        self.assertEqual(metrics.tombstoned_file_count, 0)

    def test_multiple_commits_and_churn(self):
        for i in range(5):
            data = pa.table({"id": [i, i + 10], "tag": ["active", "delete_me" if i == 0 else "keep"]})
            write_deltalake(str(self.table_path), data, mode="append")

        # Now delete from the table to produce tombstones
        dt = DeltaTable(str(self.table_path))
        dt.delete("tag = 'delete_me'")

        inspector = DeltaLogInspector(str(self.table_path))
        metrics = inspector.inspect()

        self.assertGreaterEqual(metrics.total_commits, 6)
        self.assertGreater(metrics.tombstoned_file_count, 0)
        self.assertIn(metrics.current_version, metrics.unreferenced_versions or [metrics.current_version])
        self.assertGreaterEqual(metrics.active_file_count, 1)

    def test_format_bytes(self):
        self.assertEqual(format_bytes(500), "500 B")
        self.assertEqual(format_bytes(1024), "1.00 KB")
        self.assertEqual(format_bytes(1024 * 1024), "1.00 MB")
        self.assertEqual(format_bytes(1024 * 1024 * 1024 * 2.5), "2.50 GB")


if __name__ == "__main__":
    unittest.main()
