"""
Unit and integration tests for Delta Guardian SafeDeltaExecutor.
Tests safe execution of compaction (OPTIMIZE), dead file cleanup (VACUUM), and dry-run mode.
"""

import os
import shutil
import tempfile
import unittest
from pathlib import Path

import pyarrow as pa
from deltalake import write_deltalake, DeltaTable

from src.executor import SafeDeltaExecutor
from src.inspector import DeltaLogInspector


class TestSafeDeltaExecutor(unittest.TestCase):

    def setUp(self):
        self.test_dir = tempfile.mkdtemp(prefix="delta_guard_exec_")
        self.table_path = Path(self.test_dir) / "table"

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_preflight_invalid_path(self):
        executor = SafeDeltaExecutor(str(Path(self.test_dir) / "non_existent"))
        result = executor.run_maintenance()
        self.assertFalse(result.success)
        self.assertIn("does not exist", result.error or "")

    def test_dry_run_does_not_modify(self):
        # Create small files table
        for i in range(5):
            data = pa.table({"col": [i, i * 10]})
            write_deltalake(str(self.table_path), data, mode="append")

        dt_before = DeltaTable(str(self.table_path))
        ver_before = dt_before.version()
        files_before = len(dt_before.file_uris())

        executor = SafeDeltaExecutor(str(self.table_path))
        res = executor.run_maintenance(dry_run=True, retention_hours=0)

        self.assertTrue(res.success)
        self.assertTrue(res.dry_run)

        dt_after = DeltaTable(str(self.table_path))
        self.assertEqual(dt_after.version(), ver_before)
        self.assertEqual(len(dt_after.file_uris()), files_before)

    def test_live_compact_and_vacuum(self):
        # Generate 10 small files
        for i in range(10):
            data = pa.table({"val": [i], "text": ["test"]})
            write_deltalake(str(self.table_path), data, mode="append")

        dt = DeltaTable(str(self.table_path))
        self.assertEqual(len(dt.file_uris()), 10)

        executor = SafeDeltaExecutor(str(self.table_path))
        result = executor.run_maintenance(
            do_compact=True,
            do_vacuum=True,
            retention_hours=0,
            enforce_retention=False,
            dry_run=False,
        )

        self.assertTrue(result.success)
        self.assertEqual(result.active_files_after, 1)
        self.assertGreater(result.version_after, result.version_before)

        # Inspect table afterwards
        inspector = DeltaLogInspector(str(self.table_path))
        metrics = inspector.inspect()
        self.assertEqual(metrics.active_file_count, 1)
        self.assertEqual(metrics.tombstoned_files_on_disk_count, 0)


if __name__ == "__main__":
    unittest.main()
