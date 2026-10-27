"""
End-to-end integration tests for Delta Guardian CLI commands.
Tests 'generate', 'scan', 'optimize', and 'history' via CliRunner.
"""

import json
import shutil
import tempfile
import unittest
from pathlib import Path

from click.testing import CliRunner

from main import cli


class TestDeltaGuardianCLI(unittest.TestCase):

    def setUp(self):
        self.runner = CliRunner()
        self.test_dir = tempfile.mkdtemp(prefix="delta_guard_cli_")
        self.table_path = Path(self.test_dir) / "test_table"

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_cli_version(self):
        result = self.runner.invoke(cli, ["--version"])
        self.assertEqual(result.exit_code, 0)
        self.assertIn("Delta Guardian", result.output)

    def test_cli_generate_and_scan(self):
        # 1. Generate messy table
        gen_result = self.runner.invoke(
            cli,
            ["generate", "--path", str(self.table_path), "--files", "15", "--rows-per-file", "20"],
        )
        self.assertEqual(gen_result.exit_code, 0)
        self.assertIn("Successfully generated", gen_result.output)

        # 2. Scan table with JSON output
        scan_json = self.runner.invoke(
            cli,
            ["scan", "--path", str(self.table_path), "--json-output"],
        )
        self.assertEqual(scan_json.exit_code, 0)
        data = json.loads(scan_json.output)
        self.assertIn("score", data)
        self.assertIn("metrics", data)
        self.assertGreater(data["metrics"]["active_file_count"], 1)

        # 3. Scan table with Rich dashboard output
        scan_dash = self.runner.invoke(
            cli,
            ["scan", "--path", str(self.table_path)],
        )
        self.assertEqual(scan_dash.exit_code, 0)
        self.assertIn("Health Score", scan_dash.output)

    def test_cli_optimize_and_history(self):
        # Generate table
        self.runner.invoke(
            cli,
            ["generate", "--path", str(self.table_path), "--files", "10", "--rows-per-file", "20"],
        )

        # Optimize dry-run
        dry_res = self.runner.invoke(
            cli,
            ["optimize", "--path", str(self.table_path), "--dry-run"],
        )
        self.assertEqual(dry_res.exit_code, 0)
        self.assertIn("Dry Run: True", dry_res.output)

        # Optimize live
        opt_res = self.runner.invoke(
            cli,
            ["optimize", "--path", str(self.table_path), "--retention-hours", "0"],
        )
        self.assertEqual(opt_res.exit_code, 0)
        self.assertIn("Maintenance Execution Summary", opt_res.output)

        # Check history
        hist_res = self.runner.invoke(
            cli,
            ["history", "--path", str(self.table_path), "--limit", "5"],
        )
        self.assertEqual(hist_res.exit_code, 0)
        self.assertIn("Commit History for", hist_res.output)


if __name__ == "__main__":
    unittest.main()
