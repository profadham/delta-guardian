# 🛡️ Delta Guardian

**Automated Maintenance & Cost-Optimization Daemon for Delta Lake Tables**

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-1.6+-orange.svg)](https://delta.io/)
[![Rich CLI](https://img.shields.io/badge/UI-Rich-brightgreen.svg)](https://github.com/Textualize/rich)
[![License: MIT](https://img.shields.io/badge/License-MIT-purple.svg)](LICENSE)

Delta Guardian is a lightweight, zero-data-loading maintenance daemon and CLI tool designed to inspect, score, and auto-heal Delta Lake tables. It solves the notorious **"Small File Problem"** and cleans up dead tombstoned files by analyzing Delta transaction commit logs (`_delta_log/*.json`) without reading heavy Parquet data into memory.

---

## ⚡ The Problem: The "Small File Problem" in Lakehouses

In modern real-time streaming architectures (e.g., Kafka micro-batches, IoT ingestion, Change Data Capture), Delta tables often accumulate hundreds or thousands of tiny Parquet files (< 100KB each).

```
               Typical Streaming Ingestion Pattern:
Kafka / Kinesis ──▶ Micro-Batch Stream ──▶ Appends every 5s
                                               │
                                 ┌─────────────┴─────────────┐
                                 ▼                           ▼
                        part-00001 (4 KB)           part-00002 (3 KB) ... [1,000s of tiny files]
```

### Why Tiny Files Kill Lakehouse Economics:
1. **Query Planning Degradation:** Analytical engines (Trino, Spark, DuckDB, Databricks) must open, read, and decode the footer of every individual Parquet file. 1,000 tiny files can turn a 200ms query into a 30-second crawl.
2. **Cloud Storage Cost Amplification:** S3, Azure Data Lake (ADLS), and Google Cloud Storage (GCS) charge per 1,000 `GET`/`LIST` API requests. Querying 50,000 tiny files 100 times a day costs **5,000,000 HTTP requests/day** for negligible volume.
3. **Dead Storage Leaks:** Updates and deletions in Delta Lake use Copy-on-Write / Merge-on-Read, leaving tombstoned historical Parquet files on disk until a `VACUUM` is executed.

---

## 🚀 Key Features

- 🏎️ **Zero-Data-Loading Log Inspector (`src/inspector.py`):** Parses `_delta_log/*.json` commit records directly in milliseconds, tracking active vs. tombstoned files, byte volumes, and size distributions without touching Parquet data.
- 🧠 **Heuristic Decision Engine (`src/engine.py`):** Calculates an objective Health Score (0-100) and grade (`EXCELLENT`, `GOOD`, `WARNING`, `CRITICAL`), triggering `OPTIMIZE` when small files exceed thresholds (default 30%) and `VACUUM` when dead storage accumulates.
- 🔒 **Safe Concurrency Executor (`src/executor.py`):** Safely runs `compact()` and `vacuum()` with Optimistic Concurrency Control (OCC) conflict detection, exponential backoff retries, and dry-run forecasting.
- 🎨 **Rich Terminal Dashboards (`src/ui.py`):** Formatted ASCII health meters, lakehouse size distribution tiers, before/after maintenance comparison cards, and action flags.
- 🤖 **Self-Healing Daemon Mode:** Continuously monitors tables at configured intervals and automatically runs compaction and pruning when health scores drop.
- 🧪 **Comprehensive Test Suite:** 14 automated unit and integration tests covering commit parsing, heuristic math, executor safety, and CLI commands.

---

## 🏗️ Architecture

```
                                 ┌────────────────────────┐
                                 │     CLI Entrypoint     │
                                 │       (main.py)        │
                                 └───────────┬────────────┘
                                             │
               ┌─────────────────────────────┼─────────────────────────────┐
               ▼                             ▼                             ▼
    ┌──────────────────────┐      ┌──────────────────────┐      ┌──────────────────────┐
    │     generator.py     │      │     inspector.py     │      │     executor.py      │
    │  • IoT Mock Batches  │      │  • Parse _delta_log  │      │  • Optimistic Lock   │
    │  • Tiny Parquet Files│      │  • Active / Dead Files│     │  • table.compact()   │
    │  • Churn Simulation  │      │  • Zero-Data I/O     │      │  • table.vacuum()    │
    └──────────────────────┘      └──────────┬───────────┘      └──────────────────────┘
                                             │ TableMetrics
                                             ▼
                                  ┌──────────────────────┐
                                  │      engine.py       │
                                  │  • Score: 0 - 100    │
                                  │  • OPTIMIZE / VACUUM │
                                  │  • Cost Projections  │
                                  └──────────┬───────────┘
                                             │ HealthReport
                                             ▼
                                  ┌──────────────────────┐
                                  │        ui.py         │
                                  │  • Rich Dashboard    │
                                  │  • Size Distribution │
                                  │  • Audit Summary     │
                                  └──────────────────────┘
```

### Module Breakdown

| Module | Responsibility |
|---|---|
| [`src/generator.py`](file:///d:/delta-guardian/src/generator.py) | Generates mock Delta tables with hundreds of tiny files (<100KB) and simulated update churn. |
| [`src/inspector.py`](file:///d:/delta-guardian/src/inspector.py) | Inspects `_delta_log/*.json` commit records in numerical order to compute sizes, file counts, and tombstones. |
| [`src/engine.py`](file:///d:/delta-guardian/src/engine.py) | Evaluates small-file ratios, tiny-file penalties, fragmentation indices, and tombstone accumulation. |
| [`src/executor.py`](file:///d:/delta-guardian/src/executor.py) | Safely performs `compact()` and `vacuum()` with OCC conflict retries, audit tracking, and dry-run forecasting. |
| [`src/ui.py`](file:///d:/delta-guardian/src/ui.py) | Renders terminal dashboards, ASCII health meters, size histograms, and audit comparison cards. |
| [`main.py`](file:///d:/delta-guardian/main.py) | Click CLI providing `generate`, `scan`, `optimize`, `daemon`, and `history` commands. |

---

## 📦 Installation & Setup

### Prerequisites
- Python 3.10 or higher
- PowerShell, Bash, or standard command shell

### Quick Setup

```bash
# Clone or navigate to the repository
cd delta-guardian

# (Recommended) Create and activate a virtual environment
python -m venv .venv
# On Windows:
.venv\Scripts\activate
# On Linux/macOS:
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

---

## 🛠️ Usage & Workflow

### 1. Generate a Messy Test Table
Create a realistic table populated with 100 tiny Parquet files (< 100KB each) and simulated churn:

```bash
python main.py generate --files 100 --rows-per-file 50
```

*Output:*
```text
✓ Successfully generated messy Delta table!
• Active Data Files: 81
• Historical Tombstoned Files: 20
• Current Table Version: v100
• Total Physical Size: 855.79 KB
• Generation Time: 4.85s
```

---

### 2. Scan Table Health (`scan`)
Inspect the table using the zero-data-loading log parser and display the terminal dashboard:

```bash
python main.py scan
```

*Dashboard preview:*
- **Health Score Gauge:** `[████░░░░░░░░░░░░░░░░] 19/100 (CRITICAL)`
- **Storage Layout:** 81 active files, average file size: 6.74 KB (100% < 16MB)
- **Tombstones:** 20 historical files occupying 124.69 KB (19.0% dead storage)
- **Size Distribution:** 100% of files in `< 128 KB` tier
- **Summary Flags:**
  - `⚡ OPTIMIZE NEEDED: Compaction recommended to fix small files`
  - `🧹 VACUUM NEEDED: Dead files can be purged to reclaim disk space`

#### Machine-Readable JSON Export
For CI/CD pipelines, Databricks jobs, or alerting systems:
```bash
python main.py scan --json-output
```

---

### 3. Safely Optimize & Prune Table (`optimize`)

#### Dry-Run Mode (Preview changes without touching disk)
```bash
python main.py optimize --dry-run
```

#### Live Automated Maintenance
Runs file compaction (bin-packing to target size) and vacuum pruning:
```bash
python main.py optimize --target-size-mb 128 --retention-hours 0
```

*Execution Summary:*
```text
Delta Table Maintenance Execution Summary
┌──────────────────────┬───────────┬──────────────────┬───────────────────────┐
│ Metric / Phase       │    Before │            After │       Net Improvement │
├──────────────────────┼───────────┼──────────────────┼───────────────────────┤
│ Active File Count    │        81 │                1 │      -80 files (-98.8%)│
│ Active Storage       │ 533.03 KB │        187.69 KB │ Consolidated into 1   │
│ Footprint            │           │                  │ bin-packed file       │
│ Table Version        │      v100 │             v103 │ +3 new commit(s)      │
│ Dead Files Pruned    │         - │ 20 files removed │ 124.69 KB reclaimed   │
│ Execution Duration   │         - │     0.32 seconds │ Dry Run: False        │
└──────────────────────┴───────────┴──────────────────┴───────────────────────┘
```

After re-running `python main.py scan`, the table achieves a score of **100/100 (EXCELLENT)**.

---

### 4. Continuous Self-Healing Daemon Mode (`daemon`)
Run Delta Guardian as a background daemon that periodically checks table health and automatically triggers maintenance when the score drops below a configured threshold:

```bash
python main.py daemon --interval 30 --min-health-score 70
```

*Daemon Output:*
```text
[2026-10-27 04:11:40] Scan #1: Score=19/100 (CRITICAL) Files=81 SmallFiles=100.0% Tombstones=20
  ⚡ Health score (19) fell below threshold (70). Triggering automated maintenance...
  ✓ Auto-heal complete! Active files: 81 -> 1. Vacuumed: 20 dead files (124.69 KB reclaimed).
[2026-10-27 04:12:10] Scan #2: Score=100/100 (EXCELLENT) Files=1 SmallFiles=100.0% Tombstones=0
```

---

### 5. Inspect Transaction Log Commit History (`history`)
Extract the audit trail directly from `_delta_log/*.json`:

```bash
python main.py history --limit 10
```

---

## 📊 Heuristic Decision Engine Rules

| Heuristic Rule | Threshold | Penalty | Trigger Action |
|---|---|---|---|
| **Small File Ratio** | `> 30%` files `< 16MB` | Up to 35 pts | `OPTIMIZE` |
| **Tiny Files Severity** | `> 15%` files `< 1MB` | Up to 25 pts | `OPTIMIZE` |
| **Volume Imbalance** | Actual Files / Ideal `(Volume / 128MB) > 5x` | Up to 15 pts | `OPTIMIZE` |
| **Tombstone Accumulation** | `> 15%` dead storage or `> 10` dead files on disk | Up to 25 pts | `VACUUM` |

### Health Score Grading
- 🟢 **90 – 100 (EXCELLENT):** Optimal storage layout and zero dead storage.
- 🔵 **75 – 89 (GOOD):** Minor fragmentation; query throughput unaffected.
- 🟡 **50 – 74 (WARNING):** Elevated listing overhead and sub-optimal row grouping.
- 🔴 **0 – 49 (CRITICAL):** Severe small file problem and/or significant storage leak.

---

## 🧪 Running Tests

The test suite validates inspector accuracy, heuristic decision paths, executor concurrency handling, and CLI integrations:

```bash
python -m pytest -v
```

*Test Results:*
```text
tests/test_cli.py::TestDeltaGuardianCLI::test_cli_generate_and_scan PASSED
tests/test_cli.py::TestDeltaGuardianCLI::test_cli_optimize_and_history PASSED
tests/test_cli.py::TestDeltaGuardianCLI::test_cli_version PASSED
tests/test_engine.py::TestHeuristicEngine::test_healthy_table_score PASSED
tests/test_engine.py::TestHeuristicEngine::test_invalid_table_score PASSED
tests/test_engine.py::TestHeuristicEngine::test_small_file_problem_flags_optimize PASSED
tests/test_engine.py::TestHeuristicEngine::test_tombstone_accumulation_flags_vacuum PASSED
tests/test_executor.py::TestSafeDeltaExecutor::test_dry_run_does_not_modify PASSED
tests/test_executor.py::TestSafeDeltaExecutor::test_live_compact_and_vacuum PASSED
tests/test_executor.py::TestSafeDeltaExecutor::test_preflight_invalid_path PASSED
tests/test_inspector.py::TestDeltaLogInspector::test_format_bytes PASSED
tests/test_inspector.py::TestDeltaLogInspector::test_invalid_table_detection PASSED
tests/test_inspector.py::TestDeltaLogInspector::test_multiple_commits_and_churn PASSED
tests/test_inspector.py::TestDeltaLogInspector::test_single_commit_metrics PASSED
============================== 14 passed in 2.08s ==============================
```

---

## 📄 License

MIT License. Designed and crafted for high-performance lakehouse data engineering.
