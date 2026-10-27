"""
Delta Guardian: Mock Table Generator
Generates a mock Delta Lake table populated with hundreds of tiny Parquet files (< 100KB each)
to simulate real-world streaming micro-batches, IoT ingestion, and the 'small file problem'.
"""

import os
import random
import shutil
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

console = Console()

LOCATIONS = [
    "us-east-1a",
    "us-west-2b",
    "eu-west-1c",
    "ap-southeast-1a",
    "sa-east-1a",
]

DEVICE_PREFIXES = ["sensor_alpha", "sensor_beta", "telemetry_node", "iot_core"]
STATUSES = ["HEALTHY", "HEALTHY", "HEALTHY", "WARNING", "FAULT"]


def generate_mock_batch(batch_idx: int, rows: int = 50) -> pa.Table:
    """Generate a PyArrow table batch representing IoT sensor events.
    
    With ~50 rows, the resulting Parquet file is typically 2KB to 5KB,
    far below the standard lakehouse target size (128MB+).
    """
    event_ids: List[str] = [str(uuid.uuid4()) for _ in range(rows)]
    now = datetime.now(timezone.utc)
    timestamps: List[datetime] = [
        datetime.fromtimestamp(now.timestamp() - random.randint(0, 3600), tz=timezone.utc)
        for _ in range(rows)
    ]
    devices: List[str] = [
        f"{random.choice(DEVICE_PREFIXES)}_{random.randint(100, 999)}"
        for _ in range(rows)
    ]
    locations: List[str] = [random.choice(LOCATIONS) for _ in range(rows)]
    temperatures: List[float] = [
        round(random.uniform(18.5, 95.0), 2) for _ in range(rows)
    ]
    voltages: List[float] = [
        round(random.uniform(105.0, 240.0), 2) for _ in range(rows)
    ]
    statuses: List[str] = [random.choice(STATUSES) for _ in range(rows)]
    batch_ids: List[int] = [batch_idx] * rows

    schema = pa.schema([
        ("event_id", pa.string()),
        ("timestamp", pa.timestamp("ms", tz="UTC")),
        ("device_id", pa.string()),
        ("location", pa.string()),
        ("temperature", pa.float64()),
        ("voltage", pa.float64()),
        ("status", pa.string()),
        ("batch_id", pa.int64()),
    ])

    return pa.Table.from_arrays(
        [
            pa.array(event_ids, type=pa.string()),
            pa.array(timestamps, type=pa.timestamp("ms", tz="UTC")),
            pa.array(devices, type=pa.string()),
            pa.array(locations, type=pa.string()),
            pa.array(temperatures, type=pa.float64()),
            pa.array(voltages, type=pa.float64()),
            pa.array(statuses, type=pa.string()),
            pa.array(batch_ids, type=pa.int64()),
        ],
        schema=schema,
    )


def generate_delta_table(
    table_path: str = "./data/delta_lake_table",
    num_files: int = 150,
    rows_per_file: int = 50,
    clean_existing: bool = True,
    simulate_churn: bool = True,
    show_progress: bool = True,
) -> Dict[str, Any]:
    """Generates a Delta Lake table with hundreds of tiny Parquet files.
    
    Args:
        table_path: Filesystem path for the Delta table.
        num_files: Number of small Parquet files to produce via appends.
        rows_per_file: Number of records per Parquet file.
        clean_existing: If True, wipes the existing table directory first.
        simulate_churn: If True, executes deletes to produce unreferenced tombstoned files.
        show_progress: If True, displays a Rich progress bar.
        
    Returns:
        Summary statistics dictionary of the generated table.
    """
    path_obj = Path(table_path).resolve()
    
    if clean_existing and path_obj.exists():
        shutil.rmtree(path_obj, ignore_errors=True)
    
    os.makedirs(path_obj, exist_ok=True)
    start_time = time.time()

    # Determine churn split
    # If simulate_churn is enabled, write a preliminary subset of batches,
    # delete rows from them to produce tombstoned files, and then write the remaining files.
    churn_batches = min(20, max(5, num_files // 5)) if simulate_churn and num_files >= 10 else 0
    remaining_batches = num_files - churn_batches

    progress = Progress(
        SpinnerColumn(),
        TextColumn("[bold cyan]{task.description}[/]"),
        BarColumn(bar_width=40),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
        disable=not show_progress,
    )

    tombstones_created = 0

    with progress:
        task = progress.add_task("Writing micro-batch Parquet files...", total=num_files)
        
        # Step 1: Write preliminary batches (if churn enabled)
        if churn_batches > 0:
            first_batch = generate_mock_batch(batch_idx=0, rows=rows_per_file)
            write_deltalake(str(path_obj), first_batch, mode="overwrite" if clean_existing else "append")
            progress.advance(task, 1)

            for i in range(1, churn_batches):
                b = generate_mock_batch(batch_idx=i, rows=rows_per_file)
                write_deltalake(str(path_obj), b, mode="append")
                progress.advance(task, 1)

            # Trigger churn deletion to create tombstoned files
            try:
                dt_pre = DeltaTable(str(path_obj))
                files_before = set(dt_pre.file_uris())
                # Delete FAULT records to rewrite files and tombstone old parquet files
                dt_pre.delete("status = 'FAULT'")
                dt_after_del = DeltaTable(str(path_obj))
                files_after = set(dt_after_del.file_uris())
                tombstones_created = len(files_before - files_after)
            except Exception as e:
                if show_progress:
                    console.print(f"[yellow]Warning during churn: {e}[/]")

            start_idx = churn_batches
        else:
            first_batch = generate_mock_batch(batch_idx=0, rows=rows_per_file)
            write_deltalake(str(path_obj), first_batch, mode="overwrite" if clean_existing else "append")
            progress.advance(task, 1)
            start_idx = 1

        # Step 2: Write the rest of the tiny micro-batch files
        for i in range(start_idx, num_files):
            b = generate_mock_batch(batch_idx=i, rows=rows_per_file)
            write_deltalake(str(path_obj), b, mode="append")
            progress.advance(task, 1)

    # Gather table statistics
    dt_final = DeltaTable(str(path_obj))
    active_files = dt_final.file_uris()
    active_count = len(active_files)
    current_version = dt_final.version()
    
    total_physical_size = sum(
        f.stat().st_size
        for f in path_obj.glob("**/*")
        if f.is_file() and not f.name.startswith(".")
    )

    elapsed = time.time() - start_time

    return {
        "table_path": str(path_obj),
        "total_batches": num_files,
        "rows_per_batch": rows_per_file,
        "total_rows": num_files * rows_per_file,
        "active_file_count": active_count,
        "tombstones_created": tombstones_created,
        "current_version": current_version,
        "physical_size_bytes": total_physical_size,
        "elapsed_seconds": round(elapsed, 2),
    }


if __name__ == "__main__":
    summary = generate_delta_table(num_files=30, rows_per_file=50)
    print("Generation complete:", summary)
