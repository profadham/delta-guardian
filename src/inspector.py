"""
Delta Guardian: Metadata Inspector
Fast, zero-data-loading inspection of Delta Lake tables via direct parsing
of _delta_log/*.json commit logs. Calculates file counts, sizes, small file ratios,
and unreferenced historical/tombstoned versions.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple


def format_bytes(num_bytes: float) -> str:
    """Formats raw byte counts into human-readable strings (B, KB, MB, GB, TB)."""
    if num_bytes < 0:
        return f"-{format_bytes(-num_bytes)}"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(num_bytes) < 1024.0 or unit == "TB":
            if unit in ["B"]:
                return f"{int(num_bytes)} {unit}"
            return f"{num_bytes:.2f} {unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.2f} TB"


@dataclass
class FileInfo:
    """Metadata for an active data file."""
    path: str
    size_bytes: int
    modification_time: int
    partition_values: Dict[str, str] = field(default_factory=dict)
    commit_version: int = 0
    data_change: bool = True


@dataclass
class TombstoneInfo:
    """Metadata for a removed / tombstoned file from historical commits."""
    path: str
    size_bytes: int
    deletion_timestamp: int
    commit_version: int
    data_change: bool = True
    exists_on_disk: bool = True


@dataclass
class CommitRecord:
    """Record of a single commit transaction in the Delta log."""
    version: int
    timestamp: int
    operation: str
    engine_info: str = ""
    client_version: str = ""
    files_added: int = 0
    files_removed: int = 0
    bytes_added: int = 0
    bytes_removed: int = 0
    operation_parameters: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SizeBucket:
    """Bucket statistics for file size distribution."""
    label: str
    min_bytes: int
    max_bytes: int
    count: int = 0
    total_bytes: int = 0
    pct_of_files: float = 0.0
    pct_of_size: float = 0.0


@dataclass
class SizeDistribution:
    """Multi-tiered file size distribution breakdown."""
    buckets: List[SizeBucket] = field(default_factory=list)


@dataclass
class TableMetrics:
    """Comprehensive snapshot metrics derived purely from the Delta commit log."""
    table_path: str
    is_valid_delta_table: bool
    current_version: int
    total_commits: int
    
    # Active data files
    active_file_count: int
    active_total_bytes: int
    active_avg_bytes: float
    active_min_bytes: int
    active_max_bytes: int
    
    # Small file thresholds & metrics
    small_file_threshold_bytes: int
    small_file_count: int
    small_file_pct: float
    tiny_file_threshold_bytes: int
    tiny_file_count: int
    tiny_file_pct: float
    
    # Size distribution breakdown
    size_distribution: List[Dict[str, Any]]
    
    # Historical / Tombstoned unreferenced data
    tombstoned_file_count: int
    tombstoned_total_bytes: int
    tombstoned_ratio_pct: float
    tombstoned_files_on_disk_count: int
    tombstoned_files_on_disk_bytes: int
    unreferenced_versions: List[int]
    
    # Table metadata & protocol
    partitions: List[str]
    metadata_id: Optional[str] = None
    table_name: Optional[str] = None
    min_reader_version: int = 1
    min_writer_version: int = 2
    schema_fields: List[str] = field(default_factory=list)
    
    # Log files metadata
    log_file_count: int = 0
    log_total_bytes: int = 0
    checkpoint_versions: List[int] = field(default_factory=list)
    
    # Physical filesystem footprint
    total_physical_bytes: int = 0
    
    # History sample
    recent_commits: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to a serializable dictionary."""
        return asdict(self)


class DeltaLogInspector:
    """Inspects Delta Lake tables by reading `_delta_log/*.json` directly.
    
    Operates without loading any Parquet data into memory, guaranteeing O(commits)
    speed and minimal RAM usage even for large tables.
    """

    SMALL_FILE_16MB: int = 16 * 1024 * 1024       # 16 MB
    TINY_FILE_1MB: int = 1024 * 1024              # 1 MB

    def __init__(
        self,
        table_path: str,
        small_file_threshold_mb: float = 16.0,
        tiny_file_threshold_mb: float = 1.0,
    ) -> None:
        self.table_path = Path(table_path).resolve()
        self.log_dir = self.table_path / "_delta_log"
        self.small_file_threshold_bytes = int(small_file_threshold_mb * 1024 * 1024)
        self.tiny_file_threshold_bytes = int(tiny_file_threshold_mb * 1024 * 1024)

    def is_delta_table(self) -> bool:
        """Checks if the directory contains a valid Delta transaction log."""
        if not self.log_dir.is_dir():
            return False
        # Look for at least one commit JSON file
        for item in self.log_dir.iterdir():
            if item.name.endswith(".json") and re.match(r"^\d{20}\.json$", item.name):
                return True
        return False

    def inspect(self) -> TableMetrics:
        """Parses commit logs in sequence and computes comprehensive metrics."""
        if not self.is_delta_table():
            return self._empty_metrics(is_valid=False)

        # Collect and sort commit log files numerically
        commit_files: List[Tuple[int, Path]] = []
        checkpoint_versions: List[int] = []
        log_file_count = 0
        log_total_bytes = 0

        for entry in self.log_dir.iterdir():
            if not entry.is_file():
                continue
            log_file_count += 1
            log_total_bytes += entry.stat().st_size

            # Check for standard commit JSON: 00000000000000000000.json
            m_commit = re.match(r"^(\d{20})\.json$", entry.name)
            if m_commit:
                commit_files.append((int(m_commit.group(1)), entry))
                continue

            # Check for checkpoints: 00000000000000000010.checkpoint.parquet
            m_ckpt = re.match(r"^(\d{20})\.checkpoint.*", entry.name)
            if m_ckpt:
                checkpoint_versions.append(int(m_ckpt.group(1)))

        commit_files.sort(key=lambda x: x[0])
        checkpoint_versions.sort()

        if not commit_files:
            return self._empty_metrics(is_valid=True)

        active_files: Dict[str, FileInfo] = {}
        tombstoned_files: Dict[str, TombstoneInfo] = {}
        commits: List[CommitRecord] = []
        partitions: List[str] = []
        schema_fields: List[str] = []
        metadata_id: Optional[str] = None
        table_name: Optional[str] = None
        min_reader_ver = 1
        min_writer_ver = 2

        versions_with_tombstones: Set[int] = set()

        for version, commit_file in commit_files:
            commit_timestamp = 0
            operation = "UNKNOWN"
            engine_info = ""
            client_version = ""
            op_params: Dict[str, Any] = {}
            files_added_in_commit = 0
            files_removed_in_commit = 0
            bytes_added_in_commit = 0
            bytes_removed_in_commit = 0

            with open(commit_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    if "protocol" in record:
                        proto = record["protocol"]
                        min_reader_ver = proto.get("minReaderVersion", min_reader_ver)
                        min_writer_ver = proto.get("minWriterVersion", min_writer_ver)

                    elif "metaData" in record:
                        md = record["metaData"]
                        metadata_id = md.get("id")
                        table_name = md.get("name")
                        partitions = md.get("partitionColumns", [])
                        schema_str = md.get("schemaString")
                        if schema_str:
                            try:
                                schema_obj = json.loads(schema_str)
                                schema_fields = [
                                    f["name"] for f in schema_obj.get("fields", [])
                                ]
                            except Exception:
                                pass

                    elif "commitInfo" in record:
                        ci = record["commitInfo"]
                        commit_timestamp = ci.get("timestamp", commit_timestamp)
                        operation = ci.get("operation", operation)
                        engine_info = ci.get("engineInfo", "")
                        client_version = ci.get("clientVersion", "")
                        op_params = ci.get("operationParameters", {})

                    elif "add" in record:
                        add_entry = record["add"]
                        path = add_entry["path"]
                        size = int(add_entry.get("size", 0))
                        mod_time = int(add_entry.get("modificationTime", 0))
                        part_vals = add_entry.get("partitionValues", {})
                        data_change = add_entry.get("dataChange", True)

                        active_files[path] = FileInfo(
                            path=path,
                            size_bytes=size,
                            modification_time=mod_time,
                            partition_values=part_vals,
                            commit_version=version,
                            data_change=data_change,
                        )
                        files_added_in_commit += 1
                        bytes_added_in_commit += size

                    elif "remove" in record:
                        rem_entry = record["remove"]
                        path = rem_entry["path"]
                        del_timestamp = int(rem_entry.get("deletionTimestamp", 0))
                        data_change = rem_entry.get("dataChange", True)
                        
                        # Size might be recorded directly in remove or retrieved from active_files
                        size = int(rem_entry.get("size", 0))
                        if size == 0 and path in active_files:
                            size = active_files[path].size_bytes

                        # Check if file still physically exists on disk
                        physical_path = self.table_path / path
                        exists = physical_path.exists()

                        tombstoned_files[path] = TombstoneInfo(
                            path=path,
                            size_bytes=size,
                            deletion_timestamp=del_timestamp,
                            commit_version=version,
                            data_change=data_change,
                            exists_on_disk=exists,
                        )
                        versions_with_tombstones.add(version)

                        if path in active_files:
                            del active_files[path]

                        files_removed_in_commit += 1
                        bytes_removed_in_commit += size

            commits.append(
                CommitRecord(
                    version=version,
                    timestamp=commit_timestamp,
                    operation=operation,
                    engine_info=engine_info,
                    client_version=client_version,
                    files_added=files_added_in_commit,
                    files_removed=files_removed_in_commit,
                    bytes_added=bytes_added_in_commit,
                    bytes_removed=bytes_removed_in_commit,
                    operation_parameters=op_params,
                )
            )

        # Aggregate Active File Metrics
        active_count = len(active_files)
        active_sizes = [f.size_bytes for f in active_files.values()]
        active_total_bytes = sum(active_sizes)
        active_avg_bytes = (active_total_bytes / active_count) if active_count > 0 else 0.0
        active_min_bytes = min(active_sizes) if active_count > 0 else 0
        active_max_bytes = max(active_sizes) if active_count > 0 else 0

        # Small and Tiny File Metrics
        small_files = [s for s in active_sizes if s < self.small_file_threshold_bytes]
        small_count = len(small_files)
        small_pct = (small_count / active_count * 100.0) if active_count > 0 else 0.0

        tiny_files = [s for s in active_sizes if s < self.tiny_file_threshold_bytes]
        tiny_count = len(tiny_files)
        tiny_pct = (tiny_count / active_count * 100.0) if active_count > 0 else 0.0

        # Size Distribution Bucketing
        size_dist = self._compute_size_distribution(active_sizes, active_total_bytes)

        # Tombstone / Historical Metrics
        tombstone_count = len(tombstoned_files)
        tombstone_total_bytes = sum(t.size_bytes for t in tombstoned_files.values())
        total_referenced_and_tombstoned = active_total_bytes + tombstone_total_bytes
        tombstone_ratio = (
            (tombstone_total_bytes / total_referenced_and_tombstoned * 100.0)
            if total_referenced_and_tombstoned > 0
            else 0.0
        )

        # Tombstones still physically occupying disk space
        tombstones_on_disk = [t for t in tombstoned_files.values() if t.exists_on_disk]
        tombstones_on_disk_count = len(tombstones_on_disk)
        tombstones_on_disk_bytes = sum(t.size_bytes for t in tombstones_on_disk)

        # Total Physical Directory Size
        total_physical_bytes = 0
        try:
            total_physical_bytes = sum(
                f.stat().st_size
                for f in self.table_path.glob("**/*")
                if f.is_file()
            )
        except Exception:
            pass

        current_version = commit_files[-1][0] if commit_files else 0

        recent_commits_summary = [
            {
                "version": c.version,
                "operation": c.operation,
                "timestamp": c.timestamp,
                "files_added": c.files_added,
                "files_removed": c.files_removed,
                "bytes_added": c.bytes_added,
                "bytes_removed": c.bytes_removed,
            }
            for c in commits[-10:]
        ]

        return TableMetrics(
            table_path=str(self.table_path),
            is_valid_delta_table=True,
            current_version=current_version,
            total_commits=len(commit_files),
            active_file_count=active_count,
            active_total_bytes=active_total_bytes,
            active_avg_bytes=active_avg_bytes,
            active_min_bytes=active_min_bytes,
            active_max_bytes=active_max_bytes,
            small_file_threshold_bytes=self.small_file_threshold_bytes,
            small_file_count=small_count,
            small_file_pct=round(small_pct, 2),
            tiny_file_threshold_bytes=self.tiny_file_threshold_bytes,
            tiny_file_count=tiny_count,
            tiny_file_pct=round(tiny_pct, 2),
            size_distribution=size_dist,
            tombstoned_file_count=tombstone_count,
            tombstoned_total_bytes=tombstone_total_bytes,
            tombstoned_ratio_pct=round(tombstone_ratio, 2),
            tombstoned_files_on_disk_count=tombstones_on_disk_count,
            tombstoned_files_on_disk_bytes=tombstones_on_disk_bytes,
            unreferenced_versions=sorted(list(versions_with_tombstones)),
            partitions=partitions,
            metadata_id=metadata_id,
            table_name=table_name,
            min_reader_version=min_reader_ver,
            min_writer_version=min_writer_ver,
            schema_fields=schema_fields,
            log_file_count=log_file_count,
            log_total_bytes=log_total_bytes,
            checkpoint_versions=checkpoint_versions,
            total_physical_bytes=total_physical_bytes,
            recent_commits=recent_commits_summary,
        )

    def _compute_size_distribution(
        self, sizes: List[int], total_bytes: int
    ) -> List[Dict[str, Any]]:
        """Splits file sizes into standard lakehouse tiers."""
        total_files = len(sizes)
        buckets = [
            {"label": "< 128 KB", "min": 0, "max": 128 * 1024, "count": 0, "bytes": 0},
            {"label": "128 KB - 1 MB", "min": 128 * 1024, "max": 1024 * 1024, "count": 0, "bytes": 0},
            {"label": "1 MB - 16 MB", "min": 1024 * 1024, "max": 16 * 1024 * 1024, "count": 0, "bytes": 0},
            {"label": "16 MB - 128 MB", "min": 16 * 1024 * 1024, "max": 128 * 1024 * 1024, "count": 0, "bytes": 0},
            {"label": ">= 128 MB (Target)", "min": 128 * 1024 * 1024, "max": float("inf"), "count": 0, "bytes": 0},
        ]

        for s in sizes:
            for b in buckets:
                if b["min"] <= s < b["max"]:
                    b["count"] += 1
                    b["bytes"] += s
                    break

        result = []
        for b in buckets:
            count_pct = (b["count"] / total_files * 100.0) if total_files > 0 else 0.0
            bytes_pct = (b["bytes"] / total_bytes * 100.0) if total_bytes > 0 else 0.0
            result.append({
                "label": b["label"],
                "count": b["count"],
                "total_bytes": b["bytes"],
                "formatted_bytes": format_bytes(b["bytes"]),
                "count_pct": round(count_pct, 2),
                "bytes_pct": round(bytes_pct, 2),
            })
        return result

    def _empty_metrics(self, is_valid: bool) -> TableMetrics:
        """Returns empty default metrics for uninitialized tables."""
        return TableMetrics(
            table_path=str(self.table_path),
            is_valid_delta_table=is_valid,
            current_version=0,
            total_commits=0,
            active_file_count=0,
            active_total_bytes=0,
            active_avg_bytes=0.0,
            active_min_bytes=0,
            active_max_bytes=0,
            small_file_threshold_bytes=self.small_file_threshold_bytes,
            small_file_count=0,
            small_file_pct=0.0,
            tiny_file_threshold_bytes=self.tiny_file_threshold_bytes,
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


if __name__ == "__main__":
    inspector = DeltaLogInspector("./data/delta_lake_table")
    metrics = inspector.inspect()
    import pprint
    pprint.pprint(metrics.to_dict())
