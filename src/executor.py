"""
Delta Guardian: Safe Execution Wrapper
Safely executes Delta table maintenance operations (OPTIMIZE compaction and VACUUM cleanup)
with optimistic concurrency conflict handling, validation checks, dry-run support,
and before/after audit reporting.
"""

from __future__ import annotations

import json
import os
import random
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from deltalake import DeltaTable
from deltalake.exceptions import DeltaError, CommitFailedError, TableNotFoundError

from src.inspector import DeltaLogInspector, TableMetrics, format_bytes


@dataclass
class OperationAudit:
    """Audit record for a single maintenance phase (compact or vacuum)."""
    operation_name: str
    status: str  # 'SUCCESS', 'SKIPPED', 'FAILED'
    duration_seconds: float
    details: Dict[str, Any] = field(default_factory=dict)
    error_message: Optional[str] = None


@dataclass
class ExecutionResult:
    """Complete summary of maintenance actions executed on the Delta table."""
    table_path: str
    dry_run: bool
    success: bool
    version_before: int
    version_after: int
    
    # Active files comparison
    active_files_before: int
    active_files_after: int
    files_compacted_count: int
    files_added_count: int
    
    # Storage comparison
    active_bytes_before: int
    active_bytes_after: int
    storage_reclaimed_bytes: int
    formatted_storage_reclaimed: str
    
    # Tombstone cleanup
    vacuumed_files_count: int
    vacuumed_files: List[str] = field(default_factory=list)
    
    # Execution metrics
    total_duration_seconds: float = 0.0
    audits: List[OperationAudit] = field(default_factory=list)
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert result to serializable dictionary."""
        return {
            "table_path": self.table_path,
            "dry_run": self.dry_run,
            "success": self.success,
            "version_before": self.version_before,
            "version_after": self.version_after,
            "active_files_before": self.active_files_before,
            "active_files_after": self.active_files_after,
            "files_compacted_count": self.files_compacted_count,
            "files_added_count": self.files_added_count,
            "active_bytes_before": self.active_bytes_before,
            "active_bytes_after": self.active_bytes_after,
            "storage_reclaimed_bytes": self.storage_reclaimed_bytes,
            "formatted_storage_reclaimed": self.formatted_storage_reclaimed,
            "vacuumed_files_count": self.vacuumed_files_count,
            "vacuumed_files": self.vacuumed_files,
            "total_duration_seconds": round(self.total_duration_seconds, 2),
            "audits": [
                {
                    "operation": a.operation_name,
                    "status": a.status,
                    "duration": round(a.duration_seconds, 2),
                    "details": a.details,
                    "error": a.error_message,
                }
                for a in self.audits
            ],
            "error": self.error,
        }


class SafeDeltaExecutor:
    """Safe execution manager for Delta Lake table maintenance.
    
    Handles:
    - Pre-flight table validation and lock detection.
    - Optimistic concurrency conflict detection and retries.
    - Dry-run forecasting.
    - Compaction (OPTIMIZE) with custom target file size.
    - Vacuuming (VACUUM) with retention overrides for local/automated maintenance.
    - Accurate before-and-after storage and file count tracking.
    """

    def __init__(
        self,
        table_path: str,
        max_retries: int = 3,
        retry_backoff_base_sec: float = 1.0,
    ) -> None:
        self.table_path = Path(table_path).resolve()
        self.max_retries = max_retries
        self.retry_backoff_base_sec = retry_backoff_base_sec

    def run_maintenance(
        self,
        do_compact: bool = True,
        do_vacuum: bool = True,
        target_size_mb: float = 128.0,
        retention_hours: int = 0,
        enforce_retention: bool = False,
        dry_run: bool = False,
    ) -> ExecutionResult:
        """Executes compaction and/or vacuuming with safety guarantees.
        
        Args:
            do_compact: Whether to run file compaction.
            do_vacuum: Whether to run vacuum to delete unreferenced tombstone files.
            target_size_mb: Target file size for compaction in Megabytes (default: 128 MB).
            retention_hours: Retention duration for VACUUM in hours (default: 0 for immediate local cleanup).
            enforce_retention: If False, allows retention < 168 hours (standard Delta safety check).
            dry_run: If True, simulates operations and reports impact without mutating data.
            
        Returns:
            ExecutionResult containing complete audit trails and storage metrics.
        """
        start_overall = time.time()
        audits: List[OperationAudit] = []

        # Step 1: Pre-flight validation
        if not self.table_path.exists():
            return self._failed_result(
                f"Directory does not exist: {self.table_path}",
                start_overall,
            )

        inspector = DeltaLogInspector(str(self.table_path))
        if not inspector.is_delta_table():
            return self._failed_result(
                f"Not a valid Delta Lake table: {self.table_path} (no _delta_log directory found)",
                start_overall,
            )

        # Measure baseline before execution
        metrics_before = inspector.inspect()
        version_before = metrics_before.current_version
        active_files_before = metrics_before.active_file_count
        active_bytes_before = metrics_before.active_total_bytes
        disk_bytes_before = metrics_before.total_physical_bytes

        files_compacted_count = 0
        files_added_count = 0
        vacuumed_files: List[str] = []
        target_size_bytes = int(target_size_mb * 1024 * 1024)

        # Step 2: Compaction Phase (OPTIMIZE)
        if do_compact:
            compact_audit = self._execute_compact_with_retry(
                target_size_bytes=target_size_bytes,
                dry_run=dry_run,
                metrics_before=metrics_before,
            )
            audits.append(compact_audit)
            if compact_audit.status == "SUCCESS":
                files_compacted_count = compact_audit.details.get("numFilesRemoved", 0)
                files_added_count = compact_audit.details.get("numFilesAdded", 0)

        # Step 3: Vacuum Phase (VACUUM)
        if do_vacuum:
            vacuum_audit = self._execute_vacuum_with_retry(
                retention_hours=retention_hours,
                enforce_retention=enforce_retention,
                dry_run=dry_run,
            )
            audits.append(vacuum_audit)
            if vacuum_audit.status == "SUCCESS":
                vacuumed_files = vacuum_audit.details.get("vacuumed_files", [])

        # Measure state after execution
        metrics_after = inspector.inspect()
        version_after = metrics_after.current_version
        active_files_after = metrics_after.active_file_count
        active_bytes_after = metrics_after.active_total_bytes
        disk_bytes_after = metrics_after.total_physical_bytes

        # Reclaimed storage: difference in total physical footprint or vacuumed file sizes
        storage_reclaimed = max(0, disk_bytes_before - disk_bytes_after)
        if storage_reclaimed == 0 and vacuumed_files:
            # Estimate from tombstoned bytes cleaned
            storage_reclaimed = max(0, metrics_before.tombstoned_files_on_disk_bytes - metrics_after.tombstoned_files_on_disk_bytes)

        total_elapsed = time.time() - start_overall
        has_failure = any(a.status == "FAILED" for a in audits)

        return ExecutionResult(
            table_path=str(self.table_path),
            dry_run=dry_run,
            success=not has_failure,
            version_before=version_before,
            version_after=version_after,
            active_files_before=active_files_before,
            active_files_after=active_files_after,
            files_compacted_count=files_compacted_count,
            files_added_count=files_added_count,
            active_bytes_before=active_bytes_before,
            active_bytes_after=active_bytes_after,
            storage_reclaimed_bytes=storage_reclaimed,
            formatted_storage_reclaimed=format_bytes(storage_reclaimed),
            vacuumed_files_count=len(vacuumed_files),
            vacuumed_files=vacuumed_files,
            total_duration_seconds=total_elapsed,
            audits=audits,
            error=None if not has_failure else "; ".join(a.error_message or "" for a in audits if a.error_message),
        )

    def _execute_compact_with_retry(
        self,
        target_size_bytes: int,
        dry_run: bool,
        metrics_before: TableMetrics,
    ) -> OperationAudit:
        """Runs compaction with optimistic concurrency retry handling."""
        start_t = time.time()
        
        if dry_run:
            ideal_files = max(1, metrics_before.active_total_bytes // target_size_bytes + (1 if metrics_before.active_total_bytes % target_size_bytes else 0))
            return OperationAudit(
                operation_name="OPTIMIZE_COMPACT (DRY-RUN)",
                status="SUCCESS",
                duration_seconds=time.time() - start_t,
                details={
                    "dry_run": True,
                    "target_size_bytes": target_size_bytes,
                    "candidate_files": metrics_before.active_file_count,
                    "estimated_resulting_files": ideal_files,
                    "message": "Dry-run simulation: No table modifications performed.",
                },
            )

        attempt = 0
        last_error = None

        while attempt < self.max_retries:
            attempt += 1
            try:
                dt = DeltaTable(str(self.table_path))
                # Check for table write lock or concurrent commit
                lock_file = self.table_path / "_delta_log" / ".delta.lock"
                if lock_file.exists():
                    time.sleep(self.retry_backoff_base_sec * attempt)
                    continue

                res = dt.optimize.compact(target_size=target_size_bytes)
                
                # Normalize metrics from delta-rs compact()
                num_added = res.get("numFilesAdded", 0)
                num_removed = res.get("numFilesRemoved", 0)
                
                return OperationAudit(
                    operation_name="OPTIMIZE_COMPACT",
                    status="SUCCESS",
                    duration_seconds=time.time() - start_t,
                    details={
                        "numFilesAdded": num_added,
                        "numFilesRemoved": num_removed,
                        "raw_metrics": res,
                        "attempts": attempt,
                    },
                )
            except (CommitFailedError, DeltaError, Exception) as e:
                last_error = e
                # Exponential backoff with jitter on concurrent conflict
                backoff = (self.retry_backoff_base_sec * (2 ** (attempt - 1))) + random.uniform(0.1, 0.5)
                time.sleep(backoff)

        return OperationAudit(
            operation_name="OPTIMIZE_COMPACT",
            status="FAILED",
            duration_seconds=time.time() - start_t,
            error_message=f"Compaction failed after {self.max_retries} attempts: {last_error}",
        )

    def _execute_vacuum_with_retry(
        self,
        retention_hours: int,
        enforce_retention: bool,
        dry_run: bool,
    ) -> OperationAudit:
        """Runs VACUUM with optimistic concurrency and retry handling."""
        start_t = time.time()
        attempt = 0
        last_error = None

        while attempt < self.max_retries:
            attempt += 1
            try:
                dt = DeltaTable(str(self.table_path))
                
                # Execute vacuum
                vacuum_result = dt.vacuum(
                    retention_hours=retention_hours,
                    enforce_retention_duration=enforce_retention,
                    dry_run=dry_run,
                )
                
                file_list = list(vacuum_result) if vacuum_result else []
                
                return OperationAudit(
                    operation_name="VACUUM (DRY-RUN)" if dry_run else "VACUUM",
                    status="SUCCESS",
                    duration_seconds=time.time() - start_t,
                    details={
                        "dry_run": dry_run,
                        "retention_hours": retention_hours,
                        "enforce_retention": enforce_retention,
                        "vacuumed_files_count": len(file_list),
                        "vacuumed_files": file_list,
                        "attempts": attempt,
                    },
                )
            except (CommitFailedError, DeltaError, Exception) as e:
                last_error = e
                backoff = (self.retry_backoff_base_sec * (2 ** (attempt - 1))) + random.uniform(0.1, 0.5)
                time.sleep(backoff)

        return OperationAudit(
            operation_name="VACUUM",
            status="FAILED",
            duration_seconds=time.time() - start_t,
            error_message=f"Vacuum failed after {self.max_retries} attempts: {last_error}",
        )

    def _failed_result(self, error_msg: str, start_time: float) -> ExecutionResult:
        """Constructs an ExecutionResult for pre-flight failures."""
        return ExecutionResult(
            table_path=str(self.table_path),
            dry_run=False,
            success=False,
            version_before=0,
            version_after=0,
            active_files_before=0,
            active_files_after=0,
            files_compacted_count=0,
            files_added_count=0,
            active_bytes_before=0,
            active_bytes_after=0,
            storage_reclaimed_bytes=0,
            formatted_storage_reclaimed="0 B",
            vacuumed_files_count=0,
            vacuumed_files=[],
            total_duration_seconds=time.time() - start_time,
            audits=[],
            error=error_msg,
        )


if __name__ == "__main__":
    executor = SafeDeltaExecutor("./data/delta_lake_table")
    print("Running dry-run maintenance...")
    dry_res = executor.run_maintenance(dry_run=True)
    import pprint
    pprint.pprint(dry_res.to_dict())
