"""
Delta Guardian: Rich Terminal UI
Renders terminal dashboards, ASCII health meters, size distribution histograms,
and before-and-after maintenance comparison cards using Rich.
"""

from __future__ import annotations

import math
import sys
from typing import Any, Dict, List, Optional

if sys.platform == "win32":
    try:
        if sys.stdout and hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8")
        if sys.stderr and hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

from rich.align import Align
from rich.box import DOUBLE_EDGE, HEAVY_EDGE, ROUNDED, SIMPLE
from rich.columns import Columns
from rich.console import Console, Group
from rich.panel import Panel
from rich.style import Style
from rich.table import Table
from rich.text import Text

from src.engine import HealthGrade, HealthReport, Severity
from src.executor import ExecutionResult
from src.inspector import TableMetrics, format_bytes

console = Console()


BANNER_ART = r"""
  ___       _ _          _____                     _ _             
 |   \  ___| | |_ __ _  / ____|_  _ __ _ _ _ __| (_)__ _ _ _  
 | |) |/ -_) |  _/ _` | | (_ | || / _` | '_/ _` | |/ _` | ' \ 
 |___/ \___|_|\__\__,_|  \___|\_,_\__,_|_| \__,_|_|\__,_|_||_|
"""


def render_banner() -> None:
    """Renders the Delta Guardian application banner."""
    header_text = Text()
    header_text.append("DELTA GUARDIAN", style="bold cyan")
    header_text.append(" v1.0.0", style="dim")
    header_text.append("  •  Automated Delta Lake Maintenance & Cost Daemon", style="bold white")
    
    panel = Panel(
        Align.center(header_text),
        box=ROUNDED,
        border_style="cyan",
        subtitle="[dim]Zero-Data-Loading Log Inspector  •  Heuristic Optimizer  •  VACUUM Pruner[/dim]",
        subtitle_align="center",
    )
    console.print(panel)


def render_gauge(score: int, grade: HealthGrade) -> Text:
    """Generates an ASCII bar gauge colored by severity."""
    total_blocks = 20
    filled_blocks = int(round((score / 100.0) * total_blocks))
    empty_blocks = total_blocks - filled_blocks

    if grade == HealthGrade.EXCELLENT:
        color = "bold green"
        badge = "[bold green]EXCELLENT[/]"
    elif grade == HealthGrade.GOOD:
        color = "bold cyan"
        badge = "[bold cyan]GOOD[/]"
    elif grade == HealthGrade.WARNING:
        color = "bold yellow"
        badge = "[bold yellow]WARNING[/]"
    else:
        color = "bold red"
        badge = "[bold red]CRITICAL[/]"

    gauge = Text()
    gauge.append("  [", style="bold white")
    gauge.append("█" * filled_blocks, style=color)
    gauge.append("░" * empty_blocks, style="dim white")
    gauge.append(f"]  {score}/100  ", style="bold white")
    
    return gauge


def display_dashboard(report: HealthReport) -> None:
    """Displays the full Rich scan dashboard."""
    render_banner()

    metrics = report.metrics

    # Health Overview Panel
    gauge = render_gauge(report.score, report.grade)
    
    grade_style = {
        HealthGrade.EXCELLENT: "green",
        HealthGrade.GOOD: "cyan",
        HealthGrade.WARNING: "yellow",
        HealthGrade.CRITICAL: "red",
    }.get(report.grade, "white")

    overview_text = Text()
    overview_text.append("Health Score: ", style="bold")
    overview_text.append_text(gauge)
    overview_text.append(f" Status: ", style="bold")
    overview_text.append(f" {report.grade.value} ", style=f"bold white on {grade_style}")
    overview_text.append("\nTarget Location: ", style="dim")
    overview_text.append(f"{metrics.table_path}", style="bold white")
    overview_text.append(f"  •  Current Version: ", style="dim")
    overview_text.append(f"v{metrics.current_version}", style="bold cyan")
    overview_text.append(f"  •  Total Commits: ", style="dim")
    overview_text.append(f"{metrics.total_commits}", style="bold white")

    console.print(
        Panel(
            overview_text,
            title="[bold cyan]Table Health Assessment[/bold cyan]",
            box=ROUNDED,
            border_style=grade_style,
        )
    )

    # Key Metrics Side-by-Side Tables
    metrics_table = Table(box=ROUNDED, title="[bold]Storage & Active File Layout[/bold]", expand=True)
    metrics_table.add_column("Metric", style="dim")
    metrics_table.add_column("Value", justify="right", style="bold white")
    metrics_table.add_column("Assessment", style="dim")

    metrics_table.add_row(
        "Active Data Files",
        str(metrics.active_file_count),
        "[red]High file fragmentation[/red]" if metrics.active_file_count > 50 else "[green]Reasonable[/green]",
    )
    metrics_table.add_row(
        "Active Storage Size",
        format_bytes(metrics.active_total_bytes),
        f"Avg file: {format_bytes(metrics.active_avg_bytes)}",
    )
    
    small_file_color = "green" if metrics.active_file_count <= 1 else ("red" if metrics.small_file_pct > 30 else "green")
    small_file_assessment = "Optimal (Single file)" if metrics.active_file_count <= 1 else ("Compaction required" if metrics.small_file_pct > 30 else "Healthy")
    metrics_table.add_row(
        "Small Files (< 16 MB)",
        f"{metrics.small_file_count} ({metrics.small_file_pct:.1f}%)",
        f"[{small_file_color}]{small_file_assessment}[/{small_file_color}]",
    )

    tiny_file_color = "green" if metrics.active_file_count <= 1 else ("red" if metrics.tiny_file_pct > 15 else "green")
    tiny_file_assessment = "Optimal (Single file)" if metrics.active_file_count <= 1 else ("Severe read penalty" if metrics.tiny_file_pct > 15 else "Healthy")
    metrics_table.add_row(
        "Tiny Files (< 1 MB)",
        f"{metrics.tiny_file_count} ({metrics.tiny_file_pct:.1f}%)",
        f"[{tiny_file_color}]{tiny_file_assessment}[/{tiny_file_color}]",
    )

    metrics_table.add_row(
        "File Size Range",
        f"{format_bytes(metrics.active_min_bytes)} – {format_bytes(metrics.active_max_bytes)}",
        "Min / Max Active Files",
    )

    retention_table = Table(box=ROUNDED, title="[bold]Historical Tombstones & Retention[/bold]", expand=True)
    retention_table.add_column("Metric", style="dim")
    retention_table.add_column("Value", justify="right", style="bold white")
    retention_table.add_column("Assessment", style="dim")

    tomb_color = "yellow" if metrics.tombstoned_file_count > 0 else "green"
    retention_table.add_row(
        "Tombstoned Files (Log)",
        str(metrics.tombstoned_file_count),
        f"[{tomb_color}]Historical removed files[/{tomb_color}]",
    )
    retention_table.add_row(
        "Dead Storage Footprint",
        format_bytes(metrics.tombstoned_total_bytes),
        f"{metrics.tombstoned_ratio_pct:.1f}% of total table volume",
    )
    retention_table.add_row(
        "Physical Dead Files on Disk",
        f"{metrics.tombstoned_files_on_disk_count} ({format_bytes(metrics.tombstoned_files_on_disk_bytes)})",
        "[yellow]Reclaimable via VACUUM[/yellow]" if metrics.tombstoned_files_on_disk_count > 0 else "[green]Clean disk[/green]",
    )
    retention_table.add_row(
        "Commit Versions with Deletions",
        ", ".join(str(v) for v in metrics.unreferenced_versions[:5]) + ("..." if len(metrics.unreferenced_versions) > 5 else "") or "None",
        "Historical snapshots available",
    )
    retention_table.add_row(
        "Transaction Log Files",
        f"{metrics.log_file_count} files ({format_bytes(metrics.log_total_bytes)})",
        "Direct _delta_log/*.json overhead",
    )

    console.print(Columns([metrics_table, retention_table]))

    # Size Distribution Table
    dist_table = Table(box=ROUNDED, title="[bold]File Size Distribution (Lakehouse Tiers)[/bold]", expand=True)
    dist_table.add_column("Size Tier", style="cyan")
    dist_table.add_column("File Count", justify="right")
    dist_table.add_column("% of Files", justify="right")
    dist_table.add_column("Total Volume", justify="right")
    dist_table.add_column("% of Volume", justify="right")
    dist_table.add_column("Visual Distribution", style="dim")

    for bucket in metrics.size_distribution:
        bar_len = int(round(bucket["count_pct"] / 5.0))
        bar_str = "■" * bar_len + "·" * (20 - bar_len)
        bar_color = "red" if "<" in bucket["label"] and "128 KB" in bucket["label"] else (
            "yellow" if "1 MB" in bucket["label"] else "green"
        )
        dist_table.add_row(
            bucket["label"],
            str(bucket["count"]),
            f"{bucket['count_pct']:.1f}%",
            bucket["formatted_bytes"],
            f"{bucket['bytes_pct']:.1f}%",
            f"[{bar_color}]{bar_str}[/{bar_color}]",
        )

    console.print(dist_table)

    # Heuristic Penalties Breakdown
    if report.penalties:
        penalty_table = Table(box=ROUNDED, title="[bold]Health Penalties Breakdown[/bold]", expand=True)
        penalty_table.add_column("Severity", justify="center", width=12)
        penalty_table.add_column("Rule", style="bold")
        penalty_table.add_column("Penalty", justify="right", style="bold red")
        penalty_table.add_column("Diagnostic / Root Cause", style="dim white")

        for p in report.penalties:
            sev_badge = (
                "[bold white on red] CRITICAL [/]"
                if p.severity == Severity.CRITICAL
                else "[bold black on yellow] WARNING [/]"
            )
            penalty_table.add_row(
                sev_badge,
                p.rule_name,
                f"-{p.points_deducted:.1f} pts",
                p.reason,
            )
        console.print(penalty_table)

    # Actionable Recommendations Panel
    recs_table = Table(box=ROUNDED, title="[bold]Automated Maintenance Plan & Cost Optimization[/bold]", expand=True)
    recs_table.add_column("Action", justify="center", width=14)
    recs_table.add_column("Recommendation", style="bold white")
    recs_table.add_column("Suggested Command", style="bold cyan")
    recs_table.add_column("Expected Cost & Performance Impact", style="dim")

    for r in report.recommendations:
        if r.action == "OPTIMIZE":
            badge = "[bold white on dark_red] OPTIMIZE [/]"
        elif r.action == "VACUUM":
            badge = "[bold white on dark_orange] VACUUM [/]"
        else:
            badge = "[bold white on dark_green] HEALTHY [/]"

        recs_table.add_row(
            badge,
            f"{r.title}\n[dim]{r.description}[/dim]",
            r.suggested_command,
            r.estimated_impact,
        )

    console.print(recs_table)

    # Summary Callout
    status_summary = []
    if report.requires_optimize:
        status_summary.append("[bold red]⚡ OPTIMIZE NEEDED: Compaction recommended to fix small files[/bold red]")
    else:
        status_summary.append("[bold green]✓ OPTIMIZE: Storage layout is well compacted[/bold green]")

    if report.requires_vacuum:
        status_summary.append("[bold yellow]🧹 VACUUM NEEDED: Dead files can be purged to reclaim disk space[/bold yellow]")
    else:
        status_summary.append("[bold green]✓ VACUUM: No unreferenced dead files requiring cleanup[/bold green]")

    summary_panel = Panel(
        "\n".join(status_summary),
        title="[bold]Summary Flags[/bold]",
        box=ROUNDED,
        border_style="cyan",
    )
    console.print(summary_panel)


def display_execution_result(result: ExecutionResult) -> None:
    """Renders the before-and-after maintenance summary card."""
    render_banner()

    if not result.success:
        err_panel = Panel(
            f"[bold red]Maintenance Execution Failed[/bold red]\n{result.error}",
            box=ROUNDED,
            border_style="red",
        )
        console.print(err_panel)
        return

    # Results Comparison Table
    table = Table(box=ROUNDED, title="[bold]Delta Table Maintenance Execution Summary[/bold]", expand=True)
    table.add_column("Metric / Phase", style="cyan")
    table.add_column("Before", justify="right", style="bold white")
    table.add_column("After", justify="right", style="bold green")
    table.add_column("Net Improvement", justify="right", style="bold yellow")

    # File count diff
    file_reduction = result.active_files_before - result.active_files_after
    file_reduction_pct = (
        (file_reduction / result.active_files_before * 100.0)
        if result.active_files_before > 0
        else 0.0
    )
    table.add_row(
        "Active File Count",
        str(result.active_files_before),
        str(result.active_files_after),
        f"[green]-{file_reduction} files (-{file_reduction_pct:.1f}%)[/green]" if file_reduction > 0 else "Unchanged",
    )

    # Active storage diff
    table.add_row(
        "Active Storage Footprint",
        format_bytes(result.active_bytes_before),
        format_bytes(result.active_bytes_after),
        f"Consolidated into {result.active_files_after} bin-packed file(s)",
    )

    # Version diff
    table.add_row(
        "Table Version",
        f"v{result.version_before}",
        f"v{result.version_after}",
        f"+{result.version_after - result.version_before} new commit(s)",
    )

    # Vacuumed files
    table.add_row(
        "Dead Files Pruned (VACUUM)",
        "-",
        f"{result.vacuumed_files_count} files removed",
        f"[green]{result.formatted_storage_reclaimed} reclaimed[/green]",
    )

    table.add_row(
        "Execution Duration",
        "-",
        f"{result.total_duration_seconds:.2f} seconds",
        f"Dry Run: {result.dry_run}",
    )

    console.print(table)

    # Audit Steps breakdown
    if result.audits:
        audit_table = Table(box=ROUNDED, title="[bold]Operation Audit Log[/bold]", expand=True)
        audit_table.add_column("Operation", style="bold white")
        audit_table.add_column("Status", justify="center", width=12)
        audit_table.add_column("Duration", justify="right")
        audit_table.add_column("Details", style="dim")

        for a in result.audits:
            stat_style = "bold green" if a.status == "SUCCESS" else "bold red"
            audit_table.add_row(
                a.operation_name,
                f"[{stat_style}]{a.status}[/{stat_style}]",
                f"{a.duration_seconds:.2f}s",
                str(a.details),
            )
        console.print(audit_table)
