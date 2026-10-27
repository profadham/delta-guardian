"""
Delta Guardian CLI: Automated Maintenance & Cost-Optimization Daemon for Delta Lake
Provides CLI commands to generate test tables, inspect commit logs, evaluate health scores,
and execute compaction/vacuuming.
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from typing import Optional

import click
from rich.console import Console

if sys.platform == "win32":
    try:
        if sys.stdout and hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(encoding="utf-8")
        if sys.stderr and hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

from src.engine import HeuristicEngine
from src.executor import SafeDeltaExecutor
from src.generator import generate_delta_table
from src.inspector import DeltaLogInspector, format_bytes
from src.ui import display_dashboard, display_execution_result, render_banner

console = Console()

DEFAULT_TABLE_PATH = "./data/delta_lake_table"



@click.group()
@click.version_option(version="1.0.0", prog_name="Delta Guardian")
def cli():
    """Delta Guardian: Automated maintenance and cost-optimization daemon for Delta Lake."""
    pass


@cli.command("generate")
@click.option(
    "--path",
    "-p",
    default=DEFAULT_TABLE_PATH,
    show_default=True,
    help="Filesystem path for the Delta table.",
)
@click.option(
    "--files",
    "-n",
    default=150,
    show_default=True,
    help="Number of tiny Parquet micro-batches to generate.",
)
@click.option(
    "--rows-per-file",
    "-r",
    default=50,
    show_default=True,
    help="Number of records per Parquet micro-batch (<100KB each).",
)
@click.option(
    "--simulate-churn/--no-churn",
    default=True,
    show_default=True,
    help="Simulate historical updates/deletions to create unreferenced tombstoned files.",
)
@click.option(
    "--clean/--no-clean",
    default=True,
    show_default=True,
    help="Wipe existing table directory before generating.",
)
def generate_cmd(path: str, files: int, rows_per_file: int, simulate_churn: bool, clean: bool):
    """Generate a mock local Delta table populated with hundreds of tiny Parquet files (<100KB each)."""
    render_banner()
    console.print(f"[bold cyan]Generating mock Delta Lake table at:[/] [white]{path}[/]")
    console.print(f"Target: [bold]{files}[/] micro-batches, [bold]{rows_per_file}[/] rows per file.")
    
    summary = generate_delta_table(
        table_path=path,
        num_files=files,
        rows_per_file=rows_per_file,
        clean_existing=clean,
        simulate_churn=simulate_churn,
        show_progress=True,
    )

    console.print()
    console.print("[bold green]✓ Successfully generated messy Delta table![/bold green]")
    console.print(f"• Active Data Files: [bold]{summary['active_file_count']}[/]")
    console.print(f"• Historical Tombstoned Files: [bold yellow]{summary['tombstones_created']}[/]")
    console.print(f"• Current Table Version: [bold cyan]v{summary['current_version']}[/]")
    console.print(f"• Total Physical Size: [bold]{format_bytes(summary['physical_size_bytes'])}[/]")
    console.print(f"• Generation Time: [dim]{summary['elapsed_seconds']}s[/dim]")
    console.print("\nRun [bold cyan]python main.py scan[/bold cyan] to inspect table health!")


@cli.command("scan")
@click.option(
    "--path",
    "-p",
    default=DEFAULT_TABLE_PATH,
    show_default=True,
    help="Filesystem path for the Delta table.",
)
@click.option(
    "--threshold-small-file-pct",
    default=30.0,
    show_default=True,
    help="Threshold percentage of files < 16MB before flagging OPTIMIZE.",
)
@click.option(
    "--threshold-size-mb",
    default=16.0,
    show_default=True,
    help="Small file definition threshold in Megabytes.",
)
@click.option(
    "--json-output",
    is_flag=True,
    help="Output report as machine-readable JSON for CI/CD.",
)
def scan_cmd(path: str, threshold_small_file_pct: float, threshold_size_mb: float, json_output: bool):
    """Inspect Delta table commit logs and print a rich health dashboard."""
    inspector = DeltaLogInspector(
        table_path=path,
        small_file_threshold_mb=threshold_size_mb,
    )

    metrics = inspector.inspect()
    engine = HeuristicEngine(
        small_file_threshold_pct=threshold_small_file_pct,
    )
    report = engine.evaluate(metrics)

    if json_output:
        click.echo(json.dumps(report.to_dict(), indent=2))
        return

    display_dashboard(report)


@cli.command("optimize")
@click.option(
    "--path",
    "-p",
    default=DEFAULT_TABLE_PATH,
    show_default=True,
    help="Filesystem path for the Delta table.",
)
@click.option(
    "--target-size-mb",
    default=128.0,
    show_default=True,
    help="Target compacted file size in Megabytes.",
)
@click.option(
    "--compact/--no-compact",
    default=True,
    show_default=True,
    help="Execute file compaction (OPTIMIZE).",
)
@click.option(
    "--vacuum/--no-vacuum",
    default=True,
    show_default=True,
    help="Execute dead tombstone purging (VACUUM).",
)
@click.option(
    "--retention-hours",
    default=0,
    show_default=True,
    help="Retention period in hours for VACUUM (0 for immediate demo cleanup).",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Simulate compaction and vacuum without modifying files.",
)
@click.option(
    "--json-output",
    is_flag=True,
    help="Output results as JSON.",
)
def optimize_cmd(
    path: str,
    target_size_mb: float,
    compact: bool,
    vacuum: bool,
    retention_hours: int,
    dry_run: bool,
    json_output: bool,
):
    """Automatically run compaction (OPTIMIZE) and vacuum cleanup, outputting storage saved."""
    executor = SafeDeltaExecutor(table_path=path)
    
    result = executor.run_maintenance(
        do_compact=compact,
        do_vacuum=vacuum,
        target_size_mb=target_size_mb,
        retention_hours=retention_hours,
        enforce_retention=False if retention_hours == 0 else True,
        dry_run=dry_run,
    )

    if json_output:
        click.echo(json.dumps(result.to_dict(), indent=2))
        return

    display_execution_result(result)


@cli.command("daemon")
@click.option(
    "--path",
    "-p",
    default=DEFAULT_TABLE_PATH,
    show_default=True,
    help="Filesystem path for the Delta table to monitor.",
)
@click.option(
    "--interval",
    "-i",
    default=10,
    show_default=True,
    help="Check interval in seconds.",
)
@click.option(
    "--min-health-score",
    default=70,
    show_default=True,
    help="Trigger auto-healing maintenance when health score drops below this value.",
)
@click.option(
    "--max-iterations",
    default=None,
    type=int,
    help="Max check iterations before exiting (useful for testing or batch cron jobs).",
)
def daemon_cmd(path: str, interval: int, min_health_score: int, max_iterations: Optional[int]):
    """Run Delta Guardian as an automated continuous maintenance and self-healing daemon."""
    render_banner()
    console.print(f"[bold cyan]Starting Delta Guardian Daemon[/bold cyan]")
    console.print(f"• Monitoring Table: [bold white]{path}[/bold white]")
    console.print(f"• Poll Interval: [bold]{interval}s[/bold]")
    console.print(f"• Auto-Heal Score Threshold: [bold yellow]< {min_health_score}[/bold yellow]")
    console.print("[dim]Press Ctrl+C to stop the daemon.[/dim]\n")

    iteration = 0
    executor = SafeDeltaExecutor(table_path=path)

    try:
        while True:
            iteration += 1
            inspector = DeltaLogInspector(table_path=path)
            metrics = inspector.inspect()
            engine = HeuristicEngine()
            report = engine.evaluate(metrics)

            timestamp_str = time.strftime("%Y-%m-%d %H:%M:%S")
            console.print(
                f"[{timestamp_str}] [bold]Scan #{iteration}:[/bold] "
                f"Score=[bold]{report.score}/100[/bold] ({report.grade.value})  "
                f"Files=[bold]{metrics.active_file_count}[/bold]  "
                f"SmallFiles={metrics.small_file_pct:.1f}%  "
                f"Tombstones={metrics.tombstoned_files_on_disk_count}"
            )

            if report.score < min_health_score and (report.requires_optimize or report.requires_vacuum):
                console.print(
                    f"  [bold yellow]⚡ Health score ({report.score}) fell below threshold ({min_health_score}). "
                    f"Triggering automated maintenance...[/bold yellow]"
                )
                res = executor.run_maintenance(
                    do_compact=report.requires_optimize,
                    do_vacuum=report.requires_vacuum,
                    retention_hours=0,
                    enforce_retention=False,
                    dry_run=False,
                )
                if res.success:
                    console.print(
                        f"  [bold green]✓ Auto-heal complete![/bold green] "
                        f"Active files: {res.active_files_before} -> {res.active_files_after}. "
                        f"Vacuumed: {res.vacuumed_files_count} dead files ({res.formatted_storage_reclaimed} reclaimed)."
                    )
                else:
                    console.print(f"  [bold red]✗ Auto-heal maintenance failed: {res.error}[/bold red]")

            if max_iterations is not None and iteration >= max_iterations:
                console.print(f"\n[green]Completed {iteration} iterations. Daemon shutting down.[/green]")
                break

            time.sleep(interval)
    except KeyboardInterrupt:
        console.print("\n[yellow]Delta Guardian Daemon stopped by user.[/yellow]")


@cli.command("history")
@click.option(
    "--path",
    "-p",
    default=DEFAULT_TABLE_PATH,
    show_default=True,
    help="Filesystem path for the Delta table.",
)
@click.option(
    "--limit",
    "-l",
    default=15,
    show_default=True,
    help="Number of recent commits to display.",
)
def history_cmd(path: str, limit: int):
    """View transaction commit history extracted directly from _delta_log/*.json."""
    inspector = DeltaLogInspector(table_path=path)
    metrics = inspector.inspect()

    render_banner()
    console.print(f"[bold cyan]Commit History for:[/] [white]{path}[/white] (Total Commits: {metrics.total_commits})\n")

    from rich.box import ROUNDED
    from rich.table import Table
    table = Table(box=ROUNDED, expand=True)
    table.add_column("Version", justify="right", style="cyan")
    table.add_column("Operation", style="bold white")
    table.add_column("Files Added", justify="right", style="green")
    table.add_column("Files Removed", justify="right", style="yellow")
    table.add_column("Bytes Added", justify="right")
    table.add_column("Bytes Removed", justify="right")
    table.add_column("Timestamp", style="dim")

    for c in metrics.recent_commits[-limit:]:
        ts_str = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(c["timestamp"] / 1000)) if c["timestamp"] else "-"
        table.add_row(
            f"v{c['version']}",
            c["operation"],
            f"+{c['files_added']}" if c["files_added"] else "-",
            f"-{c['files_removed']}" if c["files_removed"] else "-",
            format_bytes(c["bytes_added"]) if c["bytes_added"] else "-",
            format_bytes(c["bytes_removed"]) if c["bytes_removed"] else "-",
            ts_str,
        )

    console.print(table)


if __name__ == "__main__":
    cli()
