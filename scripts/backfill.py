"""
CLI helper for triggering historical backfill runs.

Usage:
    # All available months
    python scripts/backfill.py --all

    # Single month
    python scripts/backfill.py --month 2024-01

    # Range
    python scripts/backfill.py --from 2023-01 --to 2024-06

    # Dry run (list partitions without running)
    python scripts/backfill.py --from 2023-01 --to 2024-06 --dry-run

This script uses Dagster's Python API to submit backfill runs.
It must be run from within the project virtual environment.
"""

from __future__ import annotations

import argparse
import sys
from datetime import date

from rich.console import Console
from rich.table import Table

console = Console()


def _parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the backfill script."""
    parser = argparse.ArgumentParser(
        description="COMEXT pipeline historical backfill helper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--all", action="store_true", help="Backfill all available months from 2002-01 to today"
    )
    group.add_argument(
        "--month", metavar="YYYY-MM", help="Backfill a single month (e.g. 2024-01)"
    )
    group.add_argument(
        "--from",
        dest="from_month",
        metavar="YYYY-MM",
        help="Start of backfill range (inclusive)",
    )
    parser.add_argument(
        "--to",
        dest="to_month",
        metavar="YYYY-MM",
        default=None,
        help="End of backfill range (inclusive); defaults to current month",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List partitions that would be run without executing them",
    )
    return parser.parse_args()


def _month_range(start: str, end: str) -> list[str]:
    """Generate YYYY-MM-DD partition keys from YYYY-MM start to end (inclusive)."""
    sy, sm = int(start[:4]), int(start[5:])
    ey, em = int(end[:4]), int(end[5:])

    keys: list[str] = []
    y, m = sy, sm
    while (y, m) <= (ey, em):
        keys.append(f"{y:04d}-{m:02d}-01")
        m += 1
        if m > 12:
            m = 1
            y += 1
    return keys


def _current_month_key() -> str:
    """Return today's partition key in YYYY-MM-DD format (first of month)."""
    today = date.today()
    return f"{today.year:04d}-{today.month:02d}-01"


def main() -> None:
    """Run the backfill: resolve partition keys, then materialise assets for each."""
    args = _parse_args()

    # ── Determine partition keys to run ────────────────────────────────────────
    if args.all:
        partition_keys = _month_range("2002-01", _current_month_key()[:7])
    elif args.month:
        partition_keys = [f"{args.month}-01"]
    else:
        to_key = f"{args.to_month}-01" if args.to_month else _current_month_key()
        partition_keys = _month_range(args.from_month, to_key[:7])

    console.print(
        f"\n[bold]COMEXT Backfill[/bold] — {len(partition_keys)} partition(s) selected\n"
    )

    if args.dry_run:
        table = Table("Partition", "Period", title="Dry Run — Partitions to process")
        for key in partition_keys:
            period = key[:4] + key[5:7]
            table.add_row(key, period)
        console.print(table)
        console.print("\n[yellow]Dry run — no jobs submitted.[/yellow]")
        return

    # ── Submit via Dagster Python API ──────────────────────────────────────────
    try:
        from dagster import DagsterInstance, materialize_to_memory
        from comext_pipeline import defs
    except ImportError as e:
        console.print(f"[red]Import error: {e}[/red]")
        console.print("Make sure you have installed the package: pip install -e '.[dev]'")
        sys.exit(1)

    instance = DagsterInstance.ephemeral()
    for key in partition_keys:
        console.print(f"  Materialising partition: [cyan]{key}[/cyan]")
        result = materialize_to_memory(
            assets=list(defs.assets),
            resources=dict(defs.resources),
            partition_key=key,
            instance=instance,
        )
        status = "[green]✓[/green]" if result.success else "[red]✗[/red]"
        console.print(f"  {status} {key}")

    console.print("\n[bold green]Backfill complete.[/bold green]")


if __name__ == "__main__":
    main()