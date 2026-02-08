"""CLI commands for data validation."""

from __future__ import annotations

from typing import Optional

import typer
from rich.console import Console
from rich.table import Table
from typing_extensions import Annotated

from CXq_data.validation.models import CheckStatus

validate_app = typer.Typer(no_args_is_help=True)
console = Console()

STATUS_STYLES = {
    CheckStatus.PASS: "green",
    CheckStatus.WARN: "yellow",
    CheckStatus.FAIL: "red",
}


@validate_app.command("run")
def validate_run(
    symbols: Annotated[
        Optional[str], typer.Option("--symbols", help="Comma-separated symbols")
    ] = None,
    all_universe: Annotated[
        bool, typer.Option("--all", help="Validate all symbols in universe")
    ] = False,
) -> None:
    """Run data quality checks on processed data."""
    from CXq_data.config.loader import get_settings
    from CXq_data.storage.duckdb_manager import DuckDBManager
    from CXq_data.validation.runner import run_all_checks

    settings = get_settings()

    if all_universe:
        symbol_list = settings.universe.symbols
    elif symbols:
        symbol_list = [s.strip().upper() for s in symbols.split(",")]
    else:
        typer.echo("Error: Provide --symbols or --all", err=True)
        raise typer.Exit(1)

    manager = DuckDBManager(settings.storage.duckdb_path, settings.storage.processed_dir)

    with manager.connect() as db:
        db.create_views()

        for symbol in symbol_list:
            try:
                df = db.to_polars(
                    f"SELECT * FROM daily_ohlcv WHERE symbol = '{symbol}' ORDER BY date"
                )
            except Exception:
                typer.echo(f"\n{symbol}: No data found (daily_ohlcv view may not exist)")
                continue

            report = run_all_checks(df, symbol)
            _print_report(report)


@validate_app.command("report")
def validate_report(
    all_universe: Annotated[
        bool, typer.Option("--all", help="Full report for all symbols")
    ] = True,
) -> None:
    """Generate a summary validation report for all symbols."""
    from CXq_data.config.loader import get_settings
    from CXq_data.storage.duckdb_manager import DuckDBManager
    from CXq_data.validation.runner import run_all_checks

    settings = get_settings()
    manager = DuckDBManager(settings.storage.duckdb_path, settings.storage.processed_dir)

    summary_table = Table(title="Validation Summary", show_header=True, header_style="bold")
    summary_table.add_column("Symbol")
    summary_table.add_column("Status")
    summary_table.add_column("Checks Passed")
    summary_table.add_column("Issues")

    with manager.connect() as db:
        db.create_views()

        for symbol in settings.universe.symbols:
            try:
                df = db.to_polars(
                    f"SELECT * FROM daily_ohlcv WHERE symbol = '{symbol}' ORDER BY date"
                )
            except Exception:
                summary_table.add_row(symbol, "[red]NO DATA[/red]", "-", "-")
                continue

            report = run_all_checks(df, symbol)
            passed = sum(1 for r in report.results if r.status == CheckStatus.PASS)
            total = len(report.results)
            issues = [r.check_name for r in report.results if r.status != CheckStatus.PASS]

            style = STATUS_STYLES.get(report.overall_status, "white")
            summary_table.add_row(
                symbol,
                f"[{style}]{report.overall_status.value.upper()}[/{style}]",
                f"{passed}/{total}",
                ", ".join(issues) if issues else "-",
            )

    console.print(summary_table)


def _print_report(report) -> None:
    """Print a CheckReport as a formatted Rich table."""
    style = STATUS_STYLES.get(report.overall_status, "white")
    console.print(f"\n[bold]{report.symbol}[/bold] — [{style}]{report.overall_status.value.upper()}[/{style}]")

    table = Table(show_header=True, header_style="bold", padding=(0, 1))
    table.add_column("Check")
    table.add_column("Status")
    table.add_column("Message")

    for result in report.results:
        s = STATUS_STYLES.get(result.status, "white")
        table.add_row(
            result.check_name,
            f"[{s}]{result.status.value}[/{s}]",
            result.message,
        )

    console.print(table)
