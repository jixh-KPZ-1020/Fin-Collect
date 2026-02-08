"""CLI commands for cross-validating data across sources."""

from __future__ import annotations

import itertools
from typing import Optional

import polars as pl
import typer
from rich.console import Console
from rich.table import Table
from typing_extensions import Annotated

crossvalidate_app = typer.Typer(no_args_is_help=True)
console = Console()

# Sources that don't provide real adjusted close
_NO_ADJ_CLOSE_SOURCES = {"stooq"}


def _get_manager():
    from CXq_data.config.loader import get_settings
    from CXq_data.storage.duckdb_manager import DuckDBManager

    settings = get_settings()
    return DuckDBManager(settings.storage.duckdb_path, settings.storage.processed_dir)


@crossvalidate_app.command("compare")
def crossvalidate_compare(
    symbol: Annotated[str, typer.Option("--symbol", "-s", help="Ticker symbol")],
    sources: Annotated[str, typer.Option("--sources", help="Comma-separated source names (e.g. yfinance,stooq)")],
    start: Annotated[Optional[str], typer.Option("--start", help="Start date YYYY-MM-DD")] = None,
    end: Annotated[Optional[str], typer.Option("--end", help="End date YYYY-MM-DD")] = None,
    tolerance: Annotated[float, typer.Option("--tolerance", "-t", help="% threshold for price discrepancies")] = 1.0,
) -> None:
    """Compare daily OHLCV data across multiple sources for a symbol."""
    source_list = [s.strip() for s in sources.split(",")]
    if len(source_list) < 2:
        typer.echo("Error: Need at least 2 sources to compare", err=True)
        raise typer.Exit(1)

    symbol = symbol.upper()
    manager = _get_manager()

    with manager.connect() as db:
        db.create_views()

        # Fetch data per source
        dfs: dict[str, pl.DataFrame] = {}
        for source in source_list:
            date_filter = ""
            if start:
                date_filter += f" AND date >= '{start}'"
            if end:
                date_filter += f" AND date <= '{end}'"

            sql = f"""
                SELECT date, open, high, low, close, adjusted_close, volume
                FROM daily_ohlcv
                WHERE symbol = '{symbol}' AND source = '{source}'{date_filter}
                ORDER BY date
            """
            df = db.to_polars(sql)
            dfs[source] = df

    # Print coverage summary
    console.print(f"\n[bold]Cross-Validation: {symbol}[/bold]")

    coverage_table = Table(title="Coverage", show_header=True, header_style="bold")
    coverage_table.add_column("Source")
    coverage_table.add_column("Rows", justify="right")
    coverage_table.add_column("Date Range")

    for source, df in dfs.items():
        if df.is_empty():
            coverage_table.add_row(source, "0", "No data")
        else:
            min_d = df["date"].min()
            max_d = df["date"].max()
            coverage_table.add_row(source, str(len(df)), f"{min_d} to {max_d}")

    console.print(coverage_table)

    # Pairwise comparison
    for src_a, src_b in itertools.combinations(source_list, 2):
        df_a = dfs.get(src_a, pl.DataFrame())
        df_b = dfs.get(src_b, pl.DataFrame())

        if df_a.is_empty() or df_b.is_empty():
            console.print(f"\n[yellow]Skipping {src_a} vs {src_b}: one or both have no data[/yellow]")
            continue

        _print_pairwise_comparison(src_a, df_a, src_b, df_b, tolerance)


@crossvalidate_app.command("matrix")
def crossvalidate_matrix(
    sources: Annotated[str, typer.Option("--sources", help="Comma-separated source names")],
    symbols: Annotated[Optional[str], typer.Option("--symbols", help="Comma-separated symbols")] = None,
    all_universe: Annotated[bool, typer.Option("--all", help="All symbols in universe")] = False,
) -> None:
    """Show a summary matrix of data overlap across sources for multiple symbols."""
    from CXq_data.config.loader import get_settings

    source_list = [s.strip() for s in sources.split(",")]
    if len(source_list) < 2:
        typer.echo("Error: Need at least 2 sources", err=True)
        raise typer.Exit(1)

    settings = get_settings()
    if all_universe:
        symbol_list = settings.universe.symbols
    elif symbols:
        symbol_list = [s.strip().upper() for s in symbols.split(",")]
    else:
        typer.echo("Error: Provide --symbols or --all", err=True)
        raise typer.Exit(1)

    manager = _get_manager()

    table = Table(title="Cross-Validation Matrix", show_header=True, header_style="bold")
    table.add_column("Symbol")
    for source in source_list:
        table.add_column(f"{source} rows", justify="right")
    table.add_column("Date Overlap", justify="right")
    table.add_column("Max Close Diff %", justify="right")

    with manager.connect() as db:
        db.create_views()

        for symbol in symbol_list:
            row_counts = []
            source_dfs = []

            for source in source_list:
                sql = f"""
                    SELECT date, close
                    FROM daily_ohlcv
                    WHERE symbol = '{symbol}' AND source = '{source}'
                    ORDER BY date
                """
                try:
                    df = db.to_polars(sql)
                except Exception:
                    df = pl.DataFrame()

                row_counts.append(str(len(df)))
                source_dfs.append(df)

            # Compute overlap and max diff between first two sources
            overlap_pct = "-"
            max_diff = "-"

            if len(source_dfs) >= 2 and not source_dfs[0].is_empty() and not source_dfs[1].is_empty():
                dates_a = set(source_dfs[0]["date"].to_list())
                dates_b = set(source_dfs[1]["date"].to_list())
                common = dates_a & dates_b
                total = dates_a | dates_b

                if total:
                    overlap_pct = f"{len(common) / len(total) * 100:.1f}%"

                if common:
                    joined = source_dfs[0].join(source_dfs[1], on="date", suffix="_b")
                    diffs = (
                        (joined["close"] - joined["close_b"]).abs()
                        / joined["close"]
                        * 100
                    )
                    max_diff = f"{diffs.max():.3f}%"

            table.add_row(symbol, *row_counts, overlap_pct, max_diff)

    console.print(table)


def _print_pairwise_comparison(
    src_a: str,
    df_a: pl.DataFrame,
    src_b: str,
    df_b: pl.DataFrame,
    tolerance: float,
) -> None:
    """Print detailed pairwise comparison between two source DataFrames."""
    console.print(f"\n[bold]{src_a} vs {src_b}[/bold]")

    dates_a = set(df_a["date"].to_list())
    dates_b = set(df_b["date"].to_list())
    common = dates_a & dates_b
    only_a = dates_a - dates_b
    only_b = dates_b - dates_a

    # Join on date
    joined = df_a.join(df_b, on="date", suffix=f"_{src_b}")

    # Determine which price columns to compare
    compare_cols = ["open", "high", "low", "close"]
    skip_adj = src_a in _NO_ADJ_CLOSE_SOURCES or src_b in _NO_ADJ_CLOSE_SOURCES
    if skip_adj:
        console.print(
            f"  [dim]Note: adjusted_close comparison skipped "
            f"({'Stooq' if 'stooq' in (src_a, src_b) else 'source'} does not provide adjusted close)[/dim]"
        )

    # Compute % differences
    diff_cols = {}
    for col in compare_cols:
        col_b = f"{col}_{src_b}"
        if col in joined.columns and col_b in joined.columns:
            diff = (joined[col] - joined[col_b]).abs() / joined[col] * 100
            diff_cols[col] = diff

    # Summary table
    summary = Table(show_header=True, header_style="bold", padding=(0, 1))
    summary.add_column("Metric")
    summary.add_column("Value", justify="right")

    summary.add_row("Common dates", str(len(common)))
    summary.add_row(f"Only in {src_a}", str(len(only_a)))
    summary.add_row(f"Only in {src_b}", str(len(only_b)))

    for col, diffs in diff_cols.items():
        max_d = diffs.max()
        avg_d = diffs.mean()
        style = "[red]" if max_d and max_d > tolerance else "[green]"
        summary.add_row(f"Max {col} diff %", f"{style}{max_d:.4f}%[/]" if max_d else "-")
        summary.add_row(f"Avg {col} diff %", f"{avg_d:.4f}%" if avg_d else "-")

    # Count discrepancies above tolerance
    if diff_cols:
        any_exceeds = None
        for diffs in diff_cols.values():
            mask = diffs > tolerance
            if any_exceeds is None:
                any_exceeds = mask
            else:
                any_exceeds = any_exceeds | mask

        if any_exceeds is not None:
            exceed_count = any_exceeds.sum()
            style = "[red]" if exceed_count > 0 else "[green]"
            summary.add_row(
                f"Dates > {tolerance}% diff",
                f"{style}{exceed_count}[/]",
            )

    console.print(summary)
