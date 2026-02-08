"""CLI commands for querying data via DuckDB."""

from __future__ import annotations

from typing import Optional

import typer
from rich.console import Console
from rich.table import Table
from typing_extensions import Annotated

query_app = typer.Typer(no_args_is_help=True)
console = Console()


def _get_manager():
    from CXq_data.config.loader import get_settings
    from CXq_data.storage.duckdb_manager import DuckDBManager

    settings = get_settings()
    return DuckDBManager(settings.storage.duckdb_path, settings.storage.processed_dir)


@query_app.command("sql")
def query_sql(
    sql: Annotated[str, typer.Argument(help="SQL query to execute")],
    limit: Annotated[int, typer.Option("--limit", "-n", help="Max rows to display")] = 50,
    csv_out: Annotated[
        Optional[str], typer.Option("--csv", help="Export results to CSV file path")
    ] = None,
) -> None:
    """Run arbitrary SQL against DuckDB views."""
    manager = _get_manager()

    with manager.connect() as db:
        db.create_views()
        df = db.to_polars(sql)

    if csv_out:
        df.write_csv(csv_out)
        typer.echo(f"Exported {len(df)} rows to {csv_out}")
    else:
        _print_dataframe(df.head(limit))
        if len(df) > limit:
            typer.echo(f"\n... showing {limit} of {len(df)} rows. Use --limit to see more.")


@query_app.command("latest")
def query_latest(
    symbol: Annotated[str, typer.Option("--symbol", "-s", help="Ticker symbol")],
    rows: Annotated[int, typer.Option("--rows", "-n", help="Number of rows")] = 10,
) -> None:
    """Show the latest N rows for a symbol."""
    manager = _get_manager()

    sql = f"""
        SELECT * FROM daily_ohlcv
        WHERE symbol = '{symbol.upper()}'
        ORDER BY date DESC
        LIMIT {rows}
    """

    with manager.connect() as db:
        db.create_views()
        df = db.to_polars(sql)

    if df.is_empty():
        typer.echo(f"No data found for {symbol.upper()}")
    else:
        _print_dataframe(df)


@query_app.command("range")
def query_range(
    symbol: Annotated[str, typer.Option("--symbol", "-s", help="Ticker symbol")],
    start: Annotated[str, typer.Option("--start", help="Start date YYYY-MM-DD")],
    end: Annotated[str, typer.Option("--end", help="End date YYYY-MM-DD")],
    csv_out: Annotated[
        Optional[str], typer.Option("--csv", help="Export to CSV")
    ] = None,
) -> None:
    """Show data for a symbol within a date range."""
    manager = _get_manager()

    sql = f"""
        SELECT * FROM daily_ohlcv
        WHERE symbol = '{symbol.upper()}'
          AND date >= '{start}'
          AND date <= '{end}'
        ORDER BY date
    """

    with manager.connect() as db:
        db.create_views()
        df = db.to_polars(sql)

    if csv_out:
        df.write_csv(csv_out)
        typer.echo(f"Exported {len(df)} rows to {csv_out}")
    elif df.is_empty():
        typer.echo(f"No data found for {symbol.upper()} between {start} and {end}")
    else:
        _print_dataframe(df)


@query_app.command("export")
def query_export(
    sql: Annotated[str, typer.Argument(help="SQL query")],
    output: Annotated[str, typer.Option("--output", "-o", help="Output CSV file path")],
) -> None:
    """Export query results to CSV."""
    manager = _get_manager()

    with manager.connect() as db:
        db.create_views()
        df = db.to_polars(sql)

    df.write_csv(output)
    typer.echo(f"Exported {len(df)} rows to {output}")


def _print_dataframe(df) -> None:
    """Print a Polars DataFrame as a Rich table."""
    table = Table(show_header=True, header_style="bold")

    for col in df.columns:
        table.add_column(col)

    for row in df.iter_rows():
        table.add_row(*[str(v) for v in row])

    console.print(table)
