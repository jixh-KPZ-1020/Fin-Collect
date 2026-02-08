"""CLI commands for data ingestion."""

from __future__ import annotations

import datetime
from typing import Optional

import typer
from typing_extensions import Annotated

ingest_app = typer.Typer(no_args_is_help=True)


def _resolve_symbols(
    symbols: str | None,
    all_universe: bool,
    universe_symbols: list[str],
) -> list[str]:
    """Resolve symbol list from CLI args or config."""
    if all_universe:
        return universe_symbols
    if symbols:
        return [s.strip().upper() for s in symbols.split(",")]
    typer.echo("Error: Provide --symbols or --all", err=True)
    raise typer.Exit(1)


@ingest_app.command("daily")
def ingest_daily(
    source: Annotated[str, typer.Option("--source", "-s", help="Source key: 'av' or 'yf'")],
    symbols: Annotated[
        Optional[str], typer.Option("--symbols", help="Comma-separated symbols")
    ] = None,
    all_universe: Annotated[
        bool, typer.Option("--all", help="Ingest all symbols in configured universe")
    ] = False,
    start: Annotated[
        Optional[str], typer.Option("--start", help="Start date YYYY-MM-DD")
    ] = None,
    end: Annotated[
        Optional[str], typer.Option("--end", help="End date YYYY-MM-DD")
    ] = None,
) -> None:
    """Fetch daily OHLCV data from the specified source."""
    from CXq_data.config.loader import get_settings
    from CXq_data.ingestors import get_ingestor
    from CXq_data.utils.rate_limiter import RateLimiter

    settings = get_settings()
    ingestor = get_ingestor(source, settings)
    limiter = RateLimiter(ingestor.rate_limit)

    symbol_list = _resolve_symbols(symbols, all_universe, settings.universe.symbols)
    start_date = datetime.date.fromisoformat(start) if start else settings.universe.default_start
    end_date = datetime.date.fromisoformat(end) if end else datetime.date.today()

    typer.echo(f"Ingesting {len(symbol_list)} symbols from '{source}' ({start_date} to {end_date})")

    for sym in symbol_list:
        try:
            limiter.wait()
            result = ingestor.fetch_daily(sym, start_date, end_date, settings.storage.raw_dir)
            typer.echo(f"  [{result.source}] {sym}: {result.rows_received} rows -> {result.raw_path}")
        except Exception as e:
            typer.echo(f"  [ERROR] {sym}: {e}", err=True)

    typer.echo("Done.")


@ingest_app.command("intraday")
def ingest_intraday(
    source: Annotated[str, typer.Option("--source", "-s", help="Source key: 'av' or 'yf'")],
    interval: Annotated[str, typer.Option("--interval", "-i", help="e.g. 5m, 15m, 1h")] = "5m",
    symbols: Annotated[
        Optional[str], typer.Option("--symbols", help="Comma-separated symbols")
    ] = None,
    all_universe: Annotated[
        bool, typer.Option("--all", help="Ingest all symbols in configured universe")
    ] = False,
    start: Annotated[
        Optional[str], typer.Option("--start", help="Start date YYYY-MM-DD")
    ] = None,
    end: Annotated[
        Optional[str], typer.Option("--end", help="End date YYYY-MM-DD")
    ] = None,
) -> None:
    """Fetch intraday OHLCV data from the specified source."""
    from CXq_data.config.loader import get_settings
    from CXq_data.ingestors import get_ingestor
    from CXq_data.utils.rate_limiter import RateLimiter

    settings = get_settings()
    ingestor = get_ingestor(source, settings)
    limiter = RateLimiter(ingestor.rate_limit)

    symbol_list = _resolve_symbols(symbols, all_universe, settings.universe.symbols)
    start_date = datetime.date.fromisoformat(start) if start else settings.universe.default_start
    end_date = datetime.date.fromisoformat(end) if end else datetime.date.today()

    typer.echo(
        f"Ingesting intraday ({interval}) for {len(symbol_list)} symbols "
        f"from '{source}' ({start_date} to {end_date})"
    )

    for sym in symbol_list:
        try:
            limiter.wait()
            result = ingestor.fetch_intraday(
                sym, start_date, end_date, interval, settings.storage.raw_dir
            )
            typer.echo(f"  [{result.source}] {sym}: {result.rows_received} rows -> {result.raw_path}")
        except Exception as e:
            typer.echo(f"  [ERROR] {sym}: {e}", err=True)

    typer.echo("Done.")
