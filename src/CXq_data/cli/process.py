"""CLI commands for processing raw data into Parquet."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer
from typing_extensions import Annotated

process_app = typer.Typer(no_args_is_help=True)


def _find_raw_files(raw_dir: Path, source: str, symbols: list[str]) -> list[tuple[str, Path]]:
    """Find the most recent raw file for each symbol from a given source."""
    results = []
    source_dir = raw_dir / source

    if not source_dir.exists():
        typer.echo(f"No raw data directory for source '{source}'", err=True)
        return results

    for symbol in symbols:
        symbol_dir = source_dir / symbol
        if not symbol_dir.exists():
            typer.echo(f"  No raw data for {symbol} from {source}", err=True)
            continue

        # Find the most recent daily CSV/JSON file
        files = sorted(symbol_dir.glob("*_daily.*"), reverse=True)
        # Filter out metadata files
        files = [f for f in files if not f.name.endswith(".meta.json")]

        if files:
            results.append((symbol, files[0]))
        else:
            typer.echo(f"  No raw daily files for {symbol}", err=True)

    return results


@process_app.command("run")
def process_run(
    source: Annotated[str, typer.Option("--source", "-s", help="Source name: 'yfinance' or 'alpha_vantage'")],
    symbols: Annotated[
        Optional[str], typer.Option("--symbols", help="Comma-separated symbols")
    ] = None,
    all_universe: Annotated[
        bool, typer.Option("--all", help="Process all symbols in configured universe")
    ] = False,
) -> None:
    """Process raw files into Hive-partitioned Parquet."""
    from CXq_data.config.loader import get_settings
    from CXq_data.processing.normalizer import normalize
    from CXq_data.processing.partitioner import write_partitioned

    settings = get_settings()

    if all_universe:
        symbol_list = settings.universe.symbols
    elif symbols:
        symbol_list = [s.strip().upper() for s in symbols.split(",")]
    else:
        typer.echo("Error: Provide --symbols or --all", err=True)
        raise typer.Exit(1)

    raw_files = _find_raw_files(settings.storage.raw_dir, source, symbol_list)
    if not raw_files:
        typer.echo("No raw files found to process.")
        raise typer.Exit(1)

    typer.echo(f"Processing {len(raw_files)} symbol(s) from '{source}'")

    total_written = 0
    for symbol, raw_path in raw_files:
        try:
            df = normalize(source, raw_path, symbol)
            paths = write_partitioned(df, settings.storage.processed_dir)
            total_written += len(paths)
            for p in paths:
                typer.echo(f"  {symbol} -> {p}")
        except Exception as e:
            typer.echo(f"  [ERROR] {symbol}: {e}", err=True)

    typer.echo(f"Done. {total_written} partition(s) written.")


@process_app.command("reprocess")
def process_reprocess(
    source: Annotated[str, typer.Option("--source", "-s", help="Source name")],
    symbols: Annotated[
        Optional[str], typer.Option("--symbols", help="Comma-separated symbols")
    ] = None,
    all_universe: Annotated[
        bool, typer.Option("--all", help="Reprocess all symbols")
    ] = False,
) -> None:
    """Force re-process raw files (overwrites existing partitions)."""
    # Reprocess is identical to run since our writes are idempotent (overwrite)
    process_run(source=source, symbols=symbols, all_universe=all_universe)
