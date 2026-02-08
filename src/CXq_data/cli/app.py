"""Root CLI application."""

from __future__ import annotations

import typer

from CXq_data.cli.crossvalidate import crossvalidate_app
from CXq_data.cli.db import db_app
from CXq_data.cli.ingest import ingest_app
from CXq_data.cli.process import process_app
from CXq_data.cli.query import query_app
from CXq_data.cli.validate import validate_app

app = typer.Typer(
    name="cxq_data",
    help="Stock market data ingestion, normalization, storage, and serving.",
    no_args_is_help=True, # running cxq_data without arg. prints the help menu.
)

app.add_typer(ingest_app, name="ingest", help="Fetch raw data from providers")
app.add_typer(process_app, name="process", help="Normalize raw data to Parquet")
app.add_typer(query_app, name="query", help="Query data via DuckDB")
app.add_typer(validate_app, name="validate", help="Run data quality checks")
app.add_typer(db_app, name="db", help="Manage DuckDB database")
app.add_typer(crossvalidate_app, name="crossvalidate", help="Compare data across sources")


def main() -> None:
    from CXq_data.config.loader import get_settings
    from CXq_data.utils.logging import setup_logging

    settings = get_settings()
    setup_logging(settings.log_level)
    app()
