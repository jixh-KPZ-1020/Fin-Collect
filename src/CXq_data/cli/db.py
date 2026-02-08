"""CLI commands for managing the DuckDB database."""

from __future__ import annotations

import typer
from rich.console import Console
from rich.table import Table

db_app = typer.Typer(no_args_is_help=True)
console = Console()


@db_app.command("init")
def db_init() -> None:
    """Create or refresh DuckDB views over Parquet files."""
    from CXq_data.config.loader import get_settings
    from CXq_data.storage.duckdb_manager import DuckDBManager

    settings = get_settings()
    manager = DuckDBManager(settings.storage.duckdb_path, settings.storage.processed_dir)

    with manager.connect() as db:
        views = db.create_views()

    if views:
        typer.echo(f"Created {len(views)} view(s): {', '.join(views)}")
    else:
        typer.echo("No datasets found in processed directory. Ingest and process data first.")


@db_app.command("info")
def db_info() -> None:
    """Show view metadata, row counts, and column info."""
    from CXq_data.config.loader import get_settings
    from CXq_data.storage.duckdb_manager import DuckDBManager

    settings = get_settings()
    manager = DuckDBManager(settings.storage.duckdb_path, settings.storage.processed_dir)

    with manager.connect() as db:
        db.create_views()
        info = db.table_info()

    if not info:
        typer.echo("No views found. Run `cxq_data db init` after processing data.")
        return

    table = Table(title="DuckDB Views", show_header=True, header_style="bold")
    table.add_column("View")
    table.add_column("Rows", justify="right")
    table.add_column("Columns", justify="right")
    table.add_column("Column Names")

    for view in info:
        if "error" in view:
            table.add_row(view["name"], "ERROR", "", view["error"])
        else:
            table.add_row(
                view["name"],
                f"{view['rows']:,}",
                str(view["columns"]),
                ", ".join(view["column_names"]),
            )

    console.print(table)
