"""DuckDB connection management and view creation over Hive-partitioned Parquet."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Generator

import duckdb
import polars as pl

from CXq_data.utils.logging import get_logger

logger = get_logger(__name__)


class DuckDBManager:
    """Manages DuckDB connection lifecycle and Parquet views."""

    def __init__(self, db_path: Path, processed_dir: Path) -> None:
        self._db_path = db_path
        self._processed_dir = processed_dir
        self._conn: duckdb.DuckDBPyConnection | None = None

    @contextmanager
    def connect(self) -> Generator[DuckDBManager, None, None]:
        """Context manager for DuckDB connection lifecycle."""
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = duckdb.connect(str(self._db_path))
        try:
            yield self
        finally:
            self._conn.close()
            self._conn = None

    def create_views(self) -> list[str]:
        """Scan processed directory and create a view for each dataset.

        Returns list of created view names.
        """
        assert self._conn is not None, "Not connected. Use `with manager.connect():`"

        created = []
        if not self._processed_dir.exists():
            logger.warning("processed_dir_missing", path=str(self._processed_dir))
            return created

        for dataset_dir in self._processed_dir.iterdir():
            if not dataset_dir.is_dir():
                continue

            dataset_name = dataset_dir.name
            glob_pattern = str(dataset_dir / "**" / "*.parquet")

            # Check if any parquet files exist
            parquet_files = list(dataset_dir.rglob("*.parquet"))
            if not parquet_files:
                logger.warning("no_parquet_files", dataset=dataset_name)
                continue

            sql = f"""
                CREATE OR REPLACE VIEW {dataset_name} AS
                SELECT *
                FROM read_parquet(
                    '{glob_pattern}',
                    hive_partitioning = true,
                    hive_types = {{'symbol': VARCHAR, 'source': VARCHAR, 'year': INTEGER}}
                );
            """
            self._conn.execute(sql)
            created.append(dataset_name)
            logger.info("view_created", view=dataset_name, files=len(parquet_files))

        return created

    def execute(self, sql: str) -> duckdb.DuckDBPyRelation:
        """Execute SQL and return a DuckDB relation."""
        assert self._conn is not None, "Not connected. Use `with manager.connect():`"
        return self._conn.sql(sql)

    def to_polars(self, sql: str) -> pl.DataFrame:
        """Execute SQL and return a Polars DataFrame."""
        return self.execute(sql).pl()

    def table_info(self) -> list[dict]:
        """Return metadata about all views."""
        assert self._conn is not None, "Not connected. Use `with manager.connect():`"

        views = self._conn.sql("SHOW TABLES").fetchall()
        info = []
        for (name,) in views:
            try:
                count = self._conn.sql(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
                cols = self._conn.sql(f"DESCRIBE {name}").fetchall()
                info.append({
                    "name": name,
                    "rows": count,
                    "columns": len(cols),
                    "column_names": [c[0] for c in cols],
                })
            except Exception as e:
                info.append({"name": name, "error": str(e)})

        return info
