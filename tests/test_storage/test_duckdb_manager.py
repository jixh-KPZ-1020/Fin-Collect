"""Tests for DuckDB manager."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from CXq_data.processing.partitioner import write_partitioned
from CXq_data.storage.duckdb_manager import DuckDBManager


def test_create_views_and_query(tmp_data_dir: Path, sample_ohlcv_df: pl.DataFrame):
    """DuckDB creates views over Parquet and queries return correct data."""
    processed = tmp_data_dir / "processed"
    write_partitioned(sample_ohlcv_df, processed)

    db_path = tmp_data_dir / "test.duckdb"
    manager = DuckDBManager(db_path, processed)

    with manager.connect() as db:
        views = db.create_views()
        assert "daily_ohlcv" in views

        df = db.to_polars("SELECT * FROM daily_ohlcv WHERE symbol = 'AAPL'")
        assert len(df) == 5
        assert "symbol" in df.columns
        assert "year" in df.columns

        # Check partition pruning returns correct symbol
        assert all(s == "AAPL" for s in df["symbol"].to_list())


def test_table_info(tmp_data_dir: Path, sample_ohlcv_df: pl.DataFrame):
    """table_info() returns metadata about views."""
    processed = tmp_data_dir / "processed"
    write_partitioned(sample_ohlcv_df, processed)

    db_path = tmp_data_dir / "test.duckdb"
    manager = DuckDBManager(db_path, processed)

    with manager.connect() as db:
        db.create_views()
        info = db.table_info()

    assert len(info) == 1
    assert info[0]["name"] == "daily_ohlcv"
    assert info[0]["rows"] == 5


def test_no_parquet_files(tmp_data_dir: Path):
    """create_views() handles empty processed directory gracefully."""
    processed = tmp_data_dir / "processed"
    db_path = tmp_data_dir / "test.duckdb"
    manager = DuckDBManager(db_path, processed)

    with manager.connect() as db:
        views = db.create_views()

    assert views == []
