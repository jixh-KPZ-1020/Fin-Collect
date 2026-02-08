"""Centralized path resolution for all data directories."""

from __future__ import annotations

from pathlib import Path

from CXq_data.config.settings import StorageSettings


def raw_dir(settings: StorageSettings, source: str, symbol: str | None = None) -> Path:
    """Path to raw data directory for a given source and optional symbol."""
    path = settings.raw_dir / source
    if symbol:
        path = path / symbol
    return path


def processed_dir(settings: StorageSettings, dataset: str = "daily_ohlcv") -> Path:
    """Path to processed Parquet dataset directory."""
    return settings.processed_dir / dataset


def duckdb_path(settings: StorageSettings) -> Path:
    """Path to the DuckDB database file."""
    return settings.duckdb_path
