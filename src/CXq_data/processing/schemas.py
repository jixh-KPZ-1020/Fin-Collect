"""Canonical OHLCV column schema definition."""

from __future__ import annotations

import polars as pl

# Canonical column names and types for daily OHLCV data.
# Partition columns (symbol, year) are NOT included here — they are
# derived from the Hive directory structure by DuckDB.
DAILY_OHLCV_SCHEMA = {
    "date": pl.Date,
    "open": pl.Float64,
    "high": pl.Float64,
    "low": pl.Float64,
    "close": pl.Float64,
    "adjusted_close": pl.Float64,
    "volume": pl.Int64,
    "source": pl.Utf8,
    "ingested_at": pl.Datetime("us", "UTC"),
}

DAILY_OHLCV_COLUMNS = list(DAILY_OHLCV_SCHEMA.keys())
