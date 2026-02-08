"""Converts raw source files to canonical Polars DataFrames."""

from __future__ import annotations

import datetime
import json
from pathlib import Path

import polars as pl

from CXq_data.processing.schemas import DAILY_OHLCV_COLUMNS


def normalize_yfinance_daily(raw_path: Path, symbol: str) -> pl.DataFrame:
    """Normalize a yfinance daily CSV to the canonical schema.

    yfinance CSVs have columns: Date, Open, High, Low, Close, Adj Close, Volume
    with a DatetimeIndex as the first column.
    """
    df = pl.read_csv(raw_path)

    # yfinance uses "Date" or "Datetime" as the index column name
    date_col = "Date" if "Date" in df.columns else "Datetime"

    df = df.rename(
        {
            date_col: "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )

    # Handle adjusted close — yfinance uses "Adj Close" or "Adj. Close"
    adj_close_col = None
    for candidate in ["Adj Close", "Adj. Close", "Adjusted Close"]:
        if candidate in df.columns:
            adj_close_col = candidate
            break

    if adj_close_col:
        df = df.rename({adj_close_col: "adjusted_close"})
    else:
        df = df.with_columns(pl.col("close").alias("adjusted_close"))

    now_utc = datetime.datetime.now(datetime.timezone.utc)

    # yfinance date formats vary: "2024-01-02" or "2024-12-02 00:00:00-05:00"
    # Try plain date first, fall back to datetime parsing
    date_sample = df["date"][0]
    if ":" in str(date_sample):
        # Timezone-aware datetime string — parse as datetime, extract date
        date_expr = pl.col("date").str.to_datetime("%Y-%m-%d %H:%M:%S%z").dt.date()
    else:
        date_expr = pl.col("date").str.to_date("%Y-%m-%d")

    df = df.with_columns(
        date_expr.alias("date"),
        pl.col("open").cast(pl.Float64),
        pl.col("high").cast(pl.Float64),
        pl.col("low").cast(pl.Float64),
        pl.col("close").cast(pl.Float64),
        pl.col("adjusted_close").cast(pl.Float64),
        pl.col("volume").cast(pl.Int64),
        pl.lit("yfinance").alias("source"),
        pl.lit(now_utc).alias("ingested_at"),
    )

    # Add symbol for partitioning (will be used by partitioner, then dropped from Parquet)
    df = df.with_columns(pl.lit(symbol).alias("symbol"))

    return df.select(DAILY_OHLCV_COLUMNS + ["symbol"])


def normalize_alpha_vantage_daily(raw_path: Path, symbol: str) -> pl.DataFrame:
    """Normalize an Alpha Vantage daily JSON to the canonical schema.

    AV JSON structure:
    {
        "Time Series (Daily)": {
            "2024-01-15": {"1. open": "...", "2. high": "...", ...}
        }
    }
    """
    with open(raw_path) as f:
        data = json.load(f)

    time_series = data.get("Time Series (Daily)", {})
    if not time_series:
        raise ValueError(f"No 'Time Series (Daily)' found in {raw_path}")

    rows = []
    for date_str, values in time_series.items():
        rows.append(
            {
                "date": date_str,
                "open": float(values["1. open"]),
                "high": float(values["2. high"]),
                "low": float(values["3. low"]),
                "close": float(values["4. close"]),
                "volume": int(values["6. volume"]),
                "adjusted_close": float(values.get("5. adjusted close", values["4. close"])),
            }
        )

    now_utc = datetime.datetime.now(datetime.timezone.utc)

    df = pl.DataFrame(rows)
    df = df.with_columns(
        pl.col("date").str.to_date("%Y-%m-%d"),
        pl.lit("alpha_vantage").alias("source"),
        pl.lit(now_utc).alias("ingested_at"),
        pl.lit(symbol).alias("symbol"),
    )

    return df.sort("date").select(DAILY_OHLCV_COLUMNS + ["symbol"])


def normalize_stooq_daily(raw_path: Path, symbol: str) -> pl.DataFrame:
    """Normalize a Stooq daily CSV to the canonical schema.

    Stooq CSVs have columns: Date, Open, High, Low, Close, Volume.
    Stooq does NOT provide adjusted close — we set adjusted_close = close.
    """
    df = pl.read_csv(raw_path)

    df = df.rename(
        {
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )

    now_utc = datetime.datetime.now(datetime.timezone.utc)

    df = df.with_columns(
        pl.col("date").str.to_date("%Y-%m-%d"),
        pl.col("open").cast(pl.Float64),
        pl.col("high").cast(pl.Float64),
        pl.col("low").cast(pl.Float64),
        pl.col("close").cast(pl.Float64),
        pl.col("close").cast(pl.Float64).alias("adjusted_close"),
        pl.col("volume").cast(pl.Int64),
        pl.lit("stooq").alias("source"),
        pl.lit(now_utc).alias("ingested_at"),
        pl.lit(symbol).alias("symbol"),
    )

    return df.sort("date").select(DAILY_OHLCV_COLUMNS + ["symbol"])


# Map source names to their normalizer functions
NORMALIZERS: dict[str, callable] = {
    "yfinance": normalize_yfinance_daily,
    "alpha_vantage": normalize_alpha_vantage_daily,
    "stooq": normalize_stooq_daily,
}


def normalize(source: str, raw_path: Path, symbol: str) -> pl.DataFrame:
    """Dispatch to the appropriate normalizer by source name."""
    if source not in NORMALIZERS:
        raise ValueError(f"No normalizer for source '{source}'. Available: {list(NORMALIZERS)}")
    return NORMALIZERS[source](raw_path, symbol)
