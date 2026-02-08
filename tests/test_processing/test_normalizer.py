"""Tests for normalizer module."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from CXq_data.processing.normalizer import (
    normalize_alpha_vantage_daily,
    normalize_stooq_daily,
    normalize_yfinance_daily,
)
from CXq_data.processing.schemas import DAILY_OHLCV_COLUMNS


def test_normalize_yfinance_daily(sample_yfinance_csv: Path):
    """yfinance CSV normalizes to canonical schema."""
    df = normalize_yfinance_daily(sample_yfinance_csv, "AAPL")

    assert len(df) == 5
    assert "symbol" in df.columns
    assert df["symbol"][0] == "AAPL"

    # All canonical columns present
    for col in DAILY_OHLCV_COLUMNS:
        assert col in df.columns

    assert df["source"][0] == "yfinance"
    assert df["date"].dtype == pl.Date
    assert df["volume"].dtype == pl.Int64


def test_normalize_alpha_vantage_daily(tmp_path: Path):
    """Alpha Vantage JSON normalizes to canonical schema."""
    av_data = {
        "Meta Data": {"1. Information": "Daily Prices"},
        "Time Series (Daily)": {
            "2024-01-02": {
                "1. open": "150.00",
                "2. high": "152.00",
                "3. low": "149.00",
                "4. close": "151.00",
                "5. adjusted close": "151.00",
                "6. volume": "1000000",
            },
            "2024-01-03": {
                "1. open": "151.00",
                "2. high": "153.00",
                "3. low": "150.00",
                "4. close": "149.50",
                "5. adjusted close": "149.50",
                "6. volume": "1100000",
            },
        },
    }

    json_path = tmp_path / "daily.json"
    json_path.write_text(json.dumps(av_data))

    df = normalize_alpha_vantage_daily(json_path, "MSFT")

    assert len(df) == 2
    assert df["symbol"][0] == "MSFT"
    assert df["source"][0] == "alpha_vantage"

    for col in DAILY_OHLCV_COLUMNS:
        assert col in df.columns


def test_normalize_stooq_daily(sample_stooq_csv: Path):
    """Stooq CSV normalizes to canonical schema with adjusted_close = close."""
    df = normalize_stooq_daily(sample_stooq_csv, "AAPL")

    assert len(df) == 5
    assert "symbol" in df.columns
    assert df["symbol"][0] == "AAPL"
    assert df["source"][0] == "stooq"

    for col in DAILY_OHLCV_COLUMNS:
        assert col in df.columns

    assert df["date"].dtype == pl.Date
    assert df["volume"].dtype == pl.Int64

    # Stooq has no adjusted close — it should equal close
    assert df["adjusted_close"].to_list() == df["close"].to_list()
