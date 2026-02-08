"""Tests for validation checks."""

from __future__ import annotations

import datetime

import polars as pl

from CXq_data.validation.checks import (
    check_ohlc_consistency,
    check_price_sanity,
    check_stale_data,
    check_trading_day_gaps,
)
from CXq_data.validation.models import CheckStatus


def test_price_sanity_passes_clean_data(sample_ohlcv_df: pl.DataFrame):
    result = check_price_sanity(sample_ohlcv_df, "AAPL")
    assert result.status == CheckStatus.PASS


def test_price_sanity_fails_negative_prices():
    df = pl.DataFrame(
        {
            "date": [datetime.date(2024, 1, 2)],
            "open": [-10.0],
            "high": [5.0],
            "low": [-15.0],
            "close": [3.0],
            "adjusted_close": [3.0],
            "volume": [100],
            "source": ["test"],
            "ingested_at": [datetime.datetime.now(datetime.timezone.utc)],
        }
    )
    result = check_price_sanity(df, "BAD")
    assert result.status == CheckStatus.FAIL
    assert "negative" in result.message.lower()


def test_price_sanity_fails_high_less_than_low():
    df = pl.DataFrame(
        {
            "date": [datetime.date(2024, 1, 2)],
            "open": [10.0],
            "high": [5.0],  # high < low
            "low": [8.0],
            "close": [9.0],
            "adjusted_close": [9.0],
            "volume": [100],
            "source": ["test"],
            "ingested_at": [datetime.datetime.now(datetime.timezone.utc)],
        }
    )
    result = check_price_sanity(df, "BAD")
    assert result.status == CheckStatus.FAIL
    assert "high < low" in result.message


def test_ohlc_consistency_warns_flat_bars():
    """Identical OHLC values should warn."""
    df = pl.DataFrame(
        {
            "date": [datetime.date(2024, 1, 2)],
            "open": [100.0],
            "high": [100.0],
            "low": [100.0],
            "close": [100.0],
            "adjusted_close": [100.0],
            "volume": [0],
            "source": ["test"],
            "ingested_at": [datetime.datetime.now(datetime.timezone.utc)],
        }
    )
    result = check_ohlc_consistency(df, "FLAT")
    assert result.status == CheckStatus.WARN
    assert "identical" in result.message.lower()


def test_trading_day_gaps_detects_missing_days():
    """A gap in trading days should be detected."""
    df = pl.DataFrame(
        {
            "date": [
                datetime.date(2024, 1, 2),
                # Jan 3-5 missing (3 trading days)
                datetime.date(2024, 1, 8),
            ],
            "open": [150.0, 153.0],
            "high": [152.0, 155.0],
            "low": [149.0, 152.0],
            "close": [151.0, 154.0],
            "adjusted_close": [151.0, 154.0],
            "volume": [1000000, 1050000],
            "source": ["test", "test"],
            "ingested_at": [datetime.datetime.now(datetime.timezone.utc)] * 2,
        }
    )
    result = check_trading_day_gaps(df, "AAPL")
    assert result.status in (CheckStatus.WARN, CheckStatus.FAIL)
    assert "missing" in result.message.lower()
