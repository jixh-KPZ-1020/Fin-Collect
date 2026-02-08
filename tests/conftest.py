"""Shared test fixtures."""

from __future__ import annotations

import datetime
from pathlib import Path

import polars as pl
import pytest


@pytest.fixture
def tmp_data_dir(tmp_path: Path) -> Path:
    """Create a temporary data directory structure."""
    (tmp_path / "raw").mkdir()
    (tmp_path / "processed").mkdir()
    return tmp_path


@pytest.fixture
def sample_ohlcv_df() -> pl.DataFrame:
    """A small sample OHLCV DataFrame with canonical schema + symbol column."""
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    return pl.DataFrame(
        {
            "date": [
                datetime.date(2024, 1, 2),
                datetime.date(2024, 1, 3),
                datetime.date(2024, 1, 4),
                datetime.date(2024, 1, 5),
                datetime.date(2024, 1, 8),
            ],
            "open": [150.0, 151.0, 149.5, 152.0, 153.0],
            "high": [152.0, 153.0, 151.0, 154.0, 155.0],
            "low": [149.0, 150.0, 148.5, 151.0, 152.0],
            "close": [151.0, 149.5, 150.5, 153.5, 154.0],
            "adjusted_close": [151.0, 149.5, 150.5, 153.5, 154.0],
            "volume": [1000000, 1100000, 950000, 1200000, 1050000],
            "source": ["test"] * 5,
            "ingested_at": [now_utc] * 5,
            "symbol": ["AAPL"] * 5,
        }
    )


@pytest.fixture
def sample_yfinance_csv(tmp_data_dir: Path) -> Path:
    """Write a sample yfinance-style CSV and return its path."""
    csv_dir = tmp_data_dir / "raw" / "yfinance" / "AAPL"
    csv_dir.mkdir(parents=True)
    csv_path = csv_dir / "2024-01-15_daily.csv"

    csv_content = """Date,Open,High,Low,Close,Adj Close,Volume
2024-01-02,150.0,152.0,149.0,151.0,151.0,1000000
2024-01-03,151.0,153.0,150.0,149.5,149.5,1100000
2024-01-04,149.5,151.0,148.5,150.5,150.5,950000
2024-01-05,152.0,154.0,151.0,153.5,153.5,1200000
2024-01-08,153.0,155.0,152.0,154.0,154.0,1050000"""

    csv_path.write_text(csv_content)
    return csv_path


@pytest.fixture
def sample_stooq_csv(tmp_data_dir: Path) -> Path:
    """Write a sample Stooq-style CSV and return its path."""
    csv_dir = tmp_data_dir / "raw" / "stooq" / "AAPL"
    csv_dir.mkdir(parents=True)
    csv_path = csv_dir / "2024-01-15_daily.csv"

    csv_content = """Date,Open,High,Low,Close,Volume
2024-01-02,150.0,152.0,149.0,151.0,1000000
2024-01-03,151.0,153.0,150.0,149.5,1100000
2024-01-04,149.5,151.0,148.5,150.5,950000
2024-01-05,152.0,154.0,151.0,153.5,1200000
2024-01-08,153.0,155.0,152.0,154.0,1050000"""

    csv_path.write_text(csv_content)
    return csv_path
