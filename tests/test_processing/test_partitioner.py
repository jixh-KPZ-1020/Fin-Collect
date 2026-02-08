"""Tests for Hive-partitioned Parquet writer."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from CXq_data.processing.partitioner import write_partitioned


def test_write_partitioned_creates_hive_structure(
    tmp_data_dir: Path, sample_ohlcv_df: pl.DataFrame
):
    """Partitioner writes files in symbol=X/source=Y/year=Z/data.parquet structure."""
    processed = tmp_data_dir / "processed"
    paths = write_partitioned(sample_ohlcv_df, processed)

    assert len(paths) == 1  # All 2024 data for AAPL from one source
    assert paths[0].name == "data.parquet"
    assert "symbol=AAPL" in str(paths[0])
    assert "source=test" in str(paths[0])
    assert "year=2024" in str(paths[0])

    # Read back and verify
    df = pl.read_parquet(paths[0])
    assert len(df) == 5
    assert "symbol" not in df.columns  # Partition cols dropped from file content
    assert "source" not in df.columns
    assert "year" not in df.columns


def test_write_partitioned_multiple_years(tmp_data_dir: Path, sample_ohlcv_df: pl.DataFrame):
    """Multiple years get separate partition directories."""
    import datetime

    # Add a 2023 row
    extra = pl.DataFrame(
        {
            "date": [datetime.date(2023, 12, 29)],
            "open": [148.0],
            "high": [149.0],
            "low": [147.0],
            "close": [148.5],
            "adjusted_close": [148.5],
            "volume": [900000],
            "source": ["test"],
            "ingested_at": [sample_ohlcv_df["ingested_at"][0]],
            "symbol": ["AAPL"],
        }
    )
    df = pl.concat([sample_ohlcv_df, extra])

    processed = tmp_data_dir / "processed"
    paths = write_partitioned(df, processed)

    assert len(paths) == 2  # 2023 and 2024
    years = {str(p.parent.name) for p in paths}
    assert years == {"year=2023", "year=2024"}
