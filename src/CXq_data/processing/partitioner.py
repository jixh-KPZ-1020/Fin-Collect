"""Writes normalized DataFrames to Hive-partitioned Parquet files."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from CXq_data.processing.schemas import DAILY_OHLCV_COLUMNS
from CXq_data.utils.logging import get_logger

logger = get_logger(__name__)


# Columns that are extracted from the Hive directory path, not stored in Parquet
PARTITION_COLUMNS = ["symbol", "source", "year"]

# Columns stored inside each Parquet file (canonical minus partition cols)
_PARQUET_COLUMNS = [c for c in DAILY_OHLCV_COLUMNS if c not in PARTITION_COLUMNS]


def write_partitioned(
    df: pl.DataFrame,
    processed_dir: Path,
    dataset: str = "daily_ohlcv",
) -> list[Path]:
    """Write a normalized DataFrame to Hive-partitioned Parquet.

    Groups by (symbol, source, year), writes one file per group.
    Overwrites existing partition files (idempotent rewrite).

    Path layout: dataset/symbol=X/source=Y/year=Z/data.parquet
    This allows multiple sources to coexist for cross-validation.

    Partition columns (symbol, source, year) are dropped from the Parquet
    file content since DuckDB reconstructs them from the directory path.

    Returns list of written file paths.
    """
    # Derive year from date for partitioning
    df = df.with_columns(pl.col("date").dt.year().alias("year"))

    written: list[Path] = []

    for (symbol, source, year), group_df in df.group_by(["symbol", "source", "year"]):
        partition_dir = (
            processed_dir
            / dataset
            / f"symbol={symbol}"
            / f"source={source}"
            / f"year={year}"
        )
        partition_dir.mkdir(parents=True, exist_ok=True)

        out_path = partition_dir / "data.parquet"

        # Drop partition columns — DuckDB extracts them from the path
        write_df = group_df.select(_PARQUET_COLUMNS)

        write_df.write_parquet(
            out_path,
            compression="zstd",
            statistics=True,
        )

        logger.info(
            "partition_written",
            symbol=symbol,
            source=source,
            year=year,
            rows=len(write_df),
            path=str(out_path),
        )
        written.append(out_path)

    return written
