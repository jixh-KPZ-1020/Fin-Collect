"""Base ingestor Protocol and supporting types."""

from __future__ import annotations

import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, runtime_checkable


@dataclass(frozen=True)
class RateLimit:
    """Rate limit configuration for an API source."""

    calls_per_minute: int
    calls_per_day: int | None = None  # None means unlimited


@dataclass(frozen=True)
class FetchResult:
    """Result of a fetch operation."""

    raw_path: Path
    rows_received: int
    start_date: datetime.date
    end_date: datetime.date
    symbol: str
    source: str


class IngestorError(Exception):
    """Base exception for all ingestor failures."""


@runtime_checkable
class BaseIngestor(Protocol):
    """Protocol defining the contract every data-source ingestor must fulfill.

    Uses structural subtyping — any class with matching methods conforms
    without needing to inherit from this protocol.
    """

    @property
    def source_name(self) -> str:
        """Short identifier for this source, e.g. 'alpha_vantage', 'yfinance'."""
        ...

    @property
    def rate_limit(self) -> RateLimit:
        """Rate limit configuration for this source."""
        ...

    def fetch_daily(
        self,
        symbol: str,
        start_date: datetime.date,
        end_date: datetime.date,
        output_dir: Path,
    ) -> FetchResult:
        """Fetch daily OHLCV data for a single symbol within the date range.

        Writes the raw response to output_dir and returns a FetchResult.
        Raises IngestorError on failure.
        """
        ...

    def fetch_intraday(
        self,
        symbol: str,
        start_date: datetime.date,
        end_date: datetime.date,
        interval: str,
        output_dir: Path,
    ) -> FetchResult:
        """Fetch intraday OHLCV data. Raises NotImplementedError if unsupported."""
        ...
