"""Stooq ingestor — free CSV download, no API key required."""

from __future__ import annotations

import datetime
import json
from pathlib import Path

import httpx

from CXq_data.config.settings import SourceStooqSettings
from CXq_data.ingestors.base import FetchResult, IngestorError, RateLimit
from CXq_data.utils.logging import get_logger

logger = get_logger(__name__)


class StooqIngestor:
    """Fetches daily OHLCV data from Stooq via CSV download."""

    source_name = "stooq"

    def __init__(self, settings: SourceStooqSettings) -> None:
        self._settings = settings
        self._base_url = settings.base_url
        self._suffix = settings.symbol_suffix
        self._client = httpx.Client(
            timeout=30.0,
            headers={"User-Agent": "Mozilla/5.0 (cxq_data stock data collector)"},
        )
        self._rate_limit = RateLimit(calls_per_minute=settings.calls_per_minute)

    @property
    def rate_limit(self) -> RateLimit:
        return self._rate_limit

    def fetch_daily(
        self,
        symbol: str,
        start_date: datetime.date,
        end_date: datetime.date,
        output_dir: Path,
    ) -> FetchResult:
        """Fetch daily OHLCV data from Stooq and save as CSV."""
        logger.info("fetching_daily", source=self.source_name, symbol=symbol)

        stooq_symbol = f"{symbol.lower()}{self._suffix}"
        d1 = start_date.strftime("%Y%m%d")
        d2 = end_date.strftime("%Y%m%d")

        url = f"{self._base_url}?s={stooq_symbol}&d1={d1}&d2={d2}&i=d"

        try:
            response = self._client.get(url)
            response.raise_for_status()
        except httpx.HTTPError as e:
            raise IngestorError(f"Stooq HTTP error for {symbol}: {e}") from e

        body = response.text.strip()

        # Detect error responses
        if "No data" in body or not body:
            raise IngestorError(f"No data from Stooq for {symbol} ({start_date} to {end_date})")
        if "Exceeded" in body:
            raise IngestorError(f"Stooq daily hit limit exceeded for {symbol}")

        # Verify it looks like a CSV with headers
        lines = body.splitlines()
        if len(lines) < 2:
            raise IngestorError(f"Stooq returned empty data for {symbol}")

        # Write raw CSV
        out_path = output_dir / self.source_name / symbol
        out_path.mkdir(parents=True, exist_ok=True)

        filename = f"{datetime.date.today().isoformat()}_daily.csv"
        file_path = out_path / filename
        file_path.write_text(body)

        # Write metadata sidecar
        row_count = len(lines) - 1  # Subtract header
        meta_path = out_path / f"{datetime.date.today().isoformat()}_daily.meta.json"
        meta = {
            "source": self.source_name,
            "symbol": symbol,
            "stooq_symbol": stooq_symbol,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "rows": row_count,
            "fetched_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
        meta_path.write_text(json.dumps(meta, indent=2))

        # Parse actual date range from CSV
        # CSV columns: Date,Open,High,Low,Close,Volume
        data_lines = lines[1:]
        dates = [line.split(",")[0] for line in data_lines if line.strip()]
        actual_start = datetime.date.fromisoformat(min(dates)) if dates else start_date
        actual_end = datetime.date.fromisoformat(max(dates)) if dates else end_date

        logger.info(
            "fetch_complete",
            symbol=symbol,
            rows=row_count,
            path=str(file_path),
        )

        return FetchResult(
            raw_path=file_path,
            rows_received=row_count,
            start_date=actual_start,
            end_date=actual_end,
            symbol=symbol,
            source=self.source_name,
        )

    def fetch_intraday(
        self,
        symbol: str,
        start_date: datetime.date,
        end_date: datetime.date,
        interval: str,
        output_dir: Path,
    ) -> FetchResult:
        """Stooq CSV downloads do not reliably support intraday data."""
        raise IngestorError(
            "Stooq does not support intraday data via CSV download. "
            "Use yfinance or Alpha Vantage for intraday."
        )
