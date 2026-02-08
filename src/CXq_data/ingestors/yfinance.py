"""yfinance ingestor implementation."""

from __future__ import annotations

import datetime
import json
from pathlib import Path

import yfinance as yf

from CXq_data.config.settings import SourceYFinanceSettings
from CXq_data.ingestors.base import FetchResult, IngestorError, RateLimit
from CXq_data.utils.logging import get_logger

logger = get_logger(__name__)


class YFinanceIngestor:
    """Fetches market data using the yfinance library."""

    source_name = "yfinance"

    def __init__(self, settings: SourceYFinanceSettings) -> None:
        self._settings = settings
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
        """Fetch daily OHLCV data via yfinance and save as CSV."""
        logger.info("fetching_daily", source=self.source_name, symbol=symbol)

        try:
            ticker = yf.Ticker(symbol)
            df = ticker.history(
                start=start_date.isoformat(),
                end=(end_date + datetime.timedelta(days=1)).isoformat(),
                interval="1d",
                auto_adjust=False,
            )
        except Exception as e:
            raise IngestorError(f"yfinance fetch failed for {symbol}: {e}") from e

        if df.empty:
            raise IngestorError(f"No data returned for {symbol} ({start_date} to {end_date})")

        # Write raw CSV
        out_path = output_dir / self.source_name / symbol
        out_path.mkdir(parents=True, exist_ok=True)

        filename = f"{datetime.date.today().isoformat()}_daily.csv"
        file_path = out_path / filename
        df.to_csv(file_path)

        # Write metadata sidecar
        meta_path = out_path / f"{datetime.date.today().isoformat()}_daily.meta.json"
        meta = {
            "source": self.source_name,
            "symbol": symbol,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "rows": len(df),
            "fetched_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
        meta_path.write_text(json.dumps(meta, indent=2))

        actual_start = df.index.min().date()
        actual_end = df.index.max().date()

        logger.info(
            "fetch_complete",
            symbol=symbol,
            rows=len(df),
            path=str(file_path),
        )

        return FetchResult(
            raw_path=file_path,
            rows_received=len(df),
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
        """Fetch intraday data via yfinance.

        Note: yfinance only supports intraday data for the last 60 days
        for intervals < 1d.
        """
        logger.info(
            "fetching_intraday",
            source=self.source_name,
            symbol=symbol,
            interval=interval,
        )

        valid_intervals = {"1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h"}
        if interval not in valid_intervals:
            raise IngestorError(
                f"Invalid interval '{interval}'. Valid: {sorted(valid_intervals)}"
            )

        try:
            ticker = yf.Ticker(symbol)
            df = ticker.history(
                start=start_date.isoformat(),
                end=(end_date + datetime.timedelta(days=1)).isoformat(),
                interval=interval,
                auto_adjust=False,
            )
        except Exception as e:
            raise IngestorError(
                f"yfinance intraday fetch failed for {symbol}: {e}"
            ) from e

        if df.empty:
            raise IngestorError(
                f"No intraday data returned for {symbol} ({start_date} to {end_date})"
            )

        out_path = output_dir / self.source_name / symbol
        out_path.mkdir(parents=True, exist_ok=True)

        filename = f"{datetime.date.today().isoformat()}_{interval}.csv"
        file_path = out_path / filename
        df.to_csv(file_path)

        actual_start = df.index.min().date()
        actual_end = df.index.max().date()

        return FetchResult(
            raw_path=file_path,
            rows_received=len(df),
            start_date=actual_start,
            end_date=actual_end,
            symbol=symbol,
            source=self.source_name,
        )
