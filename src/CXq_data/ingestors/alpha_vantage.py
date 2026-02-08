"""Alpha Vantage ingestor implementation."""

from __future__ import annotations

import datetime
import json
from pathlib import Path

import httpx

from CXq_data.config.settings import SourceAlphaVantageSettings
from CXq_data.ingestors.base import FetchResult, IngestorError, RateLimit
from CXq_data.utils.logging import get_logger

logger = get_logger(__name__)


class AlphaVantageIngestor:
    """Fetches market data using the Alpha Vantage REST API."""

    source_name = "alpha_vantage"

    def __init__(self, settings: SourceAlphaVantageSettings) -> None:
        self._api_key = settings.api_key.get_secret_value()
        self._base_url = settings.base_url
        self._client = httpx.Client(timeout=30.0)
        self._rate_limit = RateLimit(
            calls_per_minute=settings.calls_per_minute,
            calls_per_day=settings.calls_per_day,
        )

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
        """Fetch daily OHLCV data via Alpha Vantage and save as JSON."""
        logger.info("fetching_daily", source=self.source_name, symbol=symbol)

        if not self._api_key:
            raise IngestorError("Alpha Vantage API key not configured (set AV_API_KEY)")

        params = {
            "function": "TIME_SERIES_DAILY_ADJUSTED",
            "symbol": symbol,
            "outputsize": "full",
            "apikey": self._api_key,
        }

        try:
            response = self._client.get(self._base_url, params=params)
            response.raise_for_status()
            raw_data = response.json()
        except httpx.HTTPError as e:
            raise IngestorError(f"Alpha Vantage API error for {symbol}: {e}") from e

        # Check for API error messages
        if "Error Message" in raw_data:
            raise IngestorError(f"AV API error: {raw_data['Error Message']}")
        if "Note" in raw_data:
            raise IngestorError(f"AV rate limit hit: {raw_data['Note']}")

        time_series = raw_data.get("Time Series (Daily)", {})
        if not time_series:
            raise IngestorError(f"No daily data in response for {symbol}")

        # Write raw JSON
        out_path = output_dir / self.source_name / symbol
        out_path.mkdir(parents=True, exist_ok=True)

        filename = f"{datetime.date.today().isoformat()}_daily.json"
        file_path = out_path / filename
        file_path.write_text(json.dumps(raw_data, indent=2))

        # Filter dates to requested range and compute stats
        dates = sorted(time_series.keys())
        filtered = [
            d for d in dates
            if start_date.isoformat() <= d <= end_date.isoformat()
        ]

        actual_start = (
            datetime.date.fromisoformat(filtered[0]) if filtered else start_date
        )
        actual_end = (
            datetime.date.fromisoformat(filtered[-1]) if filtered else end_date
        )

        logger.info(
            "fetch_complete",
            symbol=symbol,
            rows=len(filtered),
            path=str(file_path),
        )

        return FetchResult(
            raw_path=file_path,
            rows_received=len(filtered),
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
        """Fetch intraday data via Alpha Vantage TIME_SERIES_INTRADAY."""
        logger.info(
            "fetching_intraday",
            source=self.source_name,
            symbol=symbol,
            interval=interval,
        )

        if not self._api_key:
            raise IngestorError("Alpha Vantage API key not configured (set AV_API_KEY)")

        # Map common interval names to AV format
        av_intervals = {"1m": "1min", "5m": "5min", "15m": "15min", "30m": "30min", "60m": "60min"}
        av_interval = av_intervals.get(interval, interval)

        valid = {"1min", "5min", "15min", "30min", "60min"}
        if av_interval not in valid:
            raise IngestorError(f"Invalid interval '{interval}'. Valid: {sorted(valid)}")

        params = {
            "function": "TIME_SERIES_INTRADAY",
            "symbol": symbol,
            "interval": av_interval,
            "outputsize": "full",
            "apikey": self._api_key,
        }

        try:
            response = self._client.get(self._base_url, params=params)
            response.raise_for_status()
            raw_data = response.json()
        except httpx.HTTPError as e:
            raise IngestorError(f"Alpha Vantage intraday error for {symbol}: {e}") from e

        if "Error Message" in raw_data:
            raise IngestorError(f"AV API error: {raw_data['Error Message']}")

        ts_key = f"Time Series ({av_interval})"
        time_series = raw_data.get(ts_key, {})
        if not time_series:
            raise IngestorError(f"No intraday data in response for {symbol}")

        out_path = output_dir / self.source_name / symbol
        out_path.mkdir(parents=True, exist_ok=True)

        filename = f"{datetime.date.today().isoformat()}_{av_interval}.json"
        file_path = out_path / filename
        file_path.write_text(json.dumps(raw_data, indent=2))

        dates = sorted(time_series.keys())
        actual_start = datetime.date.fromisoformat(dates[0][:10]) if dates else start_date
        actual_end = datetime.date.fromisoformat(dates[-1][:10]) if dates else end_date

        return FetchResult(
            raw_path=file_path,
            rows_received=len(dates),
            start_date=actual_start,
            end_date=actual_end,
            symbol=symbol,
            source=self.source_name,
        )
