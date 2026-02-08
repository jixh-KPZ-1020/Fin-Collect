"""Tests for Stooq ingestor."""

from __future__ import annotations

from pathlib import Path

import pytest
import respx
from httpx import Response

from CXq_data.config.settings import SourceStooqSettings
from CXq_data.ingestors.base import IngestorError
from CXq_data.ingestors.stooq import StooqIngestor

SAMPLE_CSV = """Date,Open,High,Low,Close,Volume
2024-01-02,150.0,152.0,149.0,151.0,1000000
2024-01-03,151.0,153.0,150.0,149.5,1100000
2024-01-04,149.5,151.0,148.5,150.5,950000"""


@pytest.fixture
def stooq_ingestor() -> StooqIngestor:
    return StooqIngestor(SourceStooqSettings())


@respx.mock
def test_fetch_daily_success(stooq_ingestor: StooqIngestor, tmp_path: Path):
    """Successful fetch writes CSV and returns correct FetchResult."""
    import datetime

    respx.get("https://stooq.com/q/d/l/").mock(
        return_value=Response(200, text=SAMPLE_CSV)
    )

    result = stooq_ingestor.fetch_daily(
        symbol="AAPL",
        start_date=datetime.date(2024, 1, 1),
        end_date=datetime.date(2024, 1, 5),
        output_dir=tmp_path,
    )

    assert result.source == "stooq"
    assert result.symbol == "AAPL"
    assert result.rows_received == 3
    assert result.raw_path.exists()
    assert result.raw_path.read_text().startswith("Date,")


@respx.mock
def test_fetch_daily_no_data(stooq_ingestor: StooqIngestor, tmp_path: Path):
    """'No data' response raises IngestorError."""
    import datetime

    respx.get("https://stooq.com/q/d/l/").mock(
        return_value=Response(200, text="No data")
    )

    with pytest.raises(IngestorError, match="No data"):
        stooq_ingestor.fetch_daily(
            symbol="BADTICKER",
            start_date=datetime.date(2024, 1, 1),
            end_date=datetime.date(2024, 1, 5),
            output_dir=tmp_path,
        )


@respx.mock
def test_fetch_daily_rate_limit(stooq_ingestor: StooqIngestor, tmp_path: Path):
    """Exceeded limit response raises IngestorError."""
    import datetime

    respx.get("https://stooq.com/q/d/l/").mock(
        return_value=Response(200, text="Exceeded the daily hits limit")
    )

    with pytest.raises(IngestorError, match="limit"):
        stooq_ingestor.fetch_daily(
            symbol="AAPL",
            start_date=datetime.date(2024, 1, 1),
            end_date=datetime.date(2024, 1, 5),
            output_dir=tmp_path,
        )


def test_fetch_intraday_raises(stooq_ingestor: StooqIngestor, tmp_path: Path):
    """Intraday is not supported."""
    import datetime

    with pytest.raises(IngestorError, match="intraday"):
        stooq_ingestor.fetch_intraday(
            symbol="AAPL",
            start_date=datetime.date(2024, 1, 1),
            end_date=datetime.date(2024, 1, 5),
            interval="5m",
            output_dir=tmp_path,
        )
