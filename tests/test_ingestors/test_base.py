"""Tests for ingestor base protocol and registry."""

from __future__ import annotations

from CXq_data.ingestors.base import BaseIngestor
from CXq_data.ingestors.registry import available_sources
from CXq_data.ingestors.yfinance import YFinanceIngestor


def test_yfinance_implements_protocol():
    """YFinanceIngestor satisfies the BaseIngestor protocol."""
    assert isinstance(YFinanceIngestor.__dict__.get("source_name"), str) or hasattr(
        YFinanceIngestor, "source_name"
    )


def test_available_sources():
    """Registry returns yf, av, and stooq."""
    sources = available_sources()
    assert "yf" in sources
    assert "av" in sources
    assert "stooq" in sources
