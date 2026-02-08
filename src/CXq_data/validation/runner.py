"""Validation runner — orchestrates all checks and produces reports."""

from __future__ import annotations

import datetime

import polars as pl

from CXq_data.validation.checks import (
    check_ohlc_consistency,
    check_price_sanity,
    check_stale_data,
    check_trading_day_gaps,
)
from CXq_data.validation.models import CheckReport

ALL_CHECKS = [
    check_trading_day_gaps,
    check_price_sanity,
    check_stale_data,
    check_ohlc_consistency,
]


def run_all_checks(df: pl.DataFrame, symbol: str) -> CheckReport:
    """Run all validation checks against a DataFrame and return a report."""
    results = [check(df, symbol) for check in ALL_CHECKS]

    return CheckReport(
        symbol=symbol,
        timestamp=datetime.datetime.now(datetime.timezone.utc),
        results=results,
    )
