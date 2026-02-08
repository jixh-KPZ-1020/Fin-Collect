"""Individual data quality check functions."""

from __future__ import annotations

import datetime

import polars as pl

from CXq_data.utils.dates import last_trading_day, trading_days_between
from CXq_data.validation.models import CheckResult, CheckStatus


def check_trading_day_gaps(df: pl.DataFrame, symbol: str) -> CheckResult:
    """Detect missing trading days by comparing against weekday calendar.

    Warns on gaps of 1-3 days, fails on gaps > 3 consecutive trading days.
    """
    if df.is_empty():
        return CheckResult(
            check_name="trading_day_gaps",
            status=CheckStatus.FAIL,
            message=f"No data for {symbol}",
        )

    dates = df.select("date").to_series().sort()
    start = dates.min()
    end = dates.max()

    expected = set(trading_days_between(start, end))
    actual = set(dates.to_list())
    missing = sorted(expected - actual)

    if not missing:
        return CheckResult(
            check_name="trading_day_gaps",
            status=CheckStatus.PASS,
            message=f"No gaps found ({len(actual)} trading days)",
        )

    # Find consecutive gap runs
    max_gap = 1
    current_gap = 1
    for i in range(1, len(missing)):
        delta = (missing[i] - missing[i - 1]).days
        if delta <= 3:  # Within a long weekend
            current_gap += 1
            max_gap = max(max_gap, current_gap)
        else:
            current_gap = 1

    status = CheckStatus.FAIL if max_gap > 3 else CheckStatus.WARN

    return CheckResult(
        check_name="trading_day_gaps",
        status=status,
        message=f"{len(missing)} missing trading day(s), longest gap: {max_gap}",
        details={"missing_dates": [d.isoformat() for d in missing[:20]]},
    )


def check_price_sanity(df: pl.DataFrame, symbol: str) -> CheckResult:
    """Validate price data integrity.

    Checks: no negative prices, high >= low, open/close within [low, high],
    volume >= 0, no zero close prices.
    """
    issues = []

    negative = df.filter(
        (pl.col("open") < 0)
        | (pl.col("high") < 0)
        | (pl.col("low") < 0)
        | (pl.col("close") < 0)
    )
    if len(negative) > 0:
        issues.append(f"{len(negative)} rows with negative prices")

    high_low = df.filter(pl.col("high") < pl.col("low"))
    if len(high_low) > 0:
        issues.append(f"{len(high_low)} rows where high < low")

    zero_close = df.filter(pl.col("close") == 0)
    if len(zero_close) > 0:
        issues.append(f"{len(zero_close)} rows with zero close")

    neg_volume = df.filter(pl.col("volume") < 0)
    if len(neg_volume) > 0:
        issues.append(f"{len(neg_volume)} rows with negative volume")

    if not issues:
        return CheckResult(
            check_name="price_sanity",
            status=CheckStatus.PASS,
            message="All price sanity checks passed",
        )

    return CheckResult(
        check_name="price_sanity",
        status=CheckStatus.FAIL,
        message="; ".join(issues),
        details={"issue_count": len(issues)},
    )


def check_stale_data(df: pl.DataFrame, symbol: str) -> CheckResult:
    """Check if data is up to date relative to the last trading day."""
    if df.is_empty():
        return CheckResult(
            check_name="stale_data",
            status=CheckStatus.FAIL,
            message=f"No data for {symbol}",
        )

    max_date = df.select("date").to_series().max()
    last_td = last_trading_day()
    days_behind = len(trading_days_between(max_date, last_td)) - 1

    if days_behind <= 1:
        status = CheckStatus.PASS
        msg = f"Data is current (latest: {max_date})"
    elif days_behind <= 5:
        status = CheckStatus.WARN
        msg = f"Data is {days_behind} trading day(s) behind (latest: {max_date})"
    else:
        status = CheckStatus.FAIL
        msg = f"Data is {days_behind} trading day(s) behind (latest: {max_date})"

    return CheckResult(
        check_name="stale_data",
        status=status,
        message=msg,
        details={"latest_date": max_date.isoformat(), "days_behind": days_behind},
    )


def check_ohlc_consistency(df: pl.DataFrame, symbol: str) -> CheckResult:
    """Check for suspicious OHLC patterns.

    Detects: all four OHLC identical (placeholder data), volume zero with
    price movement > 1%.
    """
    issues = []

    flat = df.filter(
        (pl.col("open") == pl.col("high"))
        & (pl.col("high") == pl.col("low"))
        & (pl.col("low") == pl.col("close"))
    )
    if len(flat) > 0:
        issues.append(f"{len(flat)} rows with identical OHLC (possible placeholder)")

    zero_vol_movement = df.filter(
        (pl.col("volume") == 0)
        & (((pl.col("close") - pl.col("open")).abs() / pl.col("open")) > 0.01)
    )
    if len(zero_vol_movement) > 0:
        issues.append(f"{len(zero_vol_movement)} rows with zero volume but >1% price movement")

    if not issues:
        return CheckResult(
            check_name="ohlc_consistency",
            status=CheckStatus.PASS,
            message="OHLC consistency checks passed",
        )

    return CheckResult(
        check_name="ohlc_consistency",
        status=CheckStatus.WARN,
        message="; ".join(issues),
    )
