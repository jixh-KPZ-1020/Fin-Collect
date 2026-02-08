"""Trading calendar and date utilities."""

from __future__ import annotations

import datetime


def last_trading_day(ref_date: datetime.date | None = None) -> datetime.date:
    """Return the most recent trading day (Mon-Fri, not accounting for holidays).

    For a more accurate calendar, install pandas-market-calendars.
    """
    if ref_date is None:
        ref_date = datetime.date.today()

    # Walk backwards from ref_date until we hit a weekday
    d = ref_date
    while d.weekday() >= 5:  # 5=Saturday, 6=Sunday
        d -= datetime.timedelta(days=1)
    return d


def trading_days_between(
    start: datetime.date, end: datetime.date
) -> list[datetime.date]:
    """Return weekdays between start and end (inclusive). Excludes weekends."""
    days = []
    current = start
    while current <= end:
        if current.weekday() < 5:
            days.append(current)
        current += datetime.timedelta(days=1)
    return days
