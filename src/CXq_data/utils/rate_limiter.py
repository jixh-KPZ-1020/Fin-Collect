"""Token-bucket rate limiter for API calls."""

from __future__ import annotations

import time

from CXq_data.ingestors.base import RateLimit


class RateLimiter:
    """Simple token-bucket rate limiter that blocks until a call is allowed."""

    def __init__(self, limit: RateLimit) -> None:
        self._interval = 60.0 / limit.calls_per_minute
        self._daily_limit = limit.calls_per_day
        self._daily_count = 0
        self._last_call: float = 0.0

    def wait(self) -> None:
        """Block until the next API call is allowed."""
        if self._daily_limit is not None and self._daily_count >= self._daily_limit:
            raise RuntimeError(
                f"Daily rate limit reached ({self._daily_limit} calls/day)"
            )

        now = time.monotonic()
        elapsed = now - self._last_call
        if elapsed < self._interval:
            time.sleep(self._interval - elapsed)

        self._last_call = time.monotonic()
        self._daily_count += 1

    def reset_daily(self) -> None:
        """Reset the daily call counter."""
        self._daily_count = 0
