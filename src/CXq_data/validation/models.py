"""Data models for validation results."""

from __future__ import annotations

import datetime
from enum import Enum

from pydantic import BaseModel


class CheckStatus(str, Enum):
    PASS = "pass"
    WARN = "warn"
    FAIL = "fail"


class CheckResult(BaseModel):
    """Result of a single data quality check."""

    check_name: str
    status: CheckStatus
    message: str
    details: dict | None = None


class CheckReport(BaseModel):
    """Aggregated validation report for a symbol."""

    symbol: str
    timestamp: datetime.datetime
    results: list[CheckResult]

    @property
    def overall_status(self) -> CheckStatus:
        statuses = {r.status for r in self.results}
        if CheckStatus.FAIL in statuses:
            return CheckStatus.FAIL
        if CheckStatus.WARN in statuses:
            return CheckStatus.WARN
        return CheckStatus.PASS
