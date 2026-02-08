"""Config loading with layered resolution: env > .env > config.toml > defaults."""

from __future__ import annotations

from functools import lru_cache

from CXq_data.config.settings import AppSettings


@lru_cache(maxsize=1)
def get_settings() -> AppSettings:
    """Load and cache application settings."""
    return AppSettings()
