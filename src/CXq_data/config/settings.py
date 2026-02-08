"""Typed configuration models using pydantic-settings."""

from __future__ import annotations

import datetime
from pathlib import Path

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

try:
    from pydantic_settings import TomlConfigSettingsSource

    _HAS_TOML = True
except ImportError:
    _HAS_TOML = False


class SourceAlphaVantageSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="AV_")

    api_key: SecretStr = Field(default=SecretStr(""), description="Alpha Vantage API key")
    calls_per_minute: int = Field(default=5)
    calls_per_day: int = Field(default=25)
    base_url: str = Field(default="https://www.alphavantage.co/query")


class SourceYFinanceSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="YF_")

    calls_per_minute: int = Field(default=2, description="Self-imposed rate limit")
    proxy: str | None = Field(default=None, description="Optional HTTP proxy")


class SourceStooqSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="STOOQ_")

    calls_per_minute: int = Field(default=10, description="Self-imposed rate limit")
    base_url: str = Field(default="https://stooq.com/q/d/l/")
    symbol_suffix: str = Field(default=".us", description="Suffix for US equities")


class UniverseSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="UNIVERSE_")

    symbols: list[str] = Field(
        default=["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA"],
        description="Default symbol universe",
    )
    default_start: datetime.date = Field(
        default=datetime.date(2020, 1, 1),
        description="Default backfill start date",
    )
    frequency: str = Field(default="daily", description="Default data frequency")


class StorageSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="STORAGE_")

    data_root: Path = Field(default=Path("data"), description="Root directory for all data")
    duckdb_filename: str = Field(default="CXq_data.duckdb")

    @property
    def raw_dir(self) -> Path:
        return self.data_root / "raw"

    @property
    def processed_dir(self) -> Path:
        return self.data_root / "processed"

    @property
    def duckdb_path(self) -> Path:
        return self.data_root / self.duckdb_filename


class AppSettings(BaseSettings):
    """Top-level settings composing all sub-settings."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_nested_delimiter="__",
        toml_file="config.toml",
    )

    alpha_vantage: SourceAlphaVantageSettings = Field(
        default_factory=SourceAlphaVantageSettings,
    )
    yfinance: SourceYFinanceSettings = Field(
        default_factory=SourceYFinanceSettings,
    )
    stooq: SourceStooqSettings = Field(default_factory=SourceStooqSettings)
    universe: UniverseSettings = Field(default_factory=UniverseSettings)
    storage: StorageSettings = Field(default_factory=StorageSettings)
    log_level: str = Field(default="INFO")

    @classmethod
    def settings_customise_sources(cls, settings_cls, **kwargs):  # type: ignore[override]
        sources = (
            kwargs["env_settings"],
            kwargs["dotenv_settings"],
        )
        if _HAS_TOML:
            sources += (TomlConfigSettingsSource(settings_cls),)
        sources += (kwargs["init_settings"],)
        return sources
