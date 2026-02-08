"""Tests for configuration loading."""

from __future__ import annotations

from CXq_data.config.settings import AppSettings, StorageSettings


def test_default_settings():
    """AppSettings can be created with defaults."""
    settings = AppSettings()
    assert settings.log_level == "INFO"
    assert len(settings.universe.symbols) > 0
    assert settings.storage.data_root.name == "data"


def test_storage_paths():
    """StorageSettings computes derived paths correctly."""
    s = StorageSettings()
    assert s.raw_dir.name == "raw"
    assert s.processed_dir.name == "processed"
    assert s.duckdb_path.name == "CXq_data.duckdb"
