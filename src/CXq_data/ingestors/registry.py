# Ingestor registry — maps source keys to ingestor classes.

from __future__ import annotations

from typing import TYPE_CHECKING

from CXq_data.ingestors.base import BaseIngestor, IngestorError

if TYPE_CHECKING:
    from CXq_data.config.settings import AppSettings

# Registry populated at import time by each ingestor module
_REGISTRY: dict[str, type] = {}


def register(key: str, cls: type) -> None:
    """Register an ingestor class under a source key."""
    _REGISTRY[key] = cls


def get_ingestor(key: str, settings: AppSettings) -> BaseIngestor:
    """Create an ingestor instance by source key.

    Lazy-imports ingestor modules to avoid importing unused dependencies.
    """
    # Lazy registration on first call
    if not _REGISTRY:
        _load_builtins()

    if key not in _REGISTRY:
        available = ", ".join(sorted(_REGISTRY.keys()))
        raise IngestorError(
            f"Unknown source '{key}'. Available sources: {available}"
        )

    cls = _REGISTRY[key]

    if key == "av":
        return cls(settings.alpha_vantage)  # type: ignore[return-value]
    elif key == "yf":
        return cls(settings.yfinance)  # type: ignore[return-value]
    elif key == "stooq":
        return cls(settings.stooq)  # type: ignore[return-value]
    else:
        return cls(settings)  # type: ignore[return-value]


def _load_builtins() -> None:
    """Lazy-import built-in ingestors to populate the registry."""
    from CXq_data.ingestors.yfinance import YFinanceIngestor

    register("yf", YFinanceIngestor)

    try:
        from CXq_data.ingestors.alpha_vantage import AlphaVantageIngestor

        register("av", AlphaVantageIngestor)
    except ImportError:
        pass

    from CXq_data.ingestors.stooq import StooqIngestor

    register("stooq", StooqIngestor)


def available_sources() -> list[str]:
    """Return list of registered source keys."""
    if not _REGISTRY:
        _load_builtins()
    return sorted(_REGISTRY.keys())
