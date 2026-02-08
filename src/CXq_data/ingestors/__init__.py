"""Data source ingestors."""

from CXq_data.ingestors.registry import available_sources, get_ingestor

__all__ = ["get_ingestor", "available_sources"]
