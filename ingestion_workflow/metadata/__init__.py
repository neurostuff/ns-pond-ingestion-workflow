"""Metadata acquisition clients."""

from .base import MetadataClient, MetadataFetchError
from .enrichment import MetadataEnricher

__all__ = ["MetadataClient", "MetadataFetchError", "MetadataEnricher"]
