"""Base interface for metadata fetching clients."""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

from ingestion_workflow.models import Identifier, MetadataRecord


class MetadataFetchError(RuntimeError):
    """Raised when metadata could not be retrieved."""


class MetadataClient(ABC):
    """Abstract metadata client."""

    name: str

    @abstractmethod
    def fetch(self, identifier: Identifier) -> Optional[MetadataRecord]:
        """
        Fetch metadata for the identifier.

        Implementations should return ``None`` when no metadata can be located,
        and raise ``MetadataFetchError`` for hard failures.
        """
