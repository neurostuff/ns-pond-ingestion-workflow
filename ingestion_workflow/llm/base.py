"""Base interface for LLM-driven coordinate extraction."""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Iterable, List

from ingestion_workflow.models import CoordinateSet, TableArtifact


class CoordinateExtractionError(RuntimeError):
    """Raised when coordinate extraction fails."""


@dataclass
class ExtractionRequest:
    table: TableArtifact
    context_text: str


class CoordinateExtractor(ABC):
    """Abstract LLM interface for coordinate extraction."""

    name: str

    @abstractmethod
    def extract(self, requests: Iterable[ExtractionRequest]) -> List[CoordinateSet]:
        """Process one or more table extraction requests."""

    def describe(self) -> Dict[str, str]:
        """Return metadata about the extractor implementation."""
        return {"name": getattr(self, "name", self.__class__.__name__)}
