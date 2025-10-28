"""Base classes for paper extractors."""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Type

from ingestion_workflow.models import DownloadResult, Identifier

if False:  # pragma: no cover
    from ingestion_workflow.storage import StorageManager
    from ingestion_workflow.config import Settings


class DownloadError(RuntimeError):
    """Raised when an extractor cannot download a requested paper."""


@dataclass
class ExtractorContext:
    """Runtime context injected into extractors."""

    storage: "StorageManager"
    settings: "Settings"


class Extractor(ABC):
    """Abstract interface for download extractors."""

    name: str

    def __init__(self, context: ExtractorContext):
        self.context = context

    @abstractmethod
    def supports(self, identifier: Identifier) -> bool:
        """Return True if the extractor can attempt this identifier."""

    @abstractmethod
    def download(self, identifier: Identifier) -> DownloadResult:
        """Execute download logic and return the resulting artifacts."""


class ExtractorRegistry:
    """Registry holding extractor implementations in execution order."""

    def __init__(self):
        self._registry: Dict[str, Type[Extractor]] = {}

    def register(self, extractor_cls: Type[Extractor]) -> None:
        if extractor_cls.name in self._registry:
            raise ValueError(f"Extractor '{extractor_cls.name}' already registered")
        self._registry[extractor_cls.name] = extractor_cls

    def get(self, name: str) -> Optional[Type[Extractor]]:
        return self._registry.get(name)

    def available(self) -> List[str]:
        return list(self._registry)

    def instantiate_ordered(
        self, names: Iterable[str], context: ExtractorContext
    ) -> List[Extractor]:
        instances: List[Extractor] = []
        for name in names:
            extractor_cls = self.get(name)
            if not extractor_cls:
                continue
            instances.append(extractor_cls(context))
        return instances
