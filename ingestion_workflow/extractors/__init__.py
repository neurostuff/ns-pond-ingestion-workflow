"""Extractor interfaces and registry."""

from .base import Extractor, ExtractorRegistry, ExtractorContext, DownloadError


def register_builtin_extractors(registry: ExtractorRegistry) -> None:
    """Register built-in extractor implementations."""
    from .pubget_extractor import PubgetExtractor

    registry.register(PubgetExtractor)


__all__ = [
    "Extractor",
    "ExtractorRegistry",
    "ExtractorContext",
    "DownloadError",
    "register_builtin_extractors",
]
