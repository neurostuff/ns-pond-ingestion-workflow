"""Extractor interfaces and registry."""

from .base import Extractor, ExtractorRegistry, ExtractorContext, DownloadError


def register_builtin_extractors(registry: ExtractorRegistry) -> None:
    """Register built-in extractor implementations."""
    from .pubget_extractor import PubgetExtractor
    from .ace_extractor import ACEExtractor
    from .elsevier_extractor import ElsevierExtractor

    registry.register(PubgetExtractor)
    registry.register(ElsevierExtractor)
    registry.register(ACEExtractor)


__all__ = [
    "Extractor",
    "ExtractorRegistry",
    "ExtractorContext",
    "DownloadError",
    "register_builtin_extractors",
]
