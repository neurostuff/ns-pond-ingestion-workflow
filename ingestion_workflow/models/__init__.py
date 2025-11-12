"""Convenience re-exports for core workflow data models."""

from .analysis import (
    Analysis,
    AnalysisCollection,
    Condition,
    Contrast,
    Coordinate,
    CoordinateSpace,
    CreateAnalysesResult,
    Image,
)
from .download import DownloadResult, DownloadSource, DownloadedFile, FileType
from .extract import ArticleExtractionBundle, ExtractedContent, ExtractedTable
from .ids import Identifier, IdentifierExpansion, Identifiers
from .metadata import (
    ArticleMetadata,
    Author,
    merge_metadata_from_sources,
)
from .cache import (
    CACHE_SCHEMA_VERSION,
    CacheEnvelope,
    CacheIndex,
    CreateAnalysesResultEntry,
    CreateAnalysesResultIndex,
    DownloadCacheEntry,
    DownloadIndex,
    ExtractionResultEntry,
    ExtractionResultIndex,
    IdentifierCacheEntry,
    IdentifierCacheIndex,
    MetadataCache,
    MetadataCacheIndex,
)

# Align with earlier interface expectations.
ExtractionResult = ExtractedContent
ExtractionIndex = ExtractionResultIndex

__all__ = [
    "Analysis",
    "AnalysisCollection",
    "ArticleExtractionBundle",
    "ArticleMetadata",
    "Author",
    "CACHE_SCHEMA_VERSION",
    "CacheEnvelope",
    "CacheIndex",
    "Condition",
    "Contrast",
    "Coordinate",
    "CoordinateSpace",
    "CreateAnalysesResult",
    "CreateAnalysesResultEntry",
    "CreateAnalysesResultIndex",
    "DownloadCacheEntry",
    "DownloadIndex",
    "DownloadResult",
    "DownloadSource",
    "DownloadedFile",
    "ExtractionIndex",
    "ExtractionResult",
    "ExtractedContent",
    "ExtractedTable",
    "ExtractionResultEntry",
    "ExtractionResultIndex",
    "FileType",
    "Identifier",
    "IdentifierExpansion",
    "Identifiers",
    "IdentifierCacheEntry",
    "IdentifierCacheIndex",
    "Image",
    "MetadataCache",
    "MetadataCacheIndex",
    "merge_metadata_from_sources",
]
