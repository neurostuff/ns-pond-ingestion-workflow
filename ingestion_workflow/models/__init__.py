"""Data models shared across the pipeline."""

from .analysis import (
    Analysis,
    AnalysisCollection,
    Condition,
    Contrast,
    Coordinate,
    CoordinatePoint,
    CoordinateSpace,
    CreateAnalysesResult,
    Image,
    ParseAnalysesOutput,
    ParsedAnalysis,
    PointsValue,
)
from .download import DownloadedFile, DownloadResult, DownloadSource, FileType
from .extract import ArticleExtractionBundle, ExtractedContent, ExtractedTable
from .ids import Identifier, IdentifierExpansion, Identifiers
from .metadata import ArticleMetadata, Author, is_sufficient, merge_metadata_from_sources
from .upload import (
    BaseStudyPayload,
    PreparedAnalysis,
    StudyPayload,
    TablePayload,
    UploadOutcome,
    UploadWorkItem,
)

#: Kept for callers that predate the rename to ExtractedContent.
ExtractionResult = ExtractedContent

__all__ = [
    "Analysis",
    "AnalysisCollection",
    "ArticleExtractionBundle",
    "ArticleMetadata",
    "Author",
    "BaseStudyPayload",
    "Condition",
    "Contrast",
    "Coordinate",
    "CoordinatePoint",
    "CoordinateSpace",
    "CreateAnalysesResult",
    "DownloadResult",
    "DownloadSource",
    "DownloadedFile",
    "ExtractedContent",
    "ExtractedTable",
    "ExtractionResult",
    "FileType",
    "Identifier",
    "IdentifierExpansion",
    "Identifiers",
    "Image",
    "ParseAnalysesOutput",
    "ParsedAnalysis",
    "PointsValue",
    "PreparedAnalysis",
    "StudyPayload",
    "TablePayload",
    "UploadOutcome",
    "UploadWorkItem",
    "is_sufficient",
    "merge_metadata_from_sources",
]
