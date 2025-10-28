"""Shared data models for the ingestion workflow."""

from .core import (
    Identifier,
    IdentifierSet,
    DownloadArtifact,
    DownloadResult,
    TableArtifact,
    ProcessedPaper,
    MetadataRecord,
    CoordinateSet,
    NeurostorePayload,
    RunManifest,
)

__all__ = [
    "Identifier",
    "IdentifierSet",
    "DownloadArtifact",
    "DownloadResult",
    "TableArtifact",
    "ProcessedPaper",
    "MetadataRecord",
    "CoordinateSet",
    "NeurostorePayload",
    "RunManifest",
]
