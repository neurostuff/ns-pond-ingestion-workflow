"""Service layer abstractions for the ingestion workflow."""

from __future__ import annotations

from importlib import import_module

from . import cache, logging
from .id_lookup import (
    IDLookupService,
    OpenAlexIDLookupService,
    PubMedIDLookupService,
    SemanticScholarIDLookupService,
)
from .search import ArticleSearchService, PubMedSearchService

__all__ = [
    "ArticleSearchService",
    "cache",
    "ExportService",
    "IDLookupService",
    "logging",
    "OpenAlexIDLookupService",
    "PubMedIDLookupService",
    "PubMedSearchService",
    "SemanticScholarIDLookupService",
]


def __getattr__(name: str):
    if name == "ExportService":
        module = import_module(".export", __name__)
        return getattr(module, "ExportService")
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
