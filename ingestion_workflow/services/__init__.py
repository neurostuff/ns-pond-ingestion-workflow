"""Services: the outside world, behind narrow interfaces."""

from __future__ import annotations

from importlib import import_module
from typing import Any

#: Imported lazily so that pulling in one service does not drag in every
#: optional dependency (docling, sqlalchemy, seleniumbase) at import time.
_LAZY = {
    "ExportService": ".export",
    "IDLookupService": ".id_lookup",
    "MetadataService": ".metadata",
    "OpenAlexIDLookupService": ".id_lookup",
    "PubMedIDLookupService": ".id_lookup",
    "PubMedSearchService": ".search",
    "SemanticScholarIDLookupService": ".id_lookup",
    "UploadService": ".upload",
}

__all__ = sorted([*_LAZY, "logging", "nspond"])


def __getattr__(name: str) -> Any:
    if name in _LAZY:
        return getattr(import_module(_LAZY[name], __name__), name)
    if name in ("logging", "nspond"):
        return import_module(f".{name}", __name__)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
