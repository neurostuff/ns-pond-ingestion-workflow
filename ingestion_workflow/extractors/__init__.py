"""Extractor interfaces and concrete implementations.

Imported lazily: `pubget` pulls in nilearn and sklearn, which costs ~3s. That
is worth paying when an extractor actually runs, and not when the CLI is only
printing `--help` or reading the catalog.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

from .base import BaseExtractor

_LAZY = {
    "ACEExtractor": ".ace_extractor",
    "ElsevierExtractor": ".elsevier_extractor",
    "PdfExtractor": ".pdf_extractor",
    "PubgetExtractor": ".pubget_extractor",
}

__all__ = ["BaseExtractor", *sorted(_LAZY)]


def __getattr__(name: str) -> Any:
    if name in _LAZY:
        return getattr(import_module(_LAZY[name], __name__), name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(__all__)
