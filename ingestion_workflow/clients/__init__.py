"""HTTP clients for external literature services.

Imported lazily: the OpenAI SDK alone costs ~0.5s to import, and most commands
never make an LLM call.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

_LAZY = {
    "CoordinateParsingClient": ".coordinate_parsing",
    "GenericLLMClient": ".llm",
    "OpenAlexClient": ".openalex",
    "PubMedClient": ".pubmed",
    "SemanticScholarClient": ".semantic_scholar",
}

__all__ = sorted(_LAZY)


def __getattr__(name: str) -> Any:
    if name in _LAZY:
        return getattr(import_module(_LAZY[name], __name__), name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(__all__)
