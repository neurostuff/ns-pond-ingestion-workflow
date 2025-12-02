"""Shared helpers for neuroimaging statistic kinds."""

from __future__ import annotations

from typing import Any, Iterable, Optional


ALLOWED_STATISTIC_KINDS = frozenset(
    {
        "Z",
        "T",
        "F",
        "R",
        "P",
        "B",
        "OTHER",
    }
)


def normalize_statistic_kind(kind: Any) -> Optional[str]:
    """Return one of the allowed statistic kinds for the provided hint."""
    if kind is None:
        return None
    normalized = str(kind).strip()
    if not normalized:
        return None
    upper_value = normalized.upper()
    if upper_value in ALLOWED_STATISTIC_KINDS:
        return upper_value
    lower = normalized.lower()
    if "z" in lower:
        return "Z"
    if "t" in lower or "stat" in lower:
        return "T"
    if "f" in lower:
        return "F"
    if "r" in lower or "correlation" in lower:
        return "R"
    if lower.startswith("p"):
        return "P"
    if "beta" in lower:
        return "B"
    return "OTHER"
