"""Decide whether a table is worth sending to the coordinate parser.

The deterministic parser (`pubget._coordinates`) only reports coordinates from
tables whose columns it can map to x/y/z. It misses tables whose structure
survived extraction imperfectly -- which is the common case for PDF-derived
tables, and the reason beast-proxy's audit logs list articles with coordinate
tables but zero extracted coordinates.

These heuristics are deliberately permissive: a false positive costs one LLM
call, a false negative loses the article's coordinates entirely.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover - typing only
    from ingestion_workflow.models import ExtractedTable

# Wording that signals a table reports stereotactic results.
_COORDINATE_WORDS = re.compile(
    r"\b("
    r"mni|talairach|tal\b|stereotactic|coordinate|"
    r"peak|local\s+maxim|cluster|voxel|"
    r"activation|deactivation|significant\s+(?:region|cluster)"
    r")",
    re.IGNORECASE,
)

# An x/y/z column trio, however the extractor spelled the headers.
_XYZ_HEADERS = (
    re.compile(r"(^|[^a-z])x([^a-z]|$)", re.IGNORECASE),
    re.compile(r"(^|[^a-z])y([^a-z]|$)", re.IGNORECASE),
    re.compile(r"(^|[^a-z])z([^a-z]|$)", re.IGNORECASE),
)

# Three signed integers in a row, in plausible stereotactic range.
_COORDINATE_TRIPLE = re.compile(
    r"(?<![\d.-])-?\d{1,3}(?![\d.])\D{1,4}-?\d{1,3}(?![\d.])\D{1,4}-?\d{1,3}(?![\d.])"
)
_MAX_COORDINATE_MAGNITUDE = 120


def looks_like_coordinate_table(table: "ExtractedTable", *, sample_lines: int = 40) -> bool:
    """Whether a table plausibly reports stereotactic coordinates.

    Used only for tables the deterministic parser found nothing in; a table
    that already has coordinates does not need guessing about.
    """
    context = f"{table.caption or ''}\n{table.footer or ''}"
    content = _read_sample(table, sample_lines)

    if not _COORDINATE_WORDS.search(context) and not _COORDINATE_WORDS.search(content):
        return False

    header = content.split("\n", 1)[0] if content else ""
    if _has_xyz_headers(header):
        return True

    return _has_coordinate_triple(content)


def _read_sample(table: "ExtractedTable", sample_lines: int) -> str:
    path = Path(table.raw_content_path)
    if not path.exists():
        return ""
    try:
        with path.open(encoding="utf-8", errors="replace") as handle:
            return "".join(line for _, line in zip(range(sample_lines), handle))
    except OSError:
        return ""


def _has_xyz_headers(header: str) -> bool:
    return all(pattern.search(header) for pattern in _XYZ_HEADERS)


def _has_coordinate_triple(content: str) -> bool:
    for match in _COORDINATE_TRIPLE.finditer(content):
        values = [int(value) for value in re.findall(r"-?\d{1,3}", match.group(0))[:3]]
        if len(values) == 3 and all(
            abs(value) <= _MAX_COORDINATE_MAGNITUDE for value in values
        ):
            # All-positive small integers are as likely to be counts or ages,
            # so require at least one negative -- brains have a left side.
            if any(value < 0 for value in values):
                return True
    return False


__all__ = ["looks_like_coordinate_table"]
