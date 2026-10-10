"""Shared helpers for extractor implementations."""

from __future__ import annotations

import hashlib
import math
import re
from pathlib import Path
from typing import Any, Optional

from ingestion_workflow.models import (
    Coordinate,
    CoordinateSpace,
    DownloadResult,
    DownloadSource,
    DownloadedFile,
    FileType,
)


DEFAULT_CONTENT_TYPES: dict[FileType, str] = {
    FileType.XML: "application/xml",
    FileType.CSV: "text/csv",
    FileType.JSON: "application/json",
    FileType.HTML: "text/html",
    FileType.TEXT: "text/plain",
    FileType.PDF: "application/pdf",
    FileType.BINARY: "application/octet-stream",
}


def build_downloaded_file(
    path: Path,
    file_type: FileType,
    *,
    source: DownloadSource,
    content_type: str | None = None,
) -> DownloadedFile:
    """Create a DownloadedFile entry with consistent hashing and content-type."""
    payload = path.read_bytes()
    md5_hash = hashlib.md5(payload).hexdigest()
    resolved_content_type = content_type or DEFAULT_CONTENT_TYPES.get(
        file_type,
        DEFAULT_CONTENT_TYPES[FileType.BINARY],
    )
    return DownloadedFile(
        file_path=path,
        file_type=file_type,
        content_type=resolved_content_type,
        source=source,
        md5_hash=md5_hash,
    )


def build_failure_extraction(
    download_result: DownloadResult,
    source: DownloadSource,
    message: str,
    full_text_path: Path | None = None,
) -> "ExtractedContent":
    from ingestion_workflow.models import ExtractedContent  # local import to avoid cycles

    return ExtractedContent(
        slug=download_result.identifier.slug,
        source=source,
        identifier=download_result.identifier,
        full_text_path=full_text_path,
        tables=[],
        has_coordinates=False,
        error_message=message,
    )


def safe_hash_stem(slug: str | None) -> str:
    """Create a filesystem-safe directory stem from an identifier slug."""
    candidate = slug or ""
    sanitized = re.sub(r"[^A-Za-z0-9_-]+", "-", candidate).strip("-_")
    if sanitized:
        return sanitized.lower()
    digest = hashlib.sha256(candidate.encode("utf-8")).hexdigest()
    return digest[:16]


def sanitize_table_id(
    table_id: Optional[str],
    table_label: Optional[str],
    index: int,
) -> str:
    """Normalize table identifiers used for filenames."""
    fallback = f"table-{index + 1:03d}"
    candidate = table_id or table_label or fallback
    sanitized = re.sub(r"[^A-Za-z0-9_-]+", "-", candidate).strip("-")
    return sanitized.lower() or fallback


def coordinate_space_from_guess(guess: Optional[str]) -> Optional[CoordinateSpace]:
    """Map heuristic guesses to the CoordinateSpace enum.

    None when the guess found nothing (`UNKNOWN`): not stated is null, never
    MNI and never `OTHER`, which is for a space stated but neither MNI nor TAL.
    """
    if not guess or str(guess).strip().upper() == "UNKNOWN":
        return None
    return CoordinateSpace.from_label(guess) or CoordinateSpace.OTHER


def coordinate_from_row(
    row: Any,
    space: CoordinateSpace,
) -> Optional[Coordinate]:
    """Build a Coordinate from a mapping/DataFrame row."""
    try:
        x_val = float(row["x"])
        y_val = float(row["y"])
        z_val = float(row["z"])
    except (KeyError, TypeError, ValueError):
        return None

    if any(math.isnan(value) for value in (x_val, y_val, z_val)):
        return None

    return Coordinate(
        x=x_val,
        y=y_val,
        z=z_val,
        space=space,
    )


def parse_table_number(label: Optional[str]) -> Optional[int]:
    """Extract an integer table number from a label."""
    if not label:
        return None
    match = re.search(r"(\d+)", label)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None


__all__ = [
    "DEFAULT_CONTENT_TYPES",
    "build_downloaded_file",
    "build_failure_extraction",
    "coordinate_from_row",
    "coordinate_space_from_guess",
    "parse_table_number",
    "safe_hash_stem",
    "sanitize_table_id",
]


# -- minus signs ---------------------------------------------------------
#
# Journals do not write a coordinate's minus as a hyphen. Measured over 1,200
# tables triage passed, elsevier uses U+2212 MINUS SIGN 2,000 times against 30
# ASCII hyphens, and pubget 423 against 20; ACE's document HTML writes it as
# the entity `&#x02212;`. v19's training data contains **no** U+2212 at all --
# 91.6% of rows use an ASCII hyphen -- so normalising here does not shift the
# model off its distribution, it puts it back on.
#
# Two failures came of not doing this. An entity minus survived tag-stripping
# as the literal text `&#x02212;`, so the sign vanished and a left-hemisphere
# focus was stored on the right: 13.3% of passed tables gained negative
# numbers once decoded. And the same table rendered two ways disagreed about
# its own numbers, so the duplicate check missed it.

_MINUS_CHARS = (
    "−",  # minus sign
    "‐",  # hyphen
    "‑",  # non-breaking hyphen
    "‒",  # figure dash
    "–",  # en dash
    "—",  # em dash
    "―",  # horizontal bar
    "⁃",  # hyphen bullet
    "⁻",  # superscript minus
    "₋",  # subscript minus
    "﹣",  # small hyphen-minus
    "－",  # fullwidth hyphen-minus
)

_MINUS_TABLE = dict.fromkeys(map(ord, _MINUS_CHARS), "-")

#: Only the minus entities are decoded. A blanket `html.unescape` would turn
#: `&lt;0.05&gt;` into `<0.05>`, which the serialiser then strips as a tag --
#: `&lt;` appears 1,357 times and `&#x0003c;` 739 in the same sample.
_MINUS_ENTITY = re.compile(
    "&(?:"
    + "|".join(
        [r"#x0*%x" % ord(c) for c in _MINUS_CHARS]
        + [r"#0*%d" % ord(c) for c in _MINUS_CHARS]
        + ["minus", "ndash", "mdash", "dash", "hyphen", "horbar"]
    )
    + ");",
    re.IGNORECASE,
)


def normalize_minus(markup: str) -> str:
    """Every way a paper writes a minus, turned into an ASCII hyphen.

    Applied to markup, before tags are stripped, so the entity forms are
    caught too. Nothing else is decoded -- see `_MINUS_ENTITY`.
    """
    if not markup:
        return markup
    return _MINUS_ENTITY.sub("-", markup).translate(_MINUS_TABLE)
