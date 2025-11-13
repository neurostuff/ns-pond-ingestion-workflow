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


def coordinate_space_from_guess(guess: Optional[str]) -> CoordinateSpace:
    """Map heuristic guesses to the CoordinateSpace enum."""
    if not guess:
        return CoordinateSpace.OTHER
    normalized = guess.strip().upper()
    if normalized == "MNI":
        return CoordinateSpace.MNI
    if normalized in {"TAL", "TALAIRACH"}:
        return CoordinateSpace.TALAIRACH
    return CoordinateSpace.OTHER


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
