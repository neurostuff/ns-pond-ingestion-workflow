"""Extraction pipeline data models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Mapping, Optional

from .download import DownloadSource


@dataclass
class ExtractedTable:
    """Metadata about an extracted table."""

    table_id: str
    raw_content_path: Path
    table_number: Optional[int] = None
    caption: str = ""
    footer: str = ""
    contains_coordinates: bool = False

    def to_dict(self) -> Dict[str, object]:
        return {
            "table_id": self.table_id,
            "raw_content_path": str(self.raw_content_path),
            "table_number": self.table_number,
            "caption": self.caption,
            "footer": self.footer,
            "contains_coordinates": self.contains_coordinates,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ExtractedTable":
        return cls(
            table_id=str(payload["table_id"]),
            raw_content_path=Path(str(payload["raw_content_path"])),
            table_number=payload.get("table_number"),
            caption=str(payload.get("caption", "")),
            footer=str(payload.get("footer", "")),
            contains_coordinates=bool(
                payload.get("contains_coordinates", False)
            ),
        )


@dataclass
class ExtractedContent:
    """All content extracted from a downloaded article."""

    hash_id: str
    source: DownloadSource
    full_text_path: Optional[Path] = None
    tables: List[ExtractedTable] = field(default_factory=list)
    has_coordinates: bool = False
    extracted_at: datetime = field(default_factory=datetime.utcnow)
    error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, object]:
        return {
            "hash_id": self.hash_id,
            "source": self.source.value,
            "full_text_path": (
                str(self.full_text_path) if self.full_text_path else None
            ),
            "tables": [table.to_dict() for table in self.tables],
            "has_coordinates": self.has_coordinates,
            "extracted_at": self.extracted_at.isoformat(),
            "error_message": self.error_message,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ExtractedContent":
        tables_payload = payload.get("tables", [])
        tables = [ExtractedTable.from_dict(item) for item in tables_payload]
        return cls(
            hash_id=str(payload["hash_id"]),
            source=DownloadSource(str(payload["source"])),
            full_text_path=(
                Path(str(payload["full_text_path"]))
                if payload.get("full_text_path")
                else None
            ),
            tables=tables,
            has_coordinates=bool(payload.get("has_coordinates", False)),
            extracted_at=datetime.fromisoformat(str(payload["extracted_at"])),
            error_message=payload.get("error_message") or None,
        )


__all__ = [
    "ExtractedContent",
    "ExtractedTable",
]
