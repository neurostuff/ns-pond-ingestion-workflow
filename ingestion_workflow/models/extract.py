"""Extraction pipeline data models."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

from .analysis import Coordinate, CoordinateSpace
from .download import DownloadSource
from .ids import Identifier
from .metadata import ArticleMetadata


@dataclass
class ExtractedTable:
    """Metadata about an extracted table."""

    table_id: str
    raw_content_path: Path
    table_number: Optional[int] = None
    caption: str = ""
    footer: str = ""
    contains_coordinates: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)
    coordinates: List[Coordinate] = field(default_factory=list)
    space: Optional[CoordinateSpace] = None

    def __post_init__(self) -> None:
        self.contains_coordinates = bool(self.coordinates)
        if not isinstance(self.metadata, dict):
            self.metadata = dict(self.metadata or {})

    def to_dict(self) -> Dict[str, object]:
        return {
            "table_id": self.table_id,
            "raw_content_path": str(self.raw_content_path),
            "table_number": self.table_number,
            "caption": self.caption,
            "footer": self.footer,
            "contains_coordinates": self.contains_coordinates,
            "metadata": self.metadata,
            "coordinates": [
                {
                    # Tables never carry other statistics; the key is left out when empty so
                    # a table's payload is what it was before the field existed.
                    **{key: value for key, value in asdict(coord).items()
                       if key != "space" and (key != "other_statistics" or value)},
                    "space": (
                        coord.space.value
                        if coord.space
                        else (self.space.value if self.space else None)
                    ),
                }
                for coord in self.coordinates
            ],
            "space": self.space.value if self.space else None,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ExtractedTable":
        metadata = payload.get("metadata") or {}
        space_value = payload.get("space")
        table_space = CoordinateSpace(str(space_value)) if space_value else None
        coordinates_payload = payload.get("coordinates", [])
        resolved_coordinates: List[Coordinate] = []
        for item in coordinates_payload:
            # An extraction stored before the flags were retired still carries them.
            coord_data = {k: v for k, v in item.items() if k not in Coordinate.NON_FIELD_KEYS}
            space = coord_data.pop("space", None)
            coord_space: Optional[CoordinateSpace] = None
            if space:
                coord_space = CoordinateSpace(str(space))
            elif table_space is not None:
                coord_space = table_space
            if coord_space is not None:
                coord_data["space"] = coord_space
            resolved_coordinates.append(Coordinate(**coord_data))
        return cls(
            table_id=str(payload["table_id"]),
            raw_content_path=Path(str(payload["raw_content_path"])),
            table_number=payload.get("table_number"),
            caption=str(payload.get("caption", "")),
            footer=str(payload.get("footer", "")),
            contains_coordinates=bool(payload.get("contains_coordinates", False)),
            metadata=dict(metadata),
            coordinates=resolved_coordinates,
            space=table_space,
        )


@dataclass
class ExtractedContent:
    """All content extracted from a downloaded article."""

    slug: str
    source: DownloadSource
    identifier: Optional[Identifier] = None
    full_text_path: Optional[Path] = None
    tables: List[ExtractedTable] = field(default_factory=list)
    has_coordinates: bool = False
    extracted_at: datetime = field(default_factory=datetime.utcnow)
    error_message: Optional[str] = None
    #: Each figure caption's `{"ids", "span"}` in the text (`extractors.figure_captions`).
    figure_captions: List[Dict[str, object]] = field(default_factory=list)
    #: How the text was made, when the extractor built it more than one way
    #: (`ace_extractor.article_text`).
    text_source: Optional[Dict[str, object]] = None
    #: The version of the copy the text was read from, when the download knew it
    #: ("submittedVersion" is a preprint).
    article_version: Optional[str] = None
    #: Set when the text was read and set aside because its layer maps math symbols to
    #: digits (`pdf_extractor.symbols_as_digits`): the counts that showed it, and how
    #: many tables went with it. Such content holds no text and no tables.
    symbols_as_digits: Optional[Dict[str, int]] = None

    def to_dict(self) -> Dict[str, object]:
        out = {
            "slug": self.slug,
            "source": self.source.value,
            "identifier": (self.identifier.to_dict() if self.identifier else None),
            "full_text_path": (str(self.full_text_path) if self.full_text_path else None),
            "tables": [table.to_dict() for table in self.tables],
            "has_coordinates": self.has_coordinates,
            "extracted_at": self.extracted_at.isoformat(),
            "error_message": self.error_message,
            "figure_captions": self.figure_captions,
        }
        if self.text_source:
            out["text_source"] = self.text_source
        if self.article_version:
            out["article_version"] = self.article_version
        if self.symbols_as_digits:
            out["symbols_as_digits"] = self.symbols_as_digits
        return out

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ExtractedContent":
        tables_payload = payload.get("tables", [])
        tables = [ExtractedTable.from_dict(item) for item in tables_payload]
        identifier_data = payload.get("identifier")
        identifier = Identifier.from_dict(identifier_data) if identifier_data else None
        return cls(
            slug=str(payload["slug"]),
            source=DownloadSource(str(payload["source"])),
            identifier=identifier,
            full_text_path=(
                Path(str(payload["full_text_path"])) if payload.get("full_text_path") else None
            ),
            tables=tables,
            has_coordinates=bool(payload.get("has_coordinates", False)),
            extracted_at=datetime.fromisoformat(str(payload["extracted_at"])),
            error_message=payload.get("error_message") or None,
            figure_captions=list(payload.get("figure_captions") or []),
            text_source=payload.get("text_source") or None,
            article_version=payload.get("article_version") or None,  # type: ignore[arg-type]
            symbols_as_digits=payload.get("symbols_as_digits") or None,  # type: ignore[arg-type]
        )


def text_from(text_source: Mapping[str, object]) -> str:
    """The extract summary's `text_from`: which way the text was built, and why it was
    built more than one way."""
    return f"{text_source['method']} ({text_source['reason']})"


@dataclass
class ArticleExtractionBundle:
    """Combined extraction output and associated metadata for an article."""

    article_data: ExtractedContent
    article_metadata: ArticleMetadata

    def to_dict(self) -> Dict[str, Any]:
        return {
            "article_data": self.article_data.to_dict(),
            "article_metadata": self.article_metadata.to_dict(),
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> "ArticleExtractionBundle":
        data_payload = payload.get("article_data") or {}
        metadata_payload = payload.get("article_metadata") or {}
        return cls(
            article_data=ExtractedContent.from_dict(data_payload),
            article_metadata=ArticleMetadata.from_dict(metadata_payload),
        )


__all__ = [
    "ExtractedContent",
    "ExtractedTable",
    "ArticleExtractionBundle",
]
