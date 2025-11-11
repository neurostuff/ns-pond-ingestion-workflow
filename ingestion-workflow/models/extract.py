"""Data models for extracted content."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import json

from .download import DownloadSource
from .metadata import ArticleMetadata


@dataclass
class ExtractedTable:
    """
    Metadata about an extracted table.

    Tables may or may not contain coordinates.
    """

    table_id: str
    raw_content_path: Path
    table_number: Optional[int] = None
    caption: str = ""
    footer: str = ""
    contains_coordinates: bool = False


@dataclass
class ExtractedContent:
    """
    All content extracted from a downloaded article.

    Includes full text, tables, and detection of coordinate-containing
    tables.
    """

    hash_id: str
    source: DownloadSource
    full_text_path: Optional[Path] = None
    metadata: Optional[ArticleMetadata] = None
    tables: List[ExtractedTable] = field(default_factory=list)
    has_coordinates: bool = False
    extracted_at: datetime = field(default_factory=datetime.now)
    error_message: Optional[str] = None


@dataclass
class ExtractionIndex:
    """
    Index of all extractions in the cache.

    Maintains a mapping of hash_id -> extraction results to avoid
    redundant extraction work.
    """

    extractions: Dict[str, ExtractedContent] = field(default_factory=dict)
    index_path: Optional[Path] = None

    def add_extraction(self, content: ExtractedContent) -> None:
        """
        Add an extraction result to the index.

        Parameters
        ----------
        content : ExtractedContent
            Extraction result to add
        """
        self.extractions[content.hash_id] = content

    def get_extraction(self, hash_id: str) -> Optional[ExtractedContent]:
        """
        Get extraction result for a article.

        Parameters
        ----------
        hash_id : str
            Hash ID of the article

        Returns
        -------
        ExtractedContent or None
            Extraction result if found, None otherwise
        """
        return self.extractions.get(hash_id)

    def has_extraction(self, hash_id: str) -> bool:
        """
        Check if a article has been extracted.

        Parameters
        ----------
        hash_id : str
            Hash ID of the article

        Returns
        -------
        bool
            True if article has been extracted
        """
        return hash_id in self.extractions

    def get_articles_with_coordinates(self) -> List[str]:
        """
        Get list of hash IDs for articles that have coordinates.

        Returns
        -------
        list of str
            Hash IDs of articles with coordinate tables
        """
        return [
            hash_id
            for hash_id, content in self.extractions.items()
            if content.has_coordinates
        ]

    def save(self) -> None:
        """Save the index to disk."""
        if self.index_path is None:
            raise ValueError("index_path must be set before saving")

        # Convert dataclass to dict, excluding index_path
        from dataclasses import asdict

        payload = asdict(self)
        del payload["index_path"]

        self.index_path.parent.mkdir(parents=True, exist_ok=True)
        self.index_path.write_text(
            json.dumps(payload, indent=2, default=str),
            encoding="utf-8",
        )

    @classmethod
    def load(cls, index_path: Path) -> ExtractionIndex:
        """
        Load extraction index from disk.

        Parameters
        ----------
        index_path : Path
            Path to index file

        Returns
        -------
        ExtractionIndex
            Loaded index
        """
        if not index_path.exists():
            return cls(extractions={}, index_path=index_path)

        raw = index_path.read_text(encoding="utf-8")
        data = json.loads(raw)

        # Convert nested dicts back to dataclasses
        extractions = {}
        for hash_id, content_data in data.get("extractions", {}).items():
            tables = [
                ExtractedTable(
                    table_id=t["table_id"],
                    raw_content_path=Path(t["raw_content_path"]),
                    table_number=t.get("table_number"),
                    caption=t.get("caption", ""),
                    footer=t.get("footer", ""),
                    contains_coordinates=t.get("contains_coordinates", False),
                )
                for t in content_data.get("tables", [])
            ]
            extractions[hash_id] = ExtractedContent(
                hash_id=content_data["hash_id"],
                source=DownloadSource(content_data["source"]),
                full_text_path=(
                    Path(content_data["full_text_path"])
                    if content_data.get("full_text_path")
                    else None
                ),
                tables=tables,
                has_coordinates=content_data.get("has_coordinates", False),
                extracted_at=datetime.fromisoformat(content_data["extracted_at"]),
                error_message=content_data.get("error_message"),
            )

        return cls(extractions=extractions, index_path=index_path)
