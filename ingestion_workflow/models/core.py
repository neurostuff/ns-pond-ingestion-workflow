"""Core data structures shared across the ingestion workflow."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional


def _normalize_identifier(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    value = value.strip()
    return value or None


@dataclass(frozen=True)
class Identifier:
    """Canonical identifier representation for a paper."""

    pmid: Optional[str] = None
    doi: Optional[str] = None
    pmcid: Optional[str] = None
    other_ids: tuple[str, ...] = field(default_factory=tuple)

    def normalized(self) -> "Identifier":
        """Return a copy with whitespace stripped from identifiers."""
        return Identifier(
            pmid=_normalize_identifier(self.pmid),
            doi=_normalize_identifier(self.doi),
            pmcid=_normalize_identifier(self.pmcid),
            other_ids=tuple(
                filter(None, (_normalize_identifier(value) for value in self.other_ids))
            ),
        )

    @property
    def hash_id(self) -> str:
        """Stable hash computed from available identifiers."""
        values: List[str] = []
        for item in (self.pmid, self.doi, self.pmcid, *self.other_ids):
            if item:
                values.append(item.lower())
        digest_input = json.dumps(sorted(values)).encode("utf-8")
        return hashlib.sha256(digest_input).hexdigest()[:16]

    @property
    def available_ids(self) -> List[str]:
        """List of identifiers in priority order."""
        return [value for value in (self.pmid, self.doi, self.pmcid, *self.other_ids) if value]


@dataclass
class IdentifierSet:
    """Collection of identifiers gathered from various sources."""

    identifiers: List[Identifier] = field(default_factory=list)

    @classmethod
    def from_iterable(cls, iterable: Iterable[Identifier]) -> "IdentifierSet":
        return cls(list(iterable))

    def add(self, identifier: Identifier) -> None:
        self.identifiers.append(identifier)

    def deduplicate(self) -> None:
        """Remove duplicate entries based on their hash IDs."""
        seen: set[str] = set()
        unique: List[Identifier] = []
        for item in self.identifiers:
            normalized = item.normalized()
            digest = normalized.hash_id
            if digest in seen:
                continue
            seen.add(digest)
            unique.append(normalized)
        self.identifiers = unique


@dataclass
class DownloadArtifact:
    """Represents a single downloaded asset (full text, supplementary file, etc.)."""

    path: Path
    kind: str = "full_text"  # e.g., full_text, supplement, tables_xml
    media_type: str = "application/octet-stream"


@dataclass
class DownloadResult:
    """Output from an extractor download attempt."""

    identifier: Identifier
    source: str
    artifacts: List[DownloadArtifact]
    open_access: bool = False
    extra_metadata: Dict[str, object] = field(default_factory=dict)

    def to_manifest_entry(self) -> Dict[str, object]:
        """Serialize to manifest-friendly dictionary."""
        return {
            "hash_id": self.identifier.hash_id,
            "source": self.source,
            "open_access": self.open_access,
            "artifacts": [
                {
                    "path": str(artifact.path),
                    "kind": artifact.kind,
                    "media_type": artifact.media_type,
                }
                for artifact in self.artifacts
            ],
            "extra_metadata": self.extra_metadata,
        }


@dataclass
class TableArtifact:
    """Description of a processed table extracted from a paper."""

    name: str
    raw_path: Path
    normalized_csv_path: Path
    metadata_path: Optional[Path] = None
    is_coordinate_table: bool = False
    source_table_id: Optional[str] = None


@dataclass
class MetadataRecord:
    """Metadata associated with a paper."""

    title: str
    authors: List[str]
    journal: Optional[str] = None
    year: Optional[int] = None
    abstract: Optional[str] = None
    keywords: List[str] = field(default_factory=list)
    external_metadata: Dict[str, object] = field(default_factory=dict)


@dataclass
class CoordinateSet:
    """Coordinates derived from a table for insertion into Neurostore."""

    table_name: str
    coordinates_csv: Path
    extraction_metadata: Dict[str, object] = field(default_factory=dict)


@dataclass
class ProcessedPaper:
    """Aggregate of processed outputs for a single paper."""

    identifier: Identifier
    source: str
    full_text_path: Path
    metadata: MetadataRecord
    tables: List[TableArtifact] = field(default_factory=list)
    coordinates: List[CoordinateSet] = field(default_factory=list)
    analyses_path: Optional[Path] = None

    def coordinate_table_ids(self) -> List[str]:
        return [
            table.source_table_id
            for table in self.tables
            if table.is_coordinate_table and table.source_table_id
        ]


@dataclass
class NeurostorePayload:
    """Representation of data uploaded to Neurostore."""

    study_payload: Dict[str, object]
    analyses_payload: List[Dict[str, object]] = field(default_factory=list)


@dataclass
class RunManifest:
    """Tracks pipeline state for incremental execution."""

    created_at: datetime
    settings: Dict[str, object]
    items: Dict[str, Dict[str, object]] = field(default_factory=dict)

    def register_identifier(self, identifier: Identifier) -> None:
        """Store the identifier in the manifest for downstream stages."""
        payload = {
            "hash_id": identifier.hash_id,
            "pmid": identifier.pmid,
            "doi": identifier.doi,
            "pmcid": identifier.pmcid,
            "other_ids": list(identifier.other_ids),
        }
        self.items.setdefault(identifier.hash_id, {})
        self.items[identifier.hash_id]["identifier"] = payload

    def register_processing(self, identifier: Identifier, payload: Dict[str, object]) -> None:
        """Record processing summary for the identifier."""
        self.items.setdefault(identifier.hash_id, {})
        self.items[identifier.hash_id]["process"] = payload

    def register_download(self, result: DownloadResult) -> None:
        self.items.setdefault(result.identifier.hash_id, {})
        self.items[result.identifier.hash_id]["download"] = result.to_manifest_entry()

    def update_stage(self, hash_id: str, stage: str, payload: Dict[str, object]) -> None:
        self.items.setdefault(hash_id, {})
        self.items[hash_id][stage] = payload

    def to_dict(self) -> Dict[str, object]:
        return {
            "created_at": self.created_at.isoformat(),
            "settings": self.settings,
            "items": self.items,
        }

    def dumps(self) -> str:
        return json.dumps(self.to_dict(), indent=2, default=str)
