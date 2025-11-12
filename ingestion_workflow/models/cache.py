"""Cache envelope models that wrap workflow pipeline dataclasses."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import (
    Any,
    ClassVar,
    Dict,
    Generic,
    Mapping,
    Optional,
    Type,
    TypeVar,
)

from .analysis import CreateAnalysesResult
from .download import DownloadResult
from .extract import ExtractedContent, ExtractedTable
from .ids import Identifier, IdentifierExpansion, Identifiers
from .metadata import ArticleMetadata, Author


CACHE_SCHEMA_VERSION = 1


PayloadT = TypeVar("PayloadT")
EnvelopeT = TypeVar("EnvelopeT", bound="CacheEnvelope[Any]")


def _encode_datetime(value: datetime) -> str:
    return value.isoformat()


def _decode_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value)


@dataclass
class CacheEnvelope(Generic[PayloadT]):
    """Generic wrapper that pairs a payload with cache metadata."""

    hash_id: str
    payload: PayloadT
    cached_at: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = field(default_factory=dict)

    payload_cls: ClassVar[Type[PayloadT]]

    def cache_key(self) -> str:
        return self.hash_id

    def to_dict(self) -> Dict[str, Any]:
        return {
            "hash_id": self.hash_id,
            "cached_at": _encode_datetime(self.cached_at),
            "metadata": dict(self.metadata),
            "payload": self._encode_payload(),
        }

    def _encode_payload(self) -> Dict[str, Any]:
        encoder = getattr(self.payload, "to_dict", None)
        if callable(encoder):
            return encoder()
        if hasattr(self.payload, "__dict__"):
            return dict(self.payload.__dict__)
        raise TypeError(
            "Cache payload"
            f" {type(self.payload)!r}"
            " does not support serialization"
        )

    @classmethod
    def from_dict(
        cls,
        payload: Mapping[str, Any],
    ) -> "CacheEnvelope[PayloadT]":
        payload_blob = payload.get("payload", {})
        decoded_payload = cls._decode_payload(payload_blob)
        cached_raw = payload.get("cached_at")
        cached_at = (
            _decode_datetime(str(cached_raw))
            if cached_raw
            else datetime.utcnow()
        )
        metadata = dict(payload.get("metadata", {}))
        raw_hash_id = payload.get("hash_id")
        hash_id = (
            str(raw_hash_id)
            if raw_hash_id is not None
            else cls._derive_hash_id(decoded_payload)
        )
        return cls(
            hash_id=hash_id,
            payload=decoded_payload,
            cached_at=cached_at,
            metadata=metadata,
        )

    @classmethod
    def _decode_payload(cls, payload_blob: Mapping[str, Any]) -> PayloadT:
        decoder = getattr(cls.payload_cls, "from_dict", None)
        if callable(decoder):
            return decoder(payload_blob)
        return cls.payload_cls(**payload_blob)  # type: ignore[arg-type]

    def clone_payload(self) -> PayloadT:
        """Return a deep-cloned payload using its serializer."""

        return type(self)._decode_payload(self._encode_payload())

    @classmethod
    def _derive_hash_id(cls, payload: PayloadT) -> str:
        candidate = getattr(payload, "hash_id", None)
        if candidate is None:
            return ""
        return str(candidate)


@dataclass
class CacheIndex(Generic[EnvelopeT]):
    """Generic index for cache envelopes."""

    entries: Dict[str, EnvelopeT] = field(default_factory=dict)
    version: int = CACHE_SCHEMA_VERSION
    index_path: Optional[Path] = None

    entries_key: ClassVar[str] = "entries"
    envelope_type: ClassVar[Type[EnvelopeT]]
    schema_version: ClassVar[int] = CACHE_SCHEMA_VERSION

    def add(self, entry: EnvelopeT) -> None:
        self.entries[entry.cache_key()] = entry

    def get(self, hash_id: str) -> Optional[EnvelopeT]:
        return self.entries.get(hash_id)

    def has(self, hash_id: str) -> bool:
        return hash_id in self.entries

    def remove(self, hash_id: str) -> bool:
        return self.entries.pop(hash_id, None) is not None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            self.entries_key: {
                key: entry.to_dict() for key, entry in self.entries.items()
            },
        }

    def save(self) -> None:
        if self.index_path is None:
            raise ValueError("index_path must be set before saving")
        self.index_path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(
            self.to_dict(),
            indent=2,
            sort_keys=True,
        )
        tmp_path = self.index_path.with_suffix(".tmp")
        tmp_path.write_text(payload, encoding="utf-8")
        tmp_path.replace(self.index_path)

    @classmethod
    def load(cls, index_path: Path) -> "CacheIndex[EnvelopeT]":
        if not index_path.exists():
            index = cls()
            index.index_path = index_path
            return index

        raw = index_path.read_text(encoding="utf-8")
        data = json.loads(raw)
        entries_payload = data.get(cls.entries_key, {})
        entries: Dict[str, EnvelopeT] = {}

        if isinstance(entries_payload, dict):
            source_items = entries_payload.items()
        else:
            source_items = [(None, item) for item in entries_payload]

        for key, value in source_items:
            entry = cls.envelope_type.from_dict(value)
            entry_key = key or entry.cache_key()
            entries[entry_key] = entry

        index = cls(
            entries=entries,
            version=int(data.get("version", cls.schema_version)),
        )
        index.index_path = index_path
        return index


@dataclass
class DownloadCacheEntry(CacheEnvelope[DownloadResult]):
    """Envelope for cached download results."""

    payload_cls: ClassVar[Type[DownloadResult]] = DownloadResult

    @classmethod
    def from_result(cls, result: DownloadResult) -> "DownloadCacheEntry":
        clone = DownloadResult.from_dict(result.to_dict())
        hash_id = clone.identifier.hash_id
        return cls(hash_id=hash_id, payload=clone)

    @property
    def result(self) -> DownloadResult:
        return self.payload


@dataclass
class DownloadIndex(CacheIndex[DownloadCacheEntry]):
    """Index for cached download envelopes."""

    entries_key: ClassVar[str] = "downloads"
    envelope_type: ClassVar[Type[DownloadCacheEntry]] = DownloadCacheEntry

    def add_download(self, result: DownloadResult) -> None:
        self.add(DownloadCacheEntry.from_result(result))

    def get_download(self, hash_id: str) -> Optional[DownloadCacheEntry]:
        return self.get(hash_id)

    def remove_download(self, hash_id: str) -> bool:
        return self.remove(hash_id)


@dataclass
class IdentifierCacheEntry(CacheEnvelope[IdentifierExpansion]):
    """Envelope for cached identifier expansion results."""

    payload_cls: ClassVar[Type[IdentifierExpansion]] = IdentifierExpansion

    @classmethod
    def from_expansion(
        cls, expansion: IdentifierExpansion
    ) -> "IdentifierCacheEntry":
        clone = IdentifierExpansion.from_dict(expansion.to_dict())
        hash_id = clone.seed_identifier.hash_id
        return cls(hash_id=hash_id, payload=clone)

    @property
    def seed_identifier(self) -> Identifier:
        return self.payload.seed_identifier

    @property
    def identifiers(self) -> Identifiers:
        return self.payload.identifiers

    @property
    def sources(self) -> list[str]:
        return self.payload.sources


@dataclass
class IdentifierCacheIndex(CacheIndex[IdentifierCacheEntry]):
    """Index for identifier expansion envelopes."""

    entries_key: ClassVar[str] = "identifier_entries"
    envelope_type: ClassVar[Type[IdentifierCacheEntry]] = IdentifierCacheEntry

    def add_entry(self, entry: IdentifierCacheEntry) -> None:
        self.add(entry)


@dataclass
class ExtractionResultEntry(CacheEnvelope[ExtractedContent]):
    """Envelope for cached extraction results."""

    payload_cls: ClassVar[Type[ExtractedContent]] = ExtractedContent

    @classmethod
    def from_content(
        cls, content: ExtractedContent
    ) -> "ExtractionResultEntry":
        clone = ExtractedContent.from_dict(content.to_dict())
        return cls(hash_id=clone.hash_id, payload=clone)

    @property
    def content(self) -> ExtractedContent:
        return self.payload

    @property
    def tables(self) -> list[ExtractedTable]:
        return self.payload.tables


@dataclass
class ExtractionResultIndex(CacheIndex[ExtractionResultEntry]):
    """Index for extraction result envelopes."""

    entries_key: ClassVar[str] = "extractions"
    envelope_type: ClassVar[Type[ExtractionResultEntry]] = (
        ExtractionResultEntry
    )

    def add_extraction(self, entry: ExtractionResultEntry) -> None:
        self.add(entry)

    def get_extraction(self, hash_id: str) -> Optional[ExtractionResultEntry]:
        return self.get(hash_id)

    def has_extraction(self, hash_id: str) -> bool:
        return self.has(hash_id)


@dataclass
class CreateAnalysesResultEntry(CacheEnvelope[CreateAnalysesResult]):
    """Envelope for cached create-analyses results."""

    payload_cls: ClassVar[Type[CreateAnalysesResult]] = CreateAnalysesResult

    @classmethod
    def from_result(
        cls, result: CreateAnalysesResult
    ) -> "CreateAnalysesResultEntry":
        clone = CreateAnalysesResult.from_dict(result.to_dict())
        return cls(hash_id=clone.hash_id, payload=clone)

    @property
    def analysis_paths(self) -> list[Path]:
        return self.payload.analysis_paths

    @property
    def extra_metadata(self) -> Dict[str, Any]:
        return self.payload.metadata

    @property
    def error_message(self) -> Optional[str]:
        return self.payload.error_message


@dataclass
class CreateAnalysesResultIndex(CacheIndex[CreateAnalysesResultEntry]):
    """Index for cached create-analyses results."""

    entries_key: ClassVar[str] = "create_analyses"
    envelope_type: ClassVar[Type[CreateAnalysesResultEntry]] = (
        CreateAnalysesResultEntry
    )

    def add_result(self, entry: CreateAnalysesResultEntry) -> None:
        self.add(entry)


@dataclass
class MetadataCache(CacheEnvelope[ArticleMetadata]):
    """Envelope for cached article metadata."""

    payload_cls: ClassVar[Type[ArticleMetadata]] = ArticleMetadata

    @classmethod
    def from_metadata(
        cls,
        hash_id: str,
        metadata: ArticleMetadata,
        sources_queried: Optional[list[str]] = None,
    ) -> "MetadataCache":
        payload_dict = cls._article_to_dict(metadata)
        clone = cls._decode_payload(payload_dict)
        envelope = cls(
            hash_id=hash_id,
            payload=clone,
            metadata={
                "sources_queried": list(sources_queried or []),
            },
        )
        return envelope

    @property
    def article_metadata(self) -> ArticleMetadata:
        return self.payload

    @property
    def sources_queried(self) -> list[str]:
        stored = self.metadata.get("sources_queried", [])
        return list(stored)

    def _encode_payload(self) -> Dict[str, Any]:
        return self._article_to_dict(self.payload)

    @classmethod
    def _decode_payload(
        cls,
        payload_blob: Mapping[str, Any],
    ) -> ArticleMetadata:
        authors_payload = payload_blob.get("authors", [])
        authors = [Author(**author) for author in authors_payload]
        return ArticleMetadata(
            title=str(payload_blob.get("title", "")),
            authors=authors,
            abstract=payload_blob.get("abstract"),
            journal=payload_blob.get("journal"),
            publication_year=payload_blob.get("publication_year"),
            keywords=list(payload_blob.get("keywords", [])),
            license=payload_blob.get("license"),
            source=payload_blob.get("source"),
            raw_metadata=dict(payload_blob.get("raw_metadata", {})),
        )

    @staticmethod
    def _article_to_dict(metadata: ArticleMetadata) -> Dict[str, Any]:
        return {
            "title": metadata.title,
            "authors": [author.__dict__.copy() for author in metadata.authors],
            "abstract": metadata.abstract,
            "journal": metadata.journal,
            "publication_year": metadata.publication_year,
            "keywords": list(metadata.keywords),
            "license": metadata.license,
            "source": metadata.source,
            "raw_metadata": dict(metadata.raw_metadata),
        }


@dataclass
class MetadataCacheIndex(CacheIndex[MetadataCache]):
    """Index for cached metadata records."""

    entries_key: ClassVar[str] = "metadata_entries"
    envelope_type: ClassVar[Type[MetadataCache]] = MetadataCache

    def add_metadata(self, entry: MetadataCache) -> None:
        self.add(entry)


__all__ = [
    "CACHE_SCHEMA_VERSION",
    "CacheEnvelope",
    "CacheIndex",
    "CreateAnalysesResultEntry",
    "CreateAnalysesResultIndex",
    "DownloadCacheEntry",
    "DownloadIndex",
    "ExtractionResultEntry",
    "ExtractionResultIndex",
    "IdentifierCacheEntry",
    "IdentifierCacheIndex",
    "MetadataCache",
    "MetadataCacheIndex",
]

