"""Cache envelope models that wrap workflow pipeline dataclasses."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
import sqlite3
from typing import (
    Any,
    ClassVar,
    Dict,
    Generic,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
)

from .analysis import CreateAnalysesResult
from .download import DownloadResult
from .extract import ExtractedContent, ExtractedTable
from .ids import Identifier, IdentifierExpansion, Identifiers
from .metadata import ArticleMetadata, Author
from .upload import UploadOutcome


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

    slug: str
    payload: PayloadT
    cached_at: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = field(default_factory=dict)

    payload_cls: ClassVar[Type[PayloadT]]

    def cache_key(self) -> str:
        return self.slug

    def to_dict(self) -> Dict[str, Any]:
        return {
            "slug": self.slug,
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
        raise TypeError(f"Cache payload {type(self.payload)!r} does not support serialization")

    @classmethod
    def from_dict(
        cls,
        payload: Mapping[str, Any],
    ) -> "CacheEnvelope[PayloadT]":
        payload_blob = payload.get("payload", {})
        decoded_payload = cls._decode_payload(payload_blob)
        cached_raw = payload.get("cached_at")
        cached_at = _decode_datetime(str(cached_raw)) if cached_raw else datetime.utcnow()
        metadata = dict(payload.get("metadata", {}))
        raw_slug = payload.get("slug")
        slug = str(raw_slug) if raw_slug is not None else cls._derive_slug(decoded_payload)
        return cls(
            slug=slug,
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
    def _derive_slug(cls, payload: PayloadT) -> str:
        candidate = getattr(payload, "slug", None)
        if candidate is None:
            return ""
        return str(candidate)


class CacheIndex(Generic[EnvelopeT]):
    """SQLite-backed index for cache envelopes."""

    table_name: ClassVar[str] = "cache_entries"
    envelope_type: ClassVar[Type[EnvelopeT]]
    schema_version: ClassVar[int] = CACHE_SCHEMA_VERSION
    identifier_columns: ClassVar[Mapping[str, str]] = {
        "pmid": "TEXT",
        "pmcid": "TEXT",
        "doi": "TEXT",
    }
    extra_columns: ClassVar[Mapping[str, str]] = {}

    def __init__(self, connection: sqlite3.Connection, index_path: Path):
        self._conn = connection
        self.index_path = index_path
        self.version = CACHE_SCHEMA_VERSION
        self._initialize()

    def close(self) -> None:
        self._conn.close()

    @classmethod
    def load(cls, index_path: Path) -> "CacheIndex[EnvelopeT]":
        index_path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(index_path)
        cls._prepare_connection(connection)
        return cls(connection, index_path)

    @staticmethod
    def _prepare_connection(connection: sqlite3.Connection) -> None:
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON;")
        connection.execute("PRAGMA journal_mode=WAL;")
        connection.execute("PRAGMA synchronous=NORMAL;")

    def _initialize(self) -> None:
        column_schema = self._column_schema()
        columns_sql = "".join(f", {name} {definition}" for name, definition in column_schema.items())
        self._conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                slug TEXT PRIMARY KEY,
                payload_json BLOB NOT NULL,
                cached_at TEXT NOT NULL,
                metadata_json BLOB
                {columns_sql}
            )
            """
        )
        self._ensure_columns(column_schema)
        for statement in self._identifier_index_statements():
            self._conn.execute(statement)
        for statement in self._index_statements():
            self._conn.execute(statement)
        self._conn.commit()

    def _column_schema(self) -> Dict[str, str]:
        schema = dict(self.identifier_columns)
        schema.update(self.extra_columns)
        return schema

    def _ensure_columns(self, column_schema: Mapping[str, str]) -> None:
        cursor = self._conn.execute(f"PRAGMA table_info({self.table_name})")
        existing = {row["name"] for row in cursor}
        for name, definition in column_schema.items():
            if name in existing:
                continue
            self._conn.execute(
                f"ALTER TABLE {self.table_name} ADD COLUMN {name} {definition}"
            )

    def _identifier_index_statements(self) -> Sequence[str]:
        return tuple(
            f"CREATE INDEX IF NOT EXISTS {self.table_name}_{column}_idx ON {self.table_name}({column})"
            for column in self.identifier_columns
        )

    def _index_statements(self) -> Sequence[str]:  # pragma: no cover - overridden when needed
        return ()

    def _column_names(self) -> List[str]:
        columns = ["slug", "payload_json", "cached_at", "metadata_json"]
        columns.extend(self.identifier_columns.keys())
        columns.extend(self.extra_columns.keys())
        return columns

    def add_entries(self, entries: Sequence[EnvelopeT]) -> None:
        if not entries:
            return
        column_names = self._column_names()
        placeholders = ", ".join("?" for _ in column_names)
        assignments = ", ".join(
            f"{column}=excluded.{column}" for column in column_names if column != "slug"
        )
        sql = (
            f"INSERT INTO {self.table_name} ({', '.join(column_names)}) "
            f"VALUES ({placeholders}) "
            f"ON CONFLICT(slug) DO UPDATE SET {assignments}"
        )
        rows = [self._row_from_entry(entry) for entry in entries]
        with self._conn:
            self._conn.executemany(sql, rows)

    def add(self, entry: EnvelopeT) -> None:
        self.add_entries([entry])

    def get(self, slug: str) -> Optional[EnvelopeT]:
        cursor = self._conn.execute(
            f"SELECT * FROM {self.table_name} WHERE slug = ?",
            (slug,),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return self._entry_from_row(row)

    def get_by_identifier(self, identifier: Identifier) -> Optional[EnvelopeT]:
        """
        Attempt to resolve an entry by slug first, then by individual identifiers.

        This allows callers to hydrate cached entries even when the composed slug
        differs (e.g., legacy cache keyed by PMID only).
        """
        if identifier is None:
            return None

        slug = getattr(identifier, "slug", None)
        if slug:
            entry = self.get(slug)
            if entry is not None:
                return entry

        for column in ("pmid", "doi", "pmcid"):
            value = getattr(identifier, column, None)
            if not value:
                continue
            cursor = self._conn.execute(
                f"SELECT * FROM {self.table_name} WHERE {column} = ? LIMIT 1",
                (value,),
            )
            row = cursor.fetchone()
            if row is not None:
                return self._entry_from_row(row)

        return None

    def has(self, slug: str) -> bool:
        cursor = self._conn.execute(
            f"SELECT 1 FROM {self.table_name} WHERE slug = ? LIMIT 1",
            (slug,),
        )
        return cursor.fetchone() is not None

    def remove(self, slug: str) -> bool:
        with self._conn:
            cursor = self._conn.execute(
                f"DELETE FROM {self.table_name} WHERE slug = ?",
                (slug,),
            )
        return cursor.rowcount > 0

    def count(self) -> int:
        cursor = self._conn.execute(f"SELECT COUNT(*) FROM {self.table_name}")
        result = cursor.fetchone()
        return 0 if result is None else int(result[0])

    def iter_entries(self) -> Iterator[EnvelopeT]:
        cursor = self._conn.execute(f"SELECT * FROM {self.table_name}")
        for row in cursor:
            yield self._entry_from_row(row)

    @property
    def entries(self) -> Dict[str, EnvelopeT]:
        return {entry.cache_key(): entry for entry in self.iter_entries()}

    def _entry_from_row(self, row: sqlite3.Row) -> EnvelopeT:
        payload_blob = row["payload_json"]
        metadata_blob = row["metadata_json"]
        payload_data = json.loads(payload_blob)
        metadata = json.loads(metadata_blob) if metadata_blob else {}
        entry_payload = {
            "slug": row["slug"],
            "cached_at": row["cached_at"],
            "metadata": metadata,
            "payload": payload_data,
        }
        return self.envelope_type.from_dict(entry_payload)

    def _identifier_from_entry(self, entry: EnvelopeT) -> Optional[Identifier]:  # pragma: no cover - override hook
        return None

    def _extra_values(self, entry: EnvelopeT) -> Dict[str, Any]:  # pragma: no cover - override hook
        return {}

    def _row_from_entry(self, entry: EnvelopeT) -> tuple[Any, ...]:
        base_values: List[Any] = [
            entry.cache_key(),
            self._serialize_payload(entry),
            entry.cached_at.isoformat(),
            self._serialize_metadata(entry),
        ]
        identifier = self._identifier_from_entry(entry)
        for column in self.identifier_columns.keys():
            value = getattr(identifier, column, None) if identifier else None
            base_values.append(value)
        extras = self._extra_values(entry)
        for column in self.extra_columns.keys():
            base_values.append(extras.get(column))
        return tuple(base_values)

    @staticmethod
    def _serialize_payload(entry: CacheEnvelope[Any]) -> str:
        payload_dict = entry._encode_payload()
        return json.dumps(payload_dict)

    @staticmethod
    def _serialize_metadata(entry: CacheEnvelope[Any]) -> str:
        metadata = dict(entry.metadata)
        return json.dumps(metadata)


@dataclass
class DownloadCacheEntry(CacheEnvelope[DownloadResult]):
    """Envelope for cached download results."""

    payload_cls: ClassVar[Type[DownloadResult]] = DownloadResult

    @classmethod
    def from_result(cls, result: DownloadResult) -> "DownloadCacheEntry":
        clone = DownloadResult.from_dict(result.to_dict())
        slug = clone.identifier.slug
        return cls(slug=slug, payload=clone)

    @property
    def result(self) -> DownloadResult:
        return self.payload


class DownloadIndex(CacheIndex[DownloadCacheEntry]):
    """Index for cached download envelopes."""

    table_name: ClassVar[str] = "downloads"
    envelope_type: ClassVar[Type[DownloadCacheEntry]] = DownloadCacheEntry
    extra_columns: ClassVar[Mapping[str, str]] = {
        "source": "TEXT",
    }

    def _extra_values(self, entry: DownloadCacheEntry) -> Dict[str, Any]:
        source = entry.result.source.value if entry.result.source else None
        return {
            "source": source,
        }

    def _identifier_from_entry(self, entry: DownloadCacheEntry) -> Optional[Identifier]:
        return entry.result.identifier

    def add_download(self, result: DownloadResult) -> None:
        self.add_entries([DownloadCacheEntry.from_result(result)])

    def add_downloads(self, results: Sequence[DownloadResult]) -> None:
        entries = [DownloadCacheEntry.from_result(result) for result in results]
        self.add_entries(entries)

    def get_download(self, slug: str) -> Optional[DownloadCacheEntry]:
        return self.get(slug)

    def get_download_by_identifier(self, identifier: Identifier) -> Optional[DownloadCacheEntry]:
        return self.get_by_identifier(identifier)

    def remove_download(self, slug: str) -> bool:
        return self.remove(slug)

    def identifier_sets(self) -> Tuple[set[str], set[str], set[str], set[str]]:
        slug_set: set[str] = set()
        pmid_set: set[str] = set()
        pmcid_set: set[str] = set()
        doi_set: set[str] = set()
        cursor = self._conn.execute(
            f"SELECT slug, pmid, pmcid, doi FROM {self.table_name}"
        )
        for row in cursor:
            slug = row["slug"]
            if slug:
                slug_set.add(slug)
            pmid = row["pmid"]
            if pmid:
                pmid_set.add(pmid)
            pmcid = row["pmcid"]
            if pmcid:
                pmcid_set.add(pmcid)
            doi = row["doi"]
            if doi:
                doi_set.add(doi)
        return slug_set, pmid_set, pmcid_set, doi_set


@dataclass
class IdentifierCacheEntry(CacheEnvelope[IdentifierExpansion]):
    """Envelope for cached identifier expansion results."""

    payload_cls: ClassVar[Type[IdentifierExpansion]] = IdentifierExpansion

    @classmethod
    def from_expansion(cls, expansion: IdentifierExpansion) -> "IdentifierCacheEntry":
        clone = IdentifierExpansion.from_dict(expansion.to_dict())
        slug = clone.seed_identifier.slug
        return cls(slug=slug, payload=clone)

    @property
    def seed_identifier(self) -> Identifier:
        return self.payload.seed_identifier

    @property
    def identifiers(self) -> Identifiers:
        return self.payload.identifiers

    @property
    def sources(self) -> list[str]:
        return self.payload.sources


class IdentifierCacheIndex(CacheIndex[IdentifierCacheEntry]):
    """Index for identifier expansion envelopes."""

    table_name: ClassVar[str] = "identifier_entries"
    envelope_type: ClassVar[Type[IdentifierCacheEntry]] = IdentifierCacheEntry

    def add_entry(self, entry: IdentifierCacheEntry) -> None:
        self.add(entry)

    def _identifier_from_entry(self, entry: IdentifierCacheEntry) -> Optional[Identifier]:
        return entry.seed_identifier


@dataclass
class ExtractionResultEntry(CacheEnvelope[ExtractedContent]):
    """Envelope for cached extraction results."""

    payload_cls: ClassVar[Type[ExtractedContent]] = ExtractedContent

    @classmethod
    def from_content(cls, content: ExtractedContent) -> "ExtractionResultEntry":
        clone = ExtractedContent.from_dict(content.to_dict())
        return cls(slug=clone.slug, payload=clone)

    @property
    def content(self) -> ExtractedContent:
        return self.payload

    @property
    def tables(self) -> list[ExtractedTable]:
        return self.payload.tables


class ExtractionResultIndex(CacheIndex[ExtractionResultEntry]):
    """Index for extraction result envelopes."""

    table_name: ClassVar[str] = "extractions"
    envelope_type: ClassVar[Type[ExtractionResultEntry]] = ExtractionResultEntry

    def add_extraction(self, entry: ExtractionResultEntry) -> None:
        self.add(entry)

    def get_extraction(self, slug: str) -> Optional[ExtractionResultEntry]:
        return self.get(slug)

    def get_extraction_by_identifier(
        self, identifier: Identifier
    ) -> Optional[ExtractionResultEntry]:
        return self.get_by_identifier(identifier)

    def has_extraction(self, slug: str) -> bool:
        return self.has(slug)

    def _identifier_from_entry(self, entry: ExtractionResultEntry) -> Optional[Identifier]:
        return entry.content.identifier


@dataclass
class CreateAnalysesResultEntry(CacheEnvelope[CreateAnalysesResult]):
    """Envelope for cached create-analyses results."""

    payload_cls: ClassVar[Type[CreateAnalysesResult]] = CreateAnalysesResult

    @classmethod
    def from_result(cls, result: CreateAnalysesResult) -> "CreateAnalysesResultEntry":
        clone = CreateAnalysesResult.from_dict(result.to_dict())
        return cls(slug=clone.slug, payload=clone)

    @property
    def analysis_paths(self) -> list[Path]:
        return self.payload.analysis_paths

    @property
    def extra_metadata(self) -> Dict[str, Any]:
        return self.payload.metadata

    @property
    def error_message(self) -> Optional[str]:
        return self.payload.error_message


class CreateAnalysesResultIndex(CacheIndex[CreateAnalysesResultEntry]):
    """Index for cached create-analyses results."""

    table_name: ClassVar[str] = "create_analyses"
    envelope_type: ClassVar[Type[CreateAnalysesResultEntry]] = CreateAnalysesResultEntry

    def add_result(self, entry: CreateAnalysesResultEntry) -> None:
        self.add(entry)

    def find_by_identifier(
        self, identifier: Identifier, *, sanitized_table_id: str | None = None
    ) -> Optional[CreateAnalysesResultEntry]:
        """
        Resolve an entry by identifier columns with optional table filter.

        Slug match is attempted first; if it fails, the lookup falls back to
        pmid/doi/pmcid columns and filters by sanitized_table_id when provided.
        """
        if identifier is None:
            return None

        slug = getattr(identifier, "slug", None)
        if slug:
            entry = self.get(slug)
            if entry and (
                sanitized_table_id is None
                or entry.payload.sanitized_table_id == sanitized_table_id
            ):
                return entry

        seen: set[str] = set()
        if slug:
            seen.add(slug)

        for column in ("pmid", "doi", "pmcid"):
            value = getattr(identifier, column, None)
            if not value:
                continue
            cursor = self._conn.execute(
                f"SELECT * FROM {self.table_name} WHERE {column} = ?",
                (value,),
            )
            for row in cursor:
                row_slug = row["slug"]
                if row_slug in seen:
                    continue
                seen.add(row_slug)
                entry = self._entry_from_row(row)
                if sanitized_table_id is None:
                    return entry
                if entry.payload.sanitized_table_id == sanitized_table_id:
                    return entry

        return None

    def _identifier_from_entry(self, entry: CreateAnalysesResultEntry) -> Optional[Identifier]:
        return entry.payload.analysis_collection.identifier


@dataclass
class MetadataCache(CacheEnvelope[ArticleMetadata]):
    """Envelope for cached article metadata."""

    payload_cls: ClassVar[Type[ArticleMetadata]] = ArticleMetadata

    @classmethod
    def from_metadata(
        cls,
        slug: str,
        metadata: ArticleMetadata,
        sources_queried: Optional[list[str]] = None,
        *,
        identifier: Optional[Identifier] = None,
        metadata_path: Optional[Path] = None,
    ) -> "MetadataCache":
        payload_dict = cls._article_to_dict(metadata)
        clone = cls._decode_payload(payload_dict)
        ident_blob = identifier.to_dict() if identifier is not None else None
        envelope = cls(
            slug=slug,
            payload=clone,
            metadata={
                "sources_queried": list(sources_queried or []),
                "identifier": ident_blob,
                "metadata_path": str(metadata_path) if metadata_path else None,
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


class MetadataCacheIndex(CacheIndex[MetadataCache]):
    """Index for cached metadata records."""

    table_name: ClassVar[str] = "metadata_entries"
    envelope_type: ClassVar[Type[MetadataCache]] = MetadataCache

    def add_metadata(self, entry: MetadataCache) -> None:
        self.add(entry)

    def _identifier_from_entry(self, entry: MetadataCache) -> Optional[Identifier]:  # pragma: no cover - metadata may not include identifiers
        ident_dict = entry.metadata.get("identifier")
        if isinstance(ident_dict, dict):
            try:
                return Identifier.from_dict(ident_dict)
            except Exception:
                return None
        return None


@dataclass
class UploadCacheEntry(CacheEnvelope[UploadOutcome]):
    """Envelope for cached upload outcomes."""

    payload_cls: ClassVar[Type[UploadOutcome]] = UploadOutcome

    @classmethod
    def from_outcome(cls, outcome: UploadOutcome) -> "UploadCacheEntry":
        clone = UploadOutcome(
            slug=outcome.slug,
            base_study_id=outcome.base_study_id,
            study_id=outcome.study_id,
            analysis_ids=list(outcome.analysis_ids),
            success=outcome.success,
            error=outcome.error,
        )
        return cls(slug=clone.slug, payload=clone)

    @property
    def base_study_id(self) -> Optional[str]:
        return self.payload.base_study_id

    @property
    def study_id(self) -> Optional[str]:
        return self.payload.study_id


class UploadCacheIndex(CacheIndex[UploadCacheEntry]):
    """Index for upload outcomes."""

    table_name: ClassVar[str] = "upload_entries"
    envelope_type: ClassVar[Type[UploadCacheEntry]] = UploadCacheEntry
    extra_columns: ClassVar[Mapping[str, str]] = {
        "base_study_id": "TEXT",
        "study_id": "TEXT",
    }

    def _extra_values(self, entry: UploadCacheEntry) -> Dict[str, Any]:
        return {
            "base_study_id": entry.base_study_id,
            "study_id": entry.study_id,
        }

    def _identifier_from_entry(self, entry: UploadCacheEntry) -> Optional[Identifier]:
        return None


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
    "UploadCacheEntry",
    "UploadCacheIndex",
]
