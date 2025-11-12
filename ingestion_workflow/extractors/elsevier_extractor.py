"""Download and extract tables from articles using Elsevier services."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import os
import re
from pathlib import Path
from collections import deque
from typing import Any, Deque, Dict, Iterable, List, Mapping, Tuple

from elsevier_coordinate_extraction.client import ScienceDirectClient
from elsevier_coordinate_extraction.download.api import download_articles
from elsevier_coordinate_extraction.settings import (
    Settings as ElsevierSettings,
    get_settings as get_elsevier_settings,
)

from ingestion_workflow.config import Settings, load_settings
from ingestion_workflow.extractors.base import BaseExtractor
from ingestion_workflow.models import (
    Identifier,
    Identifiers,
    DownloadResult,
    DownloadSource,
    DownloadedFile,
    ExtractionResult,
    FileType,
)


class ElsevierExtractor(BaseExtractor):
    """Extractor that uses Elsevier to download and extract article content."""

    _EXTENSION_TO_FILETYPE: Dict[str, FileType] = {
        "xml": FileType.XML,
        "pdf": FileType.PDF,
        "html": FileType.HTML,
        "htm": FileType.HTML,
        "txt": FileType.TEXT,
        "text": FileType.TEXT,
        "csv": FileType.CSV,
        "json": FileType.JSON,
    }

    _CONTENT_TYPE_TO_EXTENSION: Dict[str, str] = {
        "application/xml": "xml",
        "text/xml": "xml",
        "application/pdf": "pdf",
        "text/html": "html",
        "application/json": "json",
        "text/plain": "txt",
        "text/csv": "csv",
    }

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        client: ScienceDirectClient | None = None,
        cache: Any | None = None,
    ) -> None:
        self.settings = settings or load_settings()
        self._client = client
        self._cache = cache

    def download(self, identifiers: Identifiers) -> List[DownloadResult]:
        """Download articles for each identifier using Elsevier services."""

        if not identifiers:
            return []

        prepared: List[Tuple[int, Identifier, Dict[str, str]]] = []
        records: List[Dict[str, str]] = []
        results_by_index: Dict[int, DownloadResult] = {}

        for index, identifier in enumerate(identifiers):
            record = self._record_from_identifier(identifier)
            if record:
                prepared.append((index, identifier, record))
                records.append(record)
            else:
                results_by_index[index] = self._build_failure_result(
                    identifier=identifier,
                    error_message=(
                        "Identifier is missing a DOI or PMID required for "
                        "Elsevier download."
                    ),
                )

        if not prepared:
            return self._ordered_results(identifiers, results_by_index)

        try:
            articles = list(self._run_download(records))
        except Exception as exc:  # pragma: no cover - surfaced to caller
            failure_reason = str(exc) or "Unknown Elsevier download failure."
            for original_index, identifier, _ in prepared:
                results_by_index[original_index] = self._build_failure_result(
                    identifier=identifier,
                    error_message=failure_reason,
                )
            return self._ordered_results(identifiers, results_by_index)

        base_dir = Path(
            self.settings.elsevier_cache_root
            or self.settings.get_cache_dir("elsevier")
        )
        base_dir.mkdir(parents=True, exist_ok=True)

        articles_by_lookup: Dict[
            Tuple[str | None, str | None],
            Deque[Any],
        ] = {}
        for article in articles:
            metadata = self._extract_metadata(article)
            identifier_lookup = metadata.get("identifier_lookup") or {}
            lookup_key = self._lookup_key(identifier_lookup)
            if lookup_key == (None, None):
                identifier_type = metadata.get("identifier_type")
                identifier_value = metadata.get("identifier")
                if identifier_type and identifier_value:
                    lookup_key = self._lookup_key(
                        {str(identifier_type): str(identifier_value)}
                    )
            articles_by_lookup.setdefault(lookup_key, deque()).append(article)

        for prepared_index, (
            original_index,
            identifier,
            record,
        ) in enumerate(prepared):
            lookup_type, lookup_value = self._primary_identifier(record)
            cache_key = self._identifier_cache_key(identifier, original_index)
            lookup_key = self._lookup_key(record)
            queue = articles_by_lookup.get(lookup_key)
            article = None
            if queue:
                article = queue.popleft()
                if not queue:
                    articles_by_lookup.pop(lookup_key, None)

            actual_lookup_type = lookup_type
            actual_lookup_value = lookup_value
            if article is not None:
                article_metadata = self._extract_metadata(article)
                metadata_type = article_metadata.get("identifier_type")
                metadata_value = article_metadata.get("identifier")
                if metadata_type:
                    actual_lookup_type = str(metadata_type)
                if metadata_value:
                    actual_lookup_value = str(metadata_value)

            results_by_index[original_index] = self._build_download_result(
                base_dir=base_dir,
                cache_key=cache_key,
                identifier=identifier,
                record=record,
                article=article,
                index=prepared_index,
                lookup_type=actual_lookup_type,
                lookup_value=actual_lookup_value,
            )

        return self._ordered_results(identifiers, results_by_index)

    # TODO: Implement extraction once downstream pipeline expectations are
    # finalized.
    def extract(
        self, download_results: List[DownloadResult]
    ) -> List[ExtractionResult]:
        """Extract tables from downloaded articles using Elsevier."""
        raise NotImplementedError(
            "ElsevierExtractor extract method not implemented."
        )  # pragma: no cover

    def _run_download(self, records: List[Dict[str, str]]) -> List[Any]:
        elsevier_settings = self._build_elsevier_settings()

        async def _runner() -> List[Any]:
            if self._client is None:
                async with ScienceDirectClient(elsevier_settings) as client:
                    return await download_articles(
                        records,
                        client=client,
                        cache=self._cache,
                        settings=elsevier_settings,
                    )
            return await download_articles(
                records,
                client=self._client,
                cache=self._cache,
                settings=elsevier_settings,
            )

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(_runner())
        if loop.is_running():  # pragma: no cover - defensive
            raise RuntimeError(
                "ElsevierExtractor.download cannot run inside an active event "
                "loop."
            )
        return loop.run_until_complete(_runner())

    def _build_elsevier_settings(self) -> ElsevierSettings:
        overrides: Dict[str, str] = {}
        if self.settings.elsevier_api_key:
            overrides["ELSEVIER_API_KEY"] = self.settings.elsevier_api_key

        cache_root = (
            self.settings.elsevier_cache_root
            or self.settings.get_cache_dir("elsevier")
        )
        overrides["ELSEVIER_CACHE_DIR"] = str(cache_root)

        if self.settings.elsevier_http_proxy:
            overrides["ELSEVIER_HTTP_PROXY"] = (
                self.settings.elsevier_http_proxy
            )
        if self.settings.elsevier_https_proxy:
            overrides["ELSEVIER_HTTPS_PROXY"] = (
                self.settings.elsevier_https_proxy
            )
        overrides["ELSEVIER_USE_PROXY"] = (
            "true" if self.settings.elsevier_use_proxy else "false"
        )
        overrides["ELSEVIER_CONCURRENCY"] = str(
            max(1, self.settings.max_workers)
        )

        with _temporary_env(overrides):
            return get_elsevier_settings(force_reload=True)

    def _build_download_result(
        self,
        *,
        base_dir: Path,
        cache_key: str,
        identifier: Identifier,
        record: Mapping[str, str],
        article: Any | None,
        index: int,
        lookup_type: str,
        lookup_value: str,
    ) -> DownloadResult:
        slug = self._slug_from_record(record, index)

        if article is None:
            return DownloadResult(
                identifier=identifier,
                source=DownloadSource.ELSEVIER,
                success=False,
                error_message=(
                    "Elsevier did not return a payload for the requested "
                    f"article ({lookup_type}: {lookup_value})."
                ),
            )

        if getattr(article, "success", None) is False:
            failure_detail = (
                getattr(article, "message", None)
                or getattr(article, "error", None)
                or "Elsevier reported a download failure."
            )
            return DownloadResult(
                identifier=identifier,
                source=DownloadSource.ELSEVIER,
                success=False,
                error_message=(
                    "Elsevier failure for "
                    f"{lookup_type}: {lookup_value}. Details: {failure_detail}"
                ),
            )

        if getattr(article, "exception", None) is not None:
            return DownloadResult(
                identifier=identifier,
                source=DownloadSource.ELSEVIER,
                success=False,
                error_message=(
                    f"Exception raised for {lookup_type}: {lookup_value}. "
                    f"Details: {getattr(article, 'exception')}"
                ),
            )

        try:
            files, has_payload = self._persist_article(
                base_dir=base_dir,
                cache_key=cache_key,
                identifier=identifier,
                slug=slug,
                record=record,
                article=article,
                lookup_type=lookup_type,
                lookup_value=lookup_value,
            )
        except Exception as exc:  # pragma: no cover - persistence bug
            return DownloadResult(
                identifier=identifier,
                source=DownloadSource.ELSEVIER,
                success=False,
                error_message=(
                    "Failed to persist Elsevier article "
                    f"for {lookup_type}: {lookup_value}. Error: {exc}"
                ),
            )

        success = has_payload and any(
            file.file_type is not FileType.JSON for file in files
        )
        error_message = None
        if not success:
            error_message = (
                "Elsevier returned metadata without content payload."
            )

        return DownloadResult(
            identifier=identifier,
            source=DownloadSource.ELSEVIER,
            success=success,
            files=files,
            error_message=error_message,
        )

    def _persist_article(
        self,
        *,
        base_dir: Path,
        cache_key: str,
        identifier: Identifier,
        slug: str,
        record: Mapping[str, str],
        article: Any,
        lookup_type: str,
        lookup_value: str,
    ) -> Tuple[List[DownloadedFile], bool]:
        article_dir = base_dir / cache_key
        article_dir.mkdir(parents=True, exist_ok=True)

        payload = self._extract_payload(article)
        content_type = self._extract_content_type(article)
        format_hint = self._extract_format_hint(article)
        extension, file_type = self._infer_file_details(
            format_hint,
            content_type,
        )

        files: List[DownloadedFile] = []
        has_payload = bool(payload)
        if has_payload:
            payload_path = article_dir / f"content.{extension}"
            payload_path.write_bytes(payload)
            md5_hash = hashlib.md5(payload).hexdigest()
            files.append(
                DownloadedFile(
                    file_path=payload_path,
                    file_type=file_type,
                    content_type=content_type,
                    source=DownloadSource.ELSEVIER,
                    md5_hash=md5_hash,
                )
            )

        metadata = self._extract_metadata(article)
        metadata.setdefault("requested_record", dict(record))
        metadata.setdefault("slug", slug)
        metadata.setdefault("lookup_type", lookup_type)
        metadata.setdefault("lookup_value", lookup_value)
        metadata.setdefault("identifier_hash", identifier.hash_id)
        if "doi" not in metadata:
            metadata["doi"] = self._extract_doi(article)

        metadata_path = article_dir / "metadata.json"
        metadata_path.write_text(
            json.dumps(metadata, indent=2, default=str),
            encoding="utf-8",
        )
        files.append(
            DownloadedFile(
                file_path=metadata_path,
                file_type=FileType.JSON,
                content_type="application/json",
                source=DownloadSource.ELSEVIER,
            )
        )

        return files, has_payload

    @staticmethod
    def _slug_from_record(record: Mapping[str, str], index: int) -> str:
        candidate = (
            record.get("doi")
            or record.get("pmid")
            or record.get("pmcid")
        )
        if not candidate:
            candidate = f"record-{index}"
        slug = re.sub(r"[^A-Za-z0-9]+", "-", candidate).strip("-").lower()
        return slug or f"record-{index}"

    def _infer_file_details(
        self, format_hint: str | None, content_type: str
    ) -> tuple[str, FileType]:
        ext = (format_hint or "").strip().lower()
        if not ext:
            clean_content_type = content_type.split(";", 1)[0].strip().lower()
            ext = self._CONTENT_TYPE_TO_EXTENSION.get(
                clean_content_type, "bin"
            )
        file_type = self._EXTENSION_TO_FILETYPE.get(ext, FileType.BINARY)
        if ext == "bin":
            clean_content_type = content_type.split(";", 1)[0].strip().lower()
            fallback_ext = self._CONTENT_TYPE_TO_EXTENSION.get(
                clean_content_type
            )
            if fallback_ext:
                file_type = self._EXTENSION_TO_FILETYPE.get(
                    fallback_ext, file_type
                )
                ext = fallback_ext
        return ext or "bin", file_type

    @staticmethod
    def _primary_identifier(record: Mapping[str, str]) -> Tuple[str, str]:
        doi = record.get("doi")
        if doi:
            return "doi", str(doi)
        pmid = record.get("pmid")
        if pmid:
            return "pmid", str(pmid)
        raise ValueError(
            "Record must contain a DOI or PMID for Elsevier download."
        )

    @staticmethod
    def _lookup_key(
        record: Mapping[str, str],
    ) -> Tuple[str | None, str | None]:
        doi = str(record.get("doi") or "").strip().lower()
        pmid = str(record.get("pmid") or "").strip()
        return (doi or None, pmid or None)

    def _build_failure_result(
        self,
        *,
        identifier: Identifier,
        error_message: str,
    ) -> DownloadResult:
        return DownloadResult(
            identifier=identifier,
            source=DownloadSource.ELSEVIER,
            success=False,
            error_message=error_message,
        )

    def _ordered_results(
        self,
        identifiers: Identifiers,
        results_by_index: Dict[int, DownloadResult],
    ) -> List[DownloadResult]:
        ordered_results: List[DownloadResult] = []
        total = len(identifiers)
        for index in range(total):
            identifier = identifiers[index]
            result = results_by_index.get(index)
            if result is None:
                result = self._build_failure_result(
                    identifier=identifier,
                    error_message=(
                        "Elsevier download did not produce a result."
                    ),
                )
            ordered_results.append(result)
        return ordered_results

    @staticmethod
    def _record_from_identifier(identifier: Identifier) -> Dict[str, str]:
        record: Dict[str, str] = {}
        if identifier.doi:
            record["doi"] = identifier.doi
        if identifier.pmid:
            record["pmid"] = identifier.pmid
        if identifier.pmcid:
            record["pmcid"] = identifier.pmcid
        if "doi" not in record and "pmid" not in record:
            return {}
        return record

    def _identifier_cache_key(
        self, identifier: Identifier, index: int
    ) -> str:
        seed = identifier.hash_id.strip()
        if not seed:
            seed = f"identifier-{index}"
        digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()
        return digest[:32]

    @staticmethod
    def _extract_payload(article: Any) -> bytes:
        payload = getattr(article, "payload", None)
        if payload is None and isinstance(article, Mapping):
            payload = article.get("payload")
        if payload is None:
            return b""
        if isinstance(payload, bytes):
            return payload
        if isinstance(payload, str):
            return payload.encode("utf-8")
        try:
            return bytes(payload)
        except Exception:  # pragma: no cover - defensive fallback
            return json.dumps(payload, default=str).encode("utf-8")

    @staticmethod
    def _extract_content_type(article: Any) -> str:
        content_type = getattr(article, "content_type", None)
        if content_type is None and isinstance(article, Mapping):
            content_type = article.get("content_type")
        return content_type or "application/octet-stream"

    @staticmethod
    def _extract_format_hint(article: Any) -> str | None:
        format_hint = getattr(article, "format", None)
        if format_hint is None and isinstance(article, Mapping):
            format_hint = article.get("format")
        return format_hint

    @staticmethod
    def _extract_metadata(article: Any) -> Dict[str, Any]:
        metadata = getattr(article, "metadata", None)
        if metadata is None and isinstance(article, Mapping):
            metadata = article.get("metadata")
        if metadata is None:
            metadata = {}
        return dict(metadata)

    @staticmethod
    def _extract_doi(article: Any) -> str | None:
        doi = getattr(article, "doi", None)
        if doi is None and isinstance(article, Mapping):
            doi = article.get("doi")
        return doi


@contextlib.contextmanager
def _temporary_env(overrides: Mapping[str, str]) -> Iterable[None]:
    original: Dict[str, str | None] = {}
    try:
        for key, value in overrides.items():
            original[key] = os.environ.get(key)
            os.environ[key] = value
        yield
    finally:  # pragma: no cover - restore environment
        for key, previous in original.items():
            if previous is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = previous
