"""Download and extract tables from articles using Elsevier services."""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import math
import os
import re
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from collections import deque
from typing import Any, Callable, Deque, Dict, Iterable, List, Mapping, Optional, Tuple

from lxml import etree
from pubget._coordinate_space import _neurosynth_guess_space
from pubget._coordinates import _extract_coordinates_from_table

from elsevier_coordinate_extraction.client import ScienceDirectClient
from elsevier_coordinate_extraction.download.api import download_articles
from elsevier_coordinate_extraction.extract.text import save_article_text
from elsevier_coordinate_extraction.settings import (
    Settings as ElsevierSettings,
    get_settings as get_elsevier_settings,
)
from elsevier_coordinate_extraction.table_extraction import (
    extract_tables_from_article,
)
from elsevier_coordinate_extraction.types import ArticleContent

from ingestion_workflow.config import Settings, load_settings
from ingestion_workflow.extractors.base import BaseExtractor
from ingestion_workflow.models import (
    Coordinate,
    CoordinateSpace,
    ExtractedContent,
    ExtractedTable,
    Identifier,
    Identifiers,
    DownloadResult,
    DownloadSource,
    DownloadedFile,
    ExtractionResult,
    FileType,
)
from ingestion_workflow.utils.progress import emit_progress


logger = logging.getLogger(__name__)


class ElsevierExtractor(BaseExtractor):
    """Extractor that uses Elsevier to download and extract article content."""

    _SUPPORTED_IDS = {"pmid", "doi"}
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

    def download(
        self,
        identifiers: Identifiers,
        progress_hook: Callable[[int], None] | None = None,
    ) -> List[DownloadResult]:
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
                        "Identifier is missing a DOI or PMID required for Elsevier download."
                    ),
                )

        if not prepared:
            return self._ordered_results(
                identifiers,
                results_by_index,
                progress_hook=progress_hook,
            )

        try:
            articles = list(self._run_download(records, progress_hook))
        except Exception as exc:  # pragma: no cover - surfaced to caller
            failure_reason = str(exc) or "Unknown Elsevier download failure."
            for original_index, identifier, _ in prepared:
                results_by_index[original_index] = self._build_failure_result(
                    identifier=identifier,
                    error_message=failure_reason,
                )
            return self._ordered_results(
                identifiers,
                results_by_index,
                progress_hook=progress_hook,
            )

        base_dir = Path(
            self.settings.elsevier_cache_root or self.settings.get_cache_dir("elsevier")
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
                    lookup_key = self._lookup_key({str(identifier_type): str(identifier_value)})
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
            emit_progress(progress_hook)

        return self._ordered_results(identifiers, results_by_index)

    def extract(
        self,
        download_results: List[DownloadResult],
        progress_hook: Callable[[int], None] | None = None,
    ) -> List[ExtractionResult]:
        """Extract tables from downloaded articles using Elsevier."""
        if not download_results:
            return []

        for download_result in download_results:
            if not download_result.success:
                raise ValueError(
                    "ElsevierExtractor.extract received an unsuccessful "
                    "download result; validate downloads before "
                    "invoking extraction."
                )

        extraction_root = self._resolve_extraction_root()
        extraction_root.mkdir(parents=True, exist_ok=True)

        ordered_results: List[Optional[ExtractedContent]] = [None] * len(download_results)

        worker_count = max(1, self.settings.max_workers)

        if worker_count == 1 or len(download_results) == 1:
            for index, download_result in enumerate(download_results):
                ordered_results[index] = _run_elsevier_extraction_task(
                    download_result,
                    extraction_root,
                )
                emit_progress(progress_hook)
        else:
            root_str = str(extraction_root)
            with ProcessPoolExecutor(max_workers=worker_count) as executor:
                future_map = {
                    executor.submit(
                        _run_elsevier_extraction_task,
                        download_result,
                        root_str,
                    ): index
                    for index, download_result in enumerate(download_results)
                }
                for future in as_completed(future_map):
                    index = future_map[future]
                    download_result = download_results[index]
                    try:
                        ordered_results[index] = future.result()
                    except Exception as exc:  # pragma: no cover
                        logger.exception(
                            "Elsevier extraction raised exception for %s",
                            download_result.identifier.hash_id,
                        )
                        ordered_results[index] = _build_failure_extraction(
                            download_result,
                            f"Elsevier extraction raised an exception: {exc}",
                        )
                    finally:
                        emit_progress(progress_hook)

        final_results: List[ExtractedContent] = []
        for index, download_result in enumerate(download_results):
            result = ordered_results[index]
            if result is None:
                result = _build_failure_extraction(
                    download_result,
                    "Elsevier extraction did not produce a result.",
                )
            final_results.append(result)

        return final_results

    def _resolve_extraction_root(self) -> Path:
        base = self.settings.get_cache_dir("extract")
        root = base / "elsevier"
        root.mkdir(parents=True, exist_ok=True)
        return root

    def _run_download(
        self,
        records: List[Dict[str, str]],
        progress_hook: Callable[[int], None] | None = None,
    ) -> List[Any]:
        elsevier_settings = self._build_elsevier_settings()
        progress_proxy = _build_progress_callback(progress_hook) if progress_hook else None

        async def _runner() -> List[Any]:
            if self._client is None:
                async with ScienceDirectClient(elsevier_settings) as client:
                    return await download_articles(
                        records,
                        client=client,
                        cache=self._cache,
                        settings=elsevier_settings,
                        progress_callback=progress_proxy,
                    )
            return await download_articles(
                records,
                client=self._client,
                cache=self._cache,
                settings=elsevier_settings,
                progress_callback=progress_proxy,
            )

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(_runner())
        if loop.is_running():  # pragma: no cover - defensive
            raise RuntimeError(
                "ElsevierExtractor.download cannot run inside an active event loop."
            )
        return loop.run_until_complete(_runner())

    def _build_elsevier_settings(self) -> ElsevierSettings:
        overrides: Dict[str, str] = {}
        if self.settings.elsevier_api_key:
            overrides["ELSEVIER_API_KEY"] = self.settings.elsevier_api_key

        cache_root = self.settings.elsevier_cache_root or self.settings.get_cache_dir("elsevier")
        overrides["ELSEVIER_CACHE_DIR"] = str(cache_root)

        if self.settings.elsevier_http_proxy:
            overrides["ELSEVIER_HTTP_PROXY"] = self.settings.elsevier_http_proxy
        if self.settings.elsevier_https_proxy:
            overrides["ELSEVIER_HTTPS_PROXY"] = self.settings.elsevier_https_proxy
        overrides["ELSEVIER_USE_PROXY"] = "true" if self.settings.elsevier_use_proxy else "false"
        overrides["ELSEVIER_CONCURRENCY"] = str(max(1, self.settings.max_workers))

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

        success = has_payload and any(file.file_type is not FileType.JSON for file in files)
        error_message = None
        if not success:
            error_message = "Elsevier returned metadata without content payload."

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
        candidate = record.get("doi") or record.get("pmid") or record.get("pmcid")
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
            ext = self._CONTENT_TYPE_TO_EXTENSION.get(clean_content_type, "bin")
        file_type = self._EXTENSION_TO_FILETYPE.get(ext, FileType.BINARY)
        if ext == "bin":
            clean_content_type = content_type.split(";", 1)[0].strip().lower()
            fallback_ext = self._CONTENT_TYPE_TO_EXTENSION.get(clean_content_type)
            if fallback_ext:
                file_type = self._EXTENSION_TO_FILETYPE.get(fallback_ext, file_type)
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
        raise ValueError("Record must contain a DOI or PMID for Elsevier download.")

    @staticmethod
    def _lookup_key(
        record: Mapping[str, str],
    ) -> Tuple[str | None, str | None]:
        doi = str(record.get("doi") or "").strip().lower()
        pmid = str(record.get("pmid") or "").strip()
        return (doi or None, pmid or None)

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
        *,
        progress_hook: Callable[[int], None] | None = None,
    ) -> List[DownloadResult]:
        ordered_results: List[DownloadResult] = []
        total = len(identifiers)
        for index in range(total):
            identifier = identifiers[index]
            result = results_by_index.get(index)
            if result is None:
                result = self._build_failure_result(
                    identifier=identifier,
                    error_message=("Elsevier download did not produce a result."),
                )
            ordered_results.append(result)
            emit_progress(progress_hook)
        return ordered_results

    def _identifier_cache_key(self, identifier: Identifier, index: int) -> str:
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


def _run_elsevier_extraction_task(
    download_result: DownloadResult,
    extraction_root: Path | str,
) -> ExtractedContent:
    """Worker function to extract content from a single Elsevier article."""
    root_path = Path(extraction_root)
    try:
        return _extract_elsevier_article(download_result, root_path)
    except Exception as exc:  # pragma: no cover - worker safeguard
        logger.exception(
            "Elsevier extraction failed for %s",
            download_result.identifier.hash_id,
        )
        return _build_failure_extraction(download_result, str(exc))


def _build_failure_extraction(
    download_result: DownloadResult,
    message: str,
    full_text_path: Path | None = None,
) -> ExtractedContent:
    """Build a failed extraction result."""
    return ExtractedContent(
        hash_id=download_result.identifier.hash_id,
        source=DownloadSource.ELSEVIER,
        identifier=download_result.identifier,
        full_text_path=full_text_path,
        tables=[],
        has_coordinates=False,
        error_message=message,
    )


def _extract_elsevier_article(
    download_result: DownloadResult,
    extraction_root: Path,
) -> ExtractedContent:
    """Extract text, tables, and coordinates from an Elsevier article."""
    # Find the XML content file
    content_file = _select_content_file(download_result)
    if content_file is None:
        raise ValueError(
            "Elsevier extraction requires XML content; PDF-only articles are not supported."
        )

    # Load metadata
    metadata_file = _select_metadata_file(download_result)
    if metadata_file is None:
        raise ValueError("Elsevier extraction requires metadata.json.")

    try:
        metadata = json.loads(metadata_file.file_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        raise ValueError(f"Failed to load Elsevier metadata: {exc}") from exc

    # Setup output directory
    hash_id = download_result.identifier.hash_id
    output_dir = extraction_root / _safe_hash_stem(hash_id)
    tables_output_dir = output_dir / "tables"
    tables_output_dir.mkdir(parents=True, exist_ok=True)

    # Read XML payload
    payload = content_file.file_path.read_bytes()

    # Build ArticleContent for text extraction
    doi = metadata.get("doi") or download_result.identifier.doi or ""
    article_content = ArticleContent(
        doi=doi,
        payload=payload,
        content_type=content_file.content_type,
        format=content_file.file_path.suffix.lstrip("."),
        retrieved_at=metadata.get("retrieved_at") or "",
        metadata=metadata,
    )

    # Extract and save article text
    try:
        full_text_path = save_article_text(
            article_content,
            output_dir,
            stem="article",
        )
    except Exception as exc:
        logger.warning(
            "Failed to extract text for %s: %s",
            hash_id,
            exc,
        )
        # Create a fallback text file
        output_dir.mkdir(parents=True, exist_ok=True)
        full_text_path = output_dir / "article.txt"
        try:
            tree = etree.fromstring(payload)
            text = " ".join(tree.xpath(".//text()"))
            full_text_path.write_text(text, encoding="utf-8")
        except Exception:  # pragma: no cover
            full_text_path.write_text("", encoding="utf-8")

    # Extract article text for space detection
    try:
        tree = etree.fromstring(payload)
        article_text = " ".join(tree.xpath(".//text()"))
    except Exception as exc:
        logger.warning(
            "Failed to parse article XML for %s: %s",
            hash_id,
            exc,
        )
        article_text = ""

    # Detect coordinate space from article text
    article_space = _coordinate_space_from_guess(_neurosynth_guess_space(article_text))

    # Extract tables
    try:
        tables = extract_tables_from_article(payload)
    except Exception as exc:
        message = f"Elsevier table extraction failed: {exc}"
        logger.error("%s: %s", hash_id, message)
        return _build_failure_extraction(
            download_result,
            message,
            full_text_path,
        )

    if not tables:
        message = "Elsevier found no tables in the article."
        logger.warning("%s: %s", hash_id, message)
        return _build_failure_extraction(
            download_result,
            message,
            full_text_path,
        )

    # Process each table
    extracted_tables: List[ExtractedTable] = []
    table_sources: Dict[str, Dict[str, str]] = {}
    failure_reasons: List[str] = []

    for index, (table_metadata, table_frame) in enumerate(tables):
        # Generate table ID
        sanitized_id = _sanitize_table_id(
            table_metadata.identifier,
            table_metadata.label,
            index,
        )

        # Write raw table XML
        raw_table_path = tables_output_dir / f"{sanitized_id}.xml"
        if table_metadata.raw_xml:
            raw_table_path.write_text(
                table_metadata.raw_xml,
                encoding="utf-8",
            )
        else:
            raw_table_path.write_text("<table/>", encoding="utf-8")

        # Extract coordinates
        try:
            coordinates_frame = _extract_coordinates_from_table(table_frame)
        except Exception as exc:
            reason = f"Coordinate parsing failed for {sanitized_id}: {exc}"
            failure_reasons.append(reason)
            logger.warning("%s: %s", hash_id, reason)
            coordinates_frame = None

        # Save coordinates CSV
        coordinates: List[Coordinate] = []
        coordinate_csv_path = tables_output_dir.joinpath(f"{sanitized_id}_coordinates.csv")
        if coordinates_frame is not None:
            coordinates_frame.to_csv(coordinate_csv_path, index=False)
            if not coordinates_frame.empty:
                for _, row in coordinates_frame.iterrows():
                    coord = _coordinate_from_row(row, article_space)
                    if coord is not None:
                        coordinates.append(coord)
        else:
            # Write header-only CSV
            coordinate_csv_path.write_text("x,y,z\n", encoding="utf-8")

        # Record sources
        table_sources[sanitized_id] = {
            "table_label": table_metadata.label or "",
            "table_id": table_metadata.identifier or "",
            "raw_xml_path": str(raw_table_path),
            "coordinates_path": str(coordinate_csv_path),
        }

        # Parse table number
        table_number = _parse_table_number(table_metadata.label)

        # Build metadata dict
        extraction_metadata = {
            "table_id": table_metadata.identifier,
            "table_label": table_metadata.label,
            "raw_xml_path": str(raw_table_path),
            "coordinates_path": str(coordinate_csv_path),
        }

        extracted_tables.append(
            ExtractedTable(
                table_id=sanitized_id,
                raw_content_path=raw_table_path,
                table_number=table_number,
                caption=table_metadata.caption or "",
                footer=table_metadata.foot or "",
                metadata={k: v for k, v in extraction_metadata.items() if v},
                coordinates=coordinates,
                space=article_space,
            )
        )

    # Check if extraction succeeded
    if not extracted_tables:
        message = "Elsevier failed to extract any tables."
        if failure_reasons:
            message = f"{message} Reasons: {' | '.join(failure_reasons)}"
        logger.error("%s: %s", hash_id, message)
        return _build_failure_extraction(
            download_result,
            message,
            full_text_path,
        )

    # Write manifest
    sources_path = tables_output_dir / "table_sources.json"
    sources_path.write_text(
        json.dumps(table_sources, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    # Build result
    error_message = None
    if failure_reasons:
        error_message = "Elsevier skipped tables: " + " | ".join(failure_reasons)

    has_coordinates = any(table.coordinates for table in extracted_tables)

    return ExtractedContent(
        hash_id=hash_id,
        source=DownloadSource.ELSEVIER,
        identifier=download_result.identifier,
        full_text_path=full_text_path,
        tables=extracted_tables,
        has_coordinates=has_coordinates,
        error_message=error_message,
    )


def _select_content_file(
    download_result: DownloadResult,
) -> Optional[DownloadedFile]:
    """Find the XML content file from download results."""
    for downloaded in download_result.files:
        if downloaded.file_type is FileType.XML and downloaded.file_path.name.startswith(
            "content."
        ):
            return downloaded
    return None


def _select_metadata_file(
    download_result: DownloadResult,
) -> Optional[DownloadedFile]:
    """Find the metadata JSON file from download results."""
    for downloaded in download_result.files:
        if downloaded.file_type is FileType.JSON and downloaded.file_path.name == "metadata.json":
            return downloaded
    return None


def _safe_hash_stem(hash_id: str | None) -> str:
    """Create a safe directory name from a hash ID."""
    candidate = hash_id or ""
    sanitized = re.sub(r"[^A-Za-z0-9_-]+", "-", candidate).strip("-_")
    if sanitized:
        return sanitized.lower()
    digest = hashlib.sha256(candidate.encode("utf-8")).hexdigest()
    return digest[:16]


def _sanitize_table_id(
    table_id: Optional[str],
    table_label: Optional[str],
    index: int,
) -> str:
    """Create a sanitized table ID from metadata."""
    fallback = f"table-{index + 1:03d}"
    candidate = table_id or table_label or fallback
    sanitized = re.sub(r"[^A-Za-z0-9_-]+", "-", candidate).strip("-")
    return sanitized.lower() or fallback


def _coordinate_space_from_guess(guess: Optional[str]) -> CoordinateSpace:
    """Convert Pubget space guess to CoordinateSpace enum."""
    if not guess:
        return CoordinateSpace.OTHER
    normalized = guess.strip().upper()
    if normalized == "MNI":
        return CoordinateSpace.MNI
    if normalized in {"TAL", "TALAIRACH"}:
        return CoordinateSpace.TALAIRACH
    return CoordinateSpace.OTHER


def _build_progress_callback(
    progress_hook: Callable[[int], None] | None,
) -> Callable[[Mapping[str, str], Any | None, BaseException | None], None] | None:
    if progress_hook is None:
        return None

    def _callback(record, article, error):
        emit_progress(progress_hook)

    return _callback


def _coordinate_from_row(
    row: Any,
    space: CoordinateSpace,
) -> Optional[Coordinate]:
    """Convert a DataFrame row to a Coordinate object."""
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


def _parse_table_number(label: Optional[str]) -> Optional[int]:
    """Extract table number from label string."""
    if not label:
        return None
    match = re.search(r"(\d+)", label)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:
        return None
