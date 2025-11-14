"""Caching services for the ingestion workflow.

This module focuses on download caching by default, but the helpers can be
reused for other workflow namespaces (for example, ``gather`` or ``extract``)
by providing the appropriate ``namespace`` parameter. Each extractor receives
its own cache namespace rooted under ``cache_root/<namespace>/<extractor>``
with an ``index.sqlite`` that records successful downloads. The cache index is
used to avoid redundant API calls and to hydrate workflow steps in cache-only
mode.

The workflow layer is responsible for deciding which downloads are needed; the
functions provided here expose helpers to:

* load the cache index for a specific extractor
* partition identifiers into cached and uncached sets
* persist new ``DownloadResult`` entries under a file lock
* invalidate cached entries on demand
* bootstrap indices for legacy downloads (stub implementation for now)

Storing and indexing legacy cache directories (for example, HTML payloads that
predate this workflow) will require lookup services to expand partial
identifiers. ``index_legacy_downloads`` serves as the entry point for that
logic and will be implemented in a follow-up pass once the lookup pipeline is
available.
"""

from __future__ import annotations

import json
import re
import xml.etree.ElementTree as ET
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable, Iterator, List, Sequence, Tuple, Type, TypeVar

from filelock import FileLock
from tqdm.auto import tqdm

from ingestion_workflow.config import Settings
from ingestion_workflow.models import (
    CreateAnalysesResult,
    CreateAnalysesResultEntry,
    CreateAnalysesResultIndex,
    DownloadIndex,
    DownloadedFile,
    DownloadResult,
    DownloadSource,
    FileType,
    ExtractionResult,
    Identifier,
    IdentifierCacheEntry,
    IdentifierCacheIndex,
    Identifiers,
)
from ingestion_workflow.models.cache import (
    CacheIndex,
    ExtractionResultEntry,
    ExtractionResultIndex,
)


DOWNLOAD_CACHE_NAMESPACE = "download"
EXTRACT_CACHE_NAMESPACE = "extract"
GATHER_CACHE_NAMESPACE = "gather"
CREATE_ANALYSES_CACHE_NAMESPACE = "create_analyses"
INDEX_FILENAME = "index.sqlite"
LOCK_FILENAME = "index.lock"
LEGACY_INDEX_BATCH_SIZE = 10000


TIndex = TypeVar("TIndex", bound=CacheIndex)


def _namespace_root(settings: Settings, namespace: str) -> Path:
    root = settings.cache_root / namespace
    root.mkdir(parents=True, exist_ok=True)
    return root


def _extractor_root(
    settings: Settings,
    namespace: str,
    extractor_name: str | None,
) -> Path:
    namespace_root = _namespace_root(settings, namespace)
    if extractor_name is None:
        return namespace_root
    extractor_dir = namespace_root / extractor_name
    extractor_dir.mkdir(parents=True, exist_ok=True)
    return extractor_dir


def _index_path(
    settings: Settings,
    namespace: str,
    extractor_name: str | None,
) -> Path:
    extractor_root = _extractor_root(settings, namespace, extractor_name)
    return extractor_root / INDEX_FILENAME


def _analysis_artifacts_dir(
    settings: Settings,
    extractor_name: str | None,
) -> Path:
    root = _extractor_root(settings, CREATE_ANALYSES_CACHE_NAMESPACE, extractor_name)
    artifacts = root / "artifacts"
    artifacts.mkdir(parents=True, exist_ok=True)
    return artifacts


def _sanitize_manifest_filename(value: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9._-]+", "-", value)
    sanitized = sanitized.strip("-")
    return sanitized or "analysis"


def _analysis_manifest_path(
    settings: Settings,
    extractor_name: str | None,
    slug: str,
) -> Path:
    base = _analysis_artifacts_dir(settings, extractor_name)
    filename = f"{_sanitize_manifest_filename(slug)}.jsonl"
    return base / filename


def _lock_path(
    settings: Settings,
    namespace: str,
    extractor_name: str | None,
) -> Path:
    extractor_root = _extractor_root(settings, namespace, extractor_name)
    return extractor_root / LOCK_FILENAME


@contextmanager
def _acquire_lock(
    settings: Settings,
    namespace: str,
    extractor_name: str | None,
) -> Iterator[None]:
    lock = FileLock(str(_lock_path(settings, namespace, extractor_name)))
    lock.acquire()
    try:
        yield
    finally:
        lock.release()


def load_download_index(
    settings: Settings,
    extractor_name: str,
    *,
    namespace: str = DOWNLOAD_CACHE_NAMESPACE,
) -> DownloadIndex:
    """Load the download index for an extractor without acquiring a lock."""

    return load_index(
        settings,
        namespace,
        extractor_name,
        index_cls=DownloadIndex,
    )


def load_index(
    settings: Settings,
    namespace: str,
    extractor_name: str | None,
    *,
    index_cls: Type[TIndex],
) -> TIndex:
    """Load an arbitrary cache index without acquiring a lock."""

    index_path = _index_path(settings, namespace, extractor_name)
    return index_cls.load(index_path)


def load_extractor_index(
    settings: Settings,
    extractor_name: str,
    *,
    namespace: str = EXTRACT_CACHE_NAMESPACE,
) -> ExtractionResultIndex:
    """Convenience wrapper for extractor namespace indices."""

    return load_index(
        settings,
        namespace,
        extractor_name,
        index_cls=ExtractionResultIndex,
    )


def load_gather_index(
    settings: Settings,
    extractor_name: str,
    *,
    namespace: str = GATHER_CACHE_NAMESPACE,
) -> IdentifierCacheIndex:
    """Convenience wrapper for gather namespace indices."""

    return load_index(
        settings,
        namespace,
        extractor_name,
        index_cls=IdentifierCacheIndex,
    )


def load_create_analyses_index(
    settings: Settings,
    extractor_name: str | None = None,
    *,
    namespace: str = CREATE_ANALYSES_CACHE_NAMESPACE,
) -> CreateAnalysesResultIndex:
    """Convenience wrapper for create_analyses namespace indices."""

    return load_index(
        settings,
        namespace,
        extractor_name,
        index_cls=CreateAnalysesResultIndex,
    )


def get_cached_create_analyses_result(
    settings: Settings,
    cache_key: str,
    extractor_name: str | None = None,
    *,
    namespace: str = CREATE_ANALYSES_CACHE_NAMESPACE,
) -> CreateAnalysesResult | None:
    """Retrieve a cached create-analyses entry if present."""

    index = load_create_analyses_index(
        settings,
        extractor_name,
        namespace=namespace,
    )
    entry = index.get(cache_key)
    if entry is None:
        return None
    return entry.clone_payload()


def cache_create_analyses_results(
    settings: Settings,
    extractor_name: str | None,
    results: Sequence[CreateAnalysesResult],
    *,
    namespace: str = CREATE_ANALYSES_CACHE_NAMESPACE,
) -> None:
    """Persist create-analyses results to the cache index."""

    if not results:
        return

    with _acquire_lock(settings, namespace, extractor_name):
        index_path = _index_path(settings, namespace, extractor_name)
        index = CreateAnalysesResultIndex.load(index_path)
        entries_to_add: List[CreateAnalysesResultEntry] = []
        for result in results:
            manifest_path = _analysis_manifest_path(settings, extractor_name, result.slug)
            manifest_path.write_text(
                json.dumps(result.analysis_collection.to_dict(), indent=2),
                encoding="utf-8",
            )
            result.analysis_paths = [manifest_path]
            entry = CreateAnalysesResultEntry.from_result(result)
            entries_to_add.append(entry)
        index.add_entries(entries_to_add)


def get_identifier_cache_entry(
    settings: Settings,
    extractor_name: str,
    identifier: Identifier,
    *,
    namespace: str = GATHER_CACHE_NAMESPACE,
) -> IdentifierCacheEntry | None:
    """Retrieve a cached identifier expansion entry if present."""

    index = load_gather_index(
        settings,
        extractor_name,
        namespace=namespace,
    )
    return index.get(identifier.slug)


def cache_identifier_entries(
    settings: Settings,
    extractor_name: str,
    entries: Sequence[IdentifierCacheEntry],
    *,
    namespace: str = GATHER_CACHE_NAMESPACE,
) -> None:
    """Persist identifier cache entries for gather namespace lookups."""

    if not entries:
        return

    with _acquire_lock(settings, namespace, extractor_name):
        index_path = _index_path(settings, namespace, extractor_name)
        index = IdentifierCacheIndex.load(index_path)
        cloned_entries = [
            IdentifierCacheEntry.from_expansion(entry.clone_payload()) for entry in entries
        ]
        index.add_entries(cloned_entries)


def partition_cached_downloads(
    settings: Settings,
    extractor_name: str,
    identifiers: Identifiers,
    *,
    namespace: str = DOWNLOAD_CACHE_NAMESPACE,
) -> Tuple[List[DownloadResult], List[Identifier]]:
    """Split identifiers into cached results and uncached identifiers.

    Returns
    -------
    cached_results : list[DownloadResult]
        Cached download results aligned with the incoming identifiers.
    missing_identifiers : list[Identifier]
        Identifiers that do not have a cached download.
    """

    index = load_download_index(settings, extractor_name, namespace=namespace)
    cached_results: List[DownloadResult] = []
    missing: List[Identifier] = []

    for identifier in identifiers.identifiers:
        entry = index.get_download(identifier.slug)
        if entry is None:
            missing.append(identifier)
            continue
        payload = entry.clone_payload()
        payload.identifier = identifier
        cached_results.append(payload)

    return cached_results, missing


def cache_download_results(
    settings: Settings,
    extractor_name: str,
    results: Sequence[DownloadResult],
    *,
    namespace: str = DOWNLOAD_CACHE_NAMESPACE,
) -> None:
    """Persist successful download results to the cache index."""

    if not results:
        return

    with _acquire_lock(settings, namespace, extractor_name):
        index_path = _index_path(settings, namespace, extractor_name)
        index = DownloadIndex.load(index_path)
        index.add_downloads(results)


def invalidate_download_cache(
    settings: Settings,
    extractor_name: str,
    slugs: Iterable[str],
    *,
    namespace: str = DOWNLOAD_CACHE_NAMESPACE,
) -> None:
    """Remove specific identifiers from the download cache index."""

    slugs = list(slugs)
    if not slugs:
        return

    with _acquire_lock(settings, namespace, extractor_name):
        index_path = _index_path(settings, namespace, extractor_name)
        index = DownloadIndex.load(index_path)
        for slug in slugs:
            index.remove_download(slug)


def index_legacy_downloads(
    settings: Settings,
    extractor_name: str,
    source_directory: Path,
    *,
    namespace: str = DOWNLOAD_CACHE_NAMESPACE,
) -> DownloadIndex:
    """Index historical downloads for ACE/Pubget into the cache."""

    extractor = DownloadSource(extractor_name)
    source_directory = Path(source_directory)
    if not source_directory.exists():
        raise FileNotFoundError(f"Legacy source directory not found: {source_directory}")
    if not source_directory.is_dir():
        raise NotADirectoryError(f"Legacy source path must be a directory: {source_directory}")

    if extractor is DownloadSource.ACE:
        results_iter = _collect_ace_legacy_results(source_directory)
    elif extractor is DownloadSource.PUBGET:
        results_iter = _collect_pubget_legacy_results(source_directory)
    else:  # pragma: no cover - unsupported legacy extractor
        raise ValueError(f"Legacy indexing not supported for extractor '{extractor_name}'.")

    index = load_download_index(settings, extractor_name, namespace=namespace)
    slug_set, pmid_set, pmcid_set, doi_set = _build_identifier_filters(index)

    progress = tqdm(
        desc=f"Index[{extractor.value}] legacy",
        unit="file",
        leave=False,
        dynamic_ncols=True,
    )
    batch: List[DownloadResult] = []

    try:
        for result in results_iter:
            progress.update(1)
            if _is_duplicate_identifier(result.identifier, slug_set, pmid_set, pmcid_set, doi_set):
                continue
            batch.append(result)
            if len(batch) >= LEGACY_INDEX_BATCH_SIZE:
                _persist_legacy_batch(
                    settings,
                    extractor_name,
                    batch,
                    slug_set,
                    pmid_set,
                    pmcid_set,
                    doi_set,
                    namespace,
                )
                batch.clear()

        if batch:
            _persist_legacy_batch(
                settings,
                extractor_name,
                batch,
                slug_set,
                pmid_set,
                pmcid_set,
                doi_set,
                namespace,
            )
    finally:
        progress.close()

    return load_download_index(settings, extractor_name, namespace=namespace)


_CONTENT_TYPE_MAP = {
    "html": (FileType.HTML, "text/html"),
    "htm": (FileType.HTML, "text/html"),
    "xml": (FileType.XML, "text/xml"),
    "json": (FileType.JSON, "application/json"),
    "csv": (FileType.CSV, "text/csv"),
    "txt": (FileType.TEXT, "text/plain"),
    "text": (FileType.TEXT, "text/plain"),
    "pdf": (FileType.PDF, "application/pdf"),
}


def _collect_ace_legacy_results(root: Path) -> Iterator[DownloadResult]:
    for html_file in root.rglob("*.html"):
        if not html_file.is_file():
            continue
        pmid = html_file.stem.strip()
        if not pmid:
            continue
        identifier = Identifier(pmid=pmid)
        downloaded_file = DownloadedFile(
            file_path=html_file,
            file_type=FileType.HTML,
            content_type="text/html",
            source=DownloadSource.ACE,
        )
        yield DownloadResult(
            identifier=identifier,
            source=DownloadSource.ACE,
            success=True,
            files=[downloaded_file],
        )


def _collect_pubget_legacy_results(root: Path) -> Iterator[DownloadResult]:
    for article_xml in _pubget_article_files(root):
        article_dir = article_xml.parent
        pmcid = _derive_pmcid_from_path(article_dir) or _extract_pmcid_from_xml(article_xml)
        if not pmcid:
            continue
        identifier = Identifier(pmcid=pmcid)
        files: List[DownloadedFile] = []
        for file_path in article_dir.rglob("*"):
            if not file_path.is_file():
                continue
            file_type, content_type = _guess_file_type(file_path.suffix)
            files.append(
                DownloadedFile(
                    file_path=file_path,
                    file_type=file_type,
                    content_type=content_type,
                    source=DownloadSource.PUBGET,
                )
            )
        if not files:
            continue
        yield DownloadResult(
            identifier=identifier,
            source=DownloadSource.PUBGET,
            success=True,
            files=files,
        )


def _pubget_article_files(root: Path) -> Iterator[Path]:
    for path in root.rglob("*"):
        if path.is_file() and path.name.lower() == "article.xml":
            yield path


def _derive_pmcid_from_path(article_dir: Path) -> Optional[str]:
    for part in reversed(article_dir.parts):
        candidate = _normalize_pmcid_token(part)
        if candidate:
            return candidate
    return None


def _normalize_pmcid_token(value: str) -> Optional[str]:
    token = value.strip()
    if not token:
        return None
    lowered = token.lower()
    if lowered.startswith("pmcid"):
        suffix = token[5:]
        suffix = suffix.lstrip("_- ")
    elif lowered.startswith("pmc"):
        suffix = token[3:]
        suffix = suffix.lstrip("_- ")
        if not suffix:
            suffix = token[3:]
    else:
        return None
    cleaned = suffix.strip().upper()
    if not cleaned:
        return None
    if not cleaned.startswith("PMC"):
        cleaned = f"PMC{cleaned}"
    return cleaned


def _extract_pmcid_from_xml(article_xml: Path) -> Optional[str]:
    try:
        for _event, elem in ET.iterparse(article_xml, events=("end",)):
            if elem.tag.lower().endswith("article-id") and elem.attrib.get("pub-id-type") == "pmcid":
                text = (elem.text or "").strip()
                elem.clear()
                if not text:
                    continue
                normalized = text.upper()
                if not normalized.startswith("PMC"):
                    normalized = f"PMC{normalized}"
                return normalized
            elem.clear()
    except ET.ParseError:
        return None
    return None


def _guess_file_type(suffix: str) -> Tuple[FileType, str]:
    normalized = suffix.lower().lstrip(".")
    if normalized in _CONTENT_TYPE_MAP:
        return _CONTENT_TYPE_MAP[normalized]
    return FileType.BINARY, "application/octet-stream"


def _enrich_identifiers_with_lookups(
    settings: Settings, results: Sequence[DownloadResult]
) -> None:
    if not results:
        return
    services = _build_lookup_services(settings)
    if not services:
        return
    identifiers = Identifiers([result.identifier for result in results])
    for service in services:
        service.find_identifiers(identifiers)


def _build_lookup_services(settings: Settings):
    from ingestion_workflow.services.id_lookup import (
        IDLookupService,
        OpenAlexIDLookupService,
        PubMedIDLookupService,
        SemanticScholarIDLookupService,
    )

    candidates: List[IDLookupService] = []
    for cls in (
        SemanticScholarIDLookupService,
        PubMedIDLookupService,
        OpenAlexIDLookupService,
    ):
        service = cls(settings)
        if service.can_run():
            candidates.append(service)
    return candidates


def _build_identifier_filters(
    index: DownloadIndex,
) -> Tuple[set[str], set[str], set[str], set[str]]:
    return index.identifier_sets()


def _persist_legacy_batch(
    settings: Settings,
    extractor_name: str,
    batch: List[DownloadResult],
    slug_set: set[str],
    pmid_set: set[str],
    pmcid_set: set[str],
    doi_set: set[str],
    namespace: str,
) -> None:
    if not batch:
        return

    _enrich_identifiers_with_lookups(settings, batch)

    new_results: List[DownloadResult] = []
    for result in batch:
        identifier = result.identifier
        if not identifier.slug:
            continue
        if _is_duplicate_identifier(identifier, slug_set, pmid_set, pmcid_set, doi_set):
            continue
        new_results.append(result)
        _add_identifier_keys(identifier, slug_set, pmid_set, pmcid_set, doi_set)

    if not new_results:
        return

    with _acquire_lock(settings, namespace, extractor_name):
        index_path = _index_path(settings, namespace, extractor_name)
        index = DownloadIndex.load(index_path)
        pending: List[DownloadResult] = []
        for result in new_results:
            slug = result.identifier.slug
            if not slug or index.has(slug):
                continue
            pending.append(result)
        if pending:
            index.add_downloads(pending)


def _is_duplicate_identifier(
    identifier: Identifier,
    slug_set: set[str],
    pmid_set: set[str],
    pmcid_set: set[str],
    doi_set: set[str],
) -> bool:
    slug = identifier.slug
    if slug and slug in slug_set:
        return True
    if identifier.pmid and identifier.pmid in pmid_set:
        return True
    if identifier.pmcid and identifier.pmcid in pmcid_set:
        return True
    if identifier.doi and identifier.doi in doi_set:
        return True
    return False


def _add_identifier_keys(
    identifier: Identifier,
    slug_set: set[str],
    pmid_set: set[str],
    pmcid_set: set[str],
    doi_set: set[str],
) -> None:
    slug = identifier.slug
    if slug:
        slug_set.add(slug)
    if identifier.pmid:
        pmid_set.add(identifier.pmid)
    if identifier.pmcid:
        pmcid_set.add(identifier.pmcid)
    if identifier.doi:
        doi_set.add(identifier.doi)


def partition_cached_extractions(
    settings: Settings,
    extractor_name: str,
    download_results: Sequence[DownloadResult],
    *,
    namespace: str = EXTRACT_CACHE_NAMESPACE,
) -> Tuple[List[ExtractionResult | None], List[DownloadResult]]:
    """Split download results into cached extractions and missing work."""

    index = load_extractor_index(settings, extractor_name, namespace=namespace)
    cached_results: List[ExtractionResult | None] = []
    missing: List[DownloadResult] = []

    for download_result in download_results:
        entry = index.get_extraction(download_result.identifier.slug)
        if entry is None:
            cached_results.append(None)
            missing.append(download_result)
            continue
        cached_results.append(entry.clone_payload())

    return cached_results, missing


def cache_extraction_results(
    settings: Settings,
    extractor_name: str,
    results: Sequence[ExtractionResult],
    *,
    namespace: str = EXTRACT_CACHE_NAMESPACE,
) -> None:
    """Persist extraction outputs to the cache index."""

    if not results:
        return

    with _acquire_lock(settings, namespace, extractor_name):
        index_path = _index_path(settings, namespace, extractor_name)
        index = ExtractionResultIndex.load(index_path)
        entries = [ExtractionResultEntry.from_content(result) for result in results]
        index.add_entries(entries)
