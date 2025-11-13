"""Caching services for the ingestion workflow.

This module focuses on download caching by default, but the helpers can be
reused for other workflow namespaces (for example, ``gather`` or ``extract``)
by providing the appropriate ``namespace`` parameter. Each extractor receives
its own cache namespace rooted under ``cache_root/<namespace>/<extractor>``
with an ``index.json`` that records successful downloads. The cache index is
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

from contextlib import contextmanager
from pathlib import Path
from typing import Iterable, Iterator, List, Sequence, Tuple, Type, TypeVar

from filelock import FileLock

from ingestion_workflow.config import Settings
from ingestion_workflow.models import (
    CreateAnalysesResult,
    CreateAnalysesResultEntry,
    CreateAnalysesResultIndex,
    DownloadIndex,
    DownloadResult,
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
INDEX_FILENAME = "index.json"
LOCK_FILENAME = "index.lock"


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
        for result in results:
            entry = CreateAnalysesResultEntry.from_result(result)
            index.add_result(entry)
        index.index_path = index_path
        index.save()


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
        for entry in entries:
            index.add_entry(IdentifierCacheEntry.from_expansion(entry.clone_payload()))
        index.index_path = index_path
        index.save()


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
        for result in results:
            index.add_download(result)
        index.index_path = index_path
        index.save()


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
        removed = False
        for slug in slugs:
            removed |= index.remove_download(slug)
        if removed:
            index.index_path = index_path
            index.save()


def index_legacy_downloads(
    settings: Settings,
    extractor_name: str,
    source_directory: Path,
    *,
    namespace: str = DOWNLOAD_CACHE_NAMESPACE,
) -> DownloadIndex:
    """Stub for indexing legacy downloads resident outside the workflow.

    The implementation will:
    1. walk ``source_directory`` and detect candidate payloads
    2. use the identifier lookup service to enrich partial identifiers
    3. persist synthetic ``DownloadResult`` entries into the cache index

    Returns the updated index once implemented.
    """

    raise NotImplementedError("Legacy download indexing not yet implemented.")


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
        for result in results:
            index.add_extraction(ExtractionResultEntry.from_content(result))
        index.index_path = index_path
        index.save()
