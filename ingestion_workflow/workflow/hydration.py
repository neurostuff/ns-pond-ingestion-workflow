"""Shared cache hydration helpers for workflow stages."""

from __future__ import annotations

from typing import Dict, Iterable, List, Optional, Set

from ingestion_workflow.models import ArticleExtractionBundle, DownloadResult, Identifiers
from ingestion_workflow.services import cache
from ingestion_workflow.workflow.common import identifier_aliases


def hydrate_downloads_from_cache(
    settings,
    identifiers: Identifiers | None,
    target_aliases: Optional[Iterable[str]] = None,
) -> List[DownloadResult]:
    """Load cached downloads for the provided identifiers, respecting alias de-duplication."""
    if identifiers is None or not identifiers.identifiers:
        return []

    requested: Set[str] | None = set(target_aliases) if target_aliases else None
    hydrated: Dict[str, DownloadResult] = {}

    for source_name in settings.download_sources:
        index = cache.load_download_index(settings, source_name)
        for identifier in identifiers.identifiers:
            aliases = identifier_aliases(identifier)
            if requested and aliases.isdisjoint(requested):
                continue
            if any(alias in hydrated for alias in aliases):
                continue  # preserve first cached hit by configured source order
            entry = index.get_download_by_identifier(identifier)
            if entry is None:
                continue
            payload = entry.clone_payload()
            payload.identifier = identifier
            for alias in aliases:
                hydrated.setdefault(alias, payload)
            hydrated.setdefault(identifier.slug, payload)

    unique: List[DownloadResult] = []
    seen: Set[int] = set()
    for payload in hydrated.values():
        marker = id(payload)
        if marker in seen:
            continue
        seen.add(marker)
        unique.append(payload)
    return unique


def hydrate_bundles_from_cache(
    settings,
    downloads: List[DownloadResult],
) -> List[ArticleExtractionBundle]:
    """Load cached extraction bundles (with metadata) for the provided downloads."""
    if not downloads:
        return []

    bundles: Dict[str, ArticleExtractionBundle] = {}
    downloads_by_source: Dict[str, List[DownloadResult]] = {}
    for download in downloads:
        downloads_by_source.setdefault(download.source.value, []).append(download)

    def _has_coords(content: ArticleExtractionBundle) -> bool:
        tables = getattr(content, "tables", []) or []
        if any(len(getattr(t, "coordinates", []) or []) > 0 for t in tables):
            return True
        return bool(getattr(content, "has_coordinates", False))

    for source_name in settings.download_sources:
        download_list = downloads_by_source.get(source_name)
        if not download_list:
            continue
        index = cache.load_extractor_index(settings, source_name)
        for download in download_list:
            entry = index.get_extraction_by_identifier(download.identifier)
            if entry is None:
                continue
            content = entry.clone_payload()
            content.identifier = download.identifier
            content.slug = download.identifier.slug
            metadata = cache.get_cached_article_metadata(
                settings,
                slug=content.slug,
                identifier=download.identifier,
            )
            if metadata is None:
                continue
            slug = download.identifier.slug
            existing = bundles.get(slug)
            candidate_bundle = ArticleExtractionBundle(
                article_data=content,
                article_metadata=metadata,
            )
            if existing is None:
                bundles[slug] = candidate_bundle
                continue
            existing_has = _has_coords(existing.article_data)
            candidate_has = _has_coords(candidate_bundle.article_data)
            if existing_has:
                continue  # keep the first bundle that has coordinates
            if candidate_has:
                bundles[slug] = candidate_bundle

    return list(bundles.values())


def hydrate_bundles_for_upload(
    settings,
    identifiers: Identifiers | None,
) -> List[ArticleExtractionBundle]:
    """Hydrate downloads then bundles for upload-only runs."""
    downloads = hydrate_downloads_from_cache(settings, identifiers)
    if not downloads:
        return []
    return hydrate_bundles_from_cache(settings, downloads)
