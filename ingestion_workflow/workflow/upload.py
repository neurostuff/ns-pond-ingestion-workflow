"""Step 4. Upload extracted tables/coordinates to database.
This module will upload the extraction results
(and associated metadata) to the database.
If the base study for an article already exists,
then a new study version will be created,
For ace and pubget articles, a new version will
be created only if results have changed."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Mapping

from ingestion_workflow.config import Settings, UploadBehavior, UploadMetadataMode
from ingestion_workflow.models import (
    ArticleExtractionBundle,
    ArticleMetadata,
    DownloadResult,
    Identifier,
    Identifiers,
    UploadOutcome,
)
from ingestion_workflow.models.cache import UploadCacheEntry
from ingestion_workflow.services import cache
from ingestion_workflow.services.cache import cache_upload_results, load_cached_analysis_collections
from ingestion_workflow.services.db import SSHTunnel, SessionFactory
from ingestion_workflow.services.logging import console_kwargs, get_logger
from ingestion_workflow.services.upload import UploadService
from ingestion_workflow.workflow.common import resolve_settings
if TYPE_CHECKING:  # pragma: no cover - for type hints only
    from ingestion_workflow.workflow.orchastrator import PipelineState

logger = get_logger(__name__)


def run_upload(
    state: "PipelineState",
    *,
    settings: Settings | None = None,
    behavior: UploadBehavior | None = None,
    metadata_only: bool | None = None,
    metadata_mode: UploadMetadataMode | None = None,
) -> List[UploadOutcome]:
    """Run the upload stage using cached analyses and metadata."""
    resolved_settings = resolve_settings(settings)
    analyses = dict(state.analyses or {})
    cached_analyses = load_cached_analysis_collections(resolved_settings)
    # Merge cached analyses for slugs not already present in state
    for slug, per_table in cached_analyses.items():
        if slug not in analyses:
            analyses[slug] = per_table
    if not analyses:
        logger.info("No analyses available after hydration; skipping upload.", extra=console_kwargs())
        return []

    # Best effort: hydrate bundles from cache so we have metadata when running upload alone.
    existing_bundles = {b.article_data.slug: b for b in getattr(state, "bundles", []) or []}
    hydrated_bundles = _hydrate_bundles_for_upload(resolved_settings)
    for bundle in hydrated_bundles:
        if bundle.article_data.slug not in existing_bundles:
            existing_bundles[bundle.article_data.slug] = bundle
    state.bundles = list(existing_bundles.values())

    behavior = behavior or resolved_settings.upload_behavior
    metadata_only = metadata_only if metadata_only is not None else resolved_settings.upload_metadata_only
    metadata_mode = metadata_mode or resolved_settings.upload_metadata_mode

    metadata_by_slug: Dict[str, ArticleMetadata] = state_to_metadata(state)
    metadata_by_slug = _hydrate_metadata_from_cache(
        resolved_settings,
        analyses,
        metadata_by_slug,
    )
    outcomes: List[UploadOutcome] = []

    with SSHTunnel(resolved_settings) as tunnel:
        session_factory = SessionFactory(resolved_settings, tunnel=tunnel)
        service = UploadService(resolved_settings, session_factory)
        work_items = service.prepare_work_items(
            analyses,
            metadata_by_slug,
            metadata_mode=metadata_mode,
        )
        outcomes = service.run(
            work_items,
            behavior=behavior,
            metadata_only=metadata_only,
            metadata_mode=metadata_mode,
        )

    cache_entries = [UploadCacheEntry.from_outcome(outcome) for outcome in outcomes]
    cache_upload_results(resolved_settings, cache_entries)

    return outcomes


def state_to_metadata(state: PipelineState) -> Dict[str, ArticleMetadata]:
    """Extract metadata by slug from pipeline state bundles."""
    bundles = getattr(state, "bundles", None) or []
    mapping: Dict[str, ArticleMetadata] = {}
    for bundle in bundles:
        mapping[bundle.article_data.slug] = bundle.article_metadata
    return mapping


# ------- cache hydration helpers for upload-only runs ---------------------


def _hydrate_bundles_for_upload(settings: Settings) -> List[ArticleExtractionBundle]:
    identifiers = _load_identifiers_from_manifest(settings)
    if identifiers is None or not identifiers.identifiers:
        return []
    downloads = _hydrate_downloads_from_cache(settings, identifiers)
    if not downloads:
        return []
    return _hydrate_bundles_from_cache(settings, downloads)


def _load_identifiers_from_manifest(settings: Settings) -> Identifiers | None:
    manifest = settings.manifest_path
    if manifest is None:
        return None
    manifest_path = Path(manifest)
    if not manifest_path.is_absolute():
        manifest_path = settings.data_root / manifest_path
    try:
        return Identifiers.load(manifest_path)
    except Exception as exc:  # pragma: no cover - defensive for upload-only
        logger.warning("Failed to load manifest for upload hydration: %s", exc, extra=console_kwargs())
        return None


def _hydrate_downloads_from_cache(
    settings: Settings,
    identifiers: Identifiers,
) -> List[DownloadResult]:
    if identifiers is None or not identifiers.identifiers:
        return []
    hydrated: Dict[str, DownloadResult] = {}
    for source_name in settings.download_sources:
        index = cache.load_download_index(settings, source_name)
        for identifier in identifiers.identifiers:
            if identifier.slug in hydrated:
                continue  # preserve first cached hit by configured source order
            entry = index.get_download_by_identifier(identifier)
            if entry is None:
                continue
            payload = entry.clone_payload()
            payload.identifier = identifier
            hydrated[identifier.slug] = payload
    return list(hydrated.values())


def _hydrate_bundles_from_cache(
    settings: Settings,
    downloads: List[DownloadResult],
) -> List[ArticleExtractionBundle]:
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
                continue  # keep first bundle that has coordinates
            if candidate_has:
                bundles[slug] = candidate_bundle

    return list(bundles.values())


def _hydrate_metadata_from_cache(
    settings: Settings,
    analyses: Mapping[str, Mapping[str, AnalysisCollection]],
    metadata_by_slug: Dict[str, ArticleMetadata],
) -> Dict[str, ArticleMetadata]:
    if not analyses:
        return metadata_by_slug

    for slug in analyses.keys():
        cached = cache.get_cached_article_metadata(
            settings,
            slug=slug,
            identifier=_resolve_identifier_for_slug(analyses, slug),
        )
        if cached is None:
            continue
        existing = metadata_by_slug.get(slug)
        if existing is None or not cache.has_substantive_metadata(existing):
            metadata_by_slug[slug] = cached
        else:
            metadata_by_slug[slug] = existing.merge_from(cached)

    return metadata_by_slug


def _resolve_identifier_for_slug(
    analyses: Mapping[str, Mapping[str, AnalysisCollection]],
    slug: str,
) -> Identifier | None:
    per_table = analyses.get(slug) or {}
    for collection in per_table.values():
        if collection.identifier is not None:
            return collection.identifier
    return None
