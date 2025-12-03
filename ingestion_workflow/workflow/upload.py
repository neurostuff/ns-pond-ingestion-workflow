"""Step 4. Upload extracted tables/coordinates to database.
This module will upload the extraction results
(and associated metadata) to the database.
If the base study for an article already exists,
then a new study version will be created,
For ace and pubget articles, a new version will
be created only if results have changed."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Mapping, Optional, Set

from ingestion_workflow.config import Settings, UploadBehavior, UploadMetadataMode
from ingestion_workflow.models import (
    AnalysisCollection,
    ArticleExtractionBundle,
    ArticleMetadata,
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
from ingestion_workflow.workflow.common import (
    expand_target_aliases,
    identifier_aliases,
    resolve_settings,
)
from ingestion_workflow.workflow.hydration import hydrate_bundles_for_upload
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
    manifest_identifiers = _resolve_manifest_identifiers(state, resolved_settings)
    manifest_slugs: Optional[Set[str]] = (
        {identifier.slug for identifier in manifest_identifiers.identifiers}
        if manifest_identifiers and manifest_identifiers.identifiers
        else None
    )
    manifest_aliases: Optional[Set[str]] = (
        expand_target_aliases(manifest_identifiers, manifest_slugs) if manifest_identifiers else None
    )
    alias_map: Dict[str, str] = {}
    if manifest_identifiers:
        for ident in manifest_identifiers.identifiers:
            for alias in identifier_aliases(ident):
                alias_map[alias] = ident.slug

    analyses = _filter_to_manifest(dict(state.analyses or {}), manifest_aliases)
    cached_analyses = load_cached_analysis_collections(resolved_settings)
    # Merge cached analyses for slugs not already present in state
    for slug, per_table in cached_analyses.items():
        if manifest_aliases is not None and slug not in manifest_aliases:
            continue
        if slug not in analyses:
            analyses[slug] = per_table
    if not analyses:
        logger.info("No analyses available after hydration; skipping upload.", extra=console_kwargs())
        return []

    # Best effort: hydrate bundles from cache so we have metadata when running upload alone.
    existing_bundles = {b.article_data.slug: b for b in getattr(state, "bundles", []) or []}
    hydrated_bundles = hydrate_bundles_for_upload(resolved_settings, manifest_identifiers)
    for bundle in hydrated_bundles:
        if manifest_aliases is not None and bundle.article_data.slug not in manifest_aliases:
            continue
        if bundle.article_data.slug not in existing_bundles:
            existing_bundles[bundle.article_data.slug] = bundle
    state.bundles = list(existing_bundles.values())

    behavior = behavior or resolved_settings.upload_behavior
    metadata_only = metadata_only if metadata_only is not None else resolved_settings.upload_metadata_only
    metadata_mode = metadata_mode or resolved_settings.upload_metadata_mode

    metadata_by_slug: Dict[str, ArticleMetadata] = state_to_metadata(state)
    if manifest_aliases is not None:
        metadata_by_slug = {slug: meta for slug, meta in metadata_by_slug.items() if slug in manifest_aliases}
    metadata_by_slug = _hydrate_metadata_from_cache(
        resolved_settings,
        analyses,
        metadata_by_slug,
    )
    if alias_map:
        for alias, canonical in alias_map.items():
            if canonical in metadata_by_slug and alias not in metadata_by_slug:
                metadata_by_slug[alias] = metadata_by_slug[canonical]
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


def _resolve_manifest_identifiers(
    state: "PipelineState",
    settings: Settings,
) -> Optional[Identifiers]:
    if state.identifiers is not None:
        return state.identifiers
    return _load_identifiers_from_manifest(settings)


def _filter_to_manifest(
    analyses: Mapping[str, Mapping[str, AnalysisCollection]],
    manifest_aliases: Optional[Set[str]],
) -> Dict[str, Mapping[str, AnalysisCollection]]:
    if manifest_aliases is None:
        return dict(analyses)
    return {slug: per_table for slug, per_table in analyses.items() if slug in manifest_aliases}
