"""Step 4. Upload extracted tables/coordinates to database.
This module will upload the extraction results
(and associated metadata) to the database.
If the base study for an article already exists,
then a new study version will be created,
For ace and pubget articles, a new version will
be created only if results have changed."""

from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List

from ingestion_workflow.config import Settings, UploadBehavior, UploadMetadataMode
from ingestion_workflow.models import ArticleMetadata, UploadOutcome
from ingestion_workflow.models.cache import UploadCacheEntry
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
    analyses = state.analyses or {}
    if not analyses:
        logger.info(
            "No analyses found in state; attempting to hydrate from cache.",
            extra=console_kwargs(),
        )
        analyses = load_cached_analysis_collections(resolved_settings)
    if not analyses:
        logger.info("No analyses available after hydration; skipping upload.", extra=console_kwargs())
        return []

    behavior = behavior or resolved_settings.upload_behavior
    metadata_only = metadata_only if metadata_only is not None else resolved_settings.upload_metadata_only
    metadata_mode = metadata_mode or resolved_settings.upload_metadata_mode

    metadata_by_slug: Dict[str, ArticleMetadata] = state_to_metadata(state)
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
