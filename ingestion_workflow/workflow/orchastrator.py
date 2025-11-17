"""Orchestrator for the ingestion workflow pipeline."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Sequence

from ingestion_workflow.config import Settings, load_settings
from ingestion_workflow.models import (
    AnalysisCollection,
    ArticleExtractionBundle,
    ArticleMetadata,
    DownloadResult,
    Identifier,
    Identifiers,
)
from ingestion_workflow.services import cache
from ingestion_workflow.services.logging import (
    configure_logging,
    console_kwargs,
)
from ingestion_workflow.workflow.stats import StageMetrics

logger = logging.getLogger(__name__)

CANONICAL_STAGES: List[str] = [
    "gather",
    "download",
    "extract",
    "create_analyses",
    "upload",
    "sync",
]


@dataclass
class PipelineState:
    identifiers: Identifiers | None = None
    downloads: List[DownloadResult] | None = None
    bundles: List[ArticleExtractionBundle] | None = None
    analyses: Dict[str, Dict[str, AnalysisCollection]] | None = None
    stage_metrics: Dict[str, StageMetrics] = field(default_factory=dict)


def run_pipeline(
    *,
    settings: Settings | None = None,
) -> PipelineState:
    """Execute the configured pipeline stages in order."""

    resolved_settings = settings or load_settings()
    resolved_settings.ensure_directories()
    _configure_logging_for_run(resolved_settings)
    selected_stages = _normalize_stages(resolved_settings.stages)
    state = PipelineState()
    _seed_identifiers_from_manifest(
        resolved_settings,
        selected_stages,
        state,
    )

    stage_handlers = {
        "gather": _run_gather_stage,
        "download": _run_download_stage,
        "extract": _run_extract_stage,
        "create_analyses": _run_create_analyses_stage,
        "upload": _run_upload_stage,
        "sync": _run_sync_stage,
    }

    for stage in selected_stages:
        handler = stage_handlers[stage]
        logger.info("Starting stage: %s", stage, extra=console_kwargs())
        handler(resolved_settings, state)
        _log_stage_summary(stage, state)
        logger.info("Completed stage: %s", stage, extra=console_kwargs())

    return state


def _run_gather_stage(settings: Settings, state: PipelineState) -> None:
    if settings.dry_run:
        logger.info("Dry-run enabled: gather stage skipped.")
        return
    from ingestion_workflow.workflow.gather import gather_identifiers

    manifest = settings.manifest_path
    identifiers = gather_identifiers(settings=settings, manifest=manifest)
    state.identifiers = identifiers


def _run_download_stage(settings: Settings, state: PipelineState) -> None:
    if settings.dry_run:
        logger.info("Dry-run enabled: download stage skipped.")
        return
    _ensure_identifiers(settings, state)
    from ingestion_workflow.workflow.download import run_downloads

    download_metrics = StageMetrics()
    downloads = run_downloads(
        state.identifiers,
        settings=settings,
        metrics=download_metrics,
    )
    state.downloads = downloads
    state.stage_metrics["download"] = download_metrics


def _run_extract_stage(settings: Settings, state: PipelineState) -> None:
    if settings.dry_run:
        logger.info("Dry-run enabled: extract stage skipped.")
        return
    if not _ensure_downloads(settings, state):
        logger.error("Extraction skipped: downloads unavailable.")
        state.bundles = []
        state.stage_metrics["extract"] = StageMetrics()
        return
    from ingestion_workflow.workflow.extract import run_extraction

    successful_downloads = [download for download in state.downloads or [] if download.success]

    if not successful_downloads:
        logger.info("No successful downloads available for extraction.")
        state.bundles = []
        state.stage_metrics["extract"] = StageMetrics()
        return

    extract_metrics = StageMetrics()
    bundles = run_extraction(
        successful_downloads,
        settings=settings,
        metrics=extract_metrics,
    )
    state.bundles = bundles
    state.stage_metrics["extract"] = extract_metrics


def _run_create_analyses_stage(settings: Settings, state: PipelineState) -> None:
    if settings.dry_run:
        logger.info("Dry-run enabled: create_analyses stage skipped.")
        return
    if not _ensure_bundles(settings, state):
        logger.error("Create_analyses skipped: extraction bundles unavailable.")
        state.analyses = {}
        state.stage_metrics["create_analyses"] = StageMetrics()
        return
    from ingestion_workflow.workflow.create_analyses import run_create_analyses

    analyses_metrics = StageMetrics()
    analyses = run_create_analyses(
        state.bundles,
        settings=settings,
        extractor_name=None,
        metrics=analyses_metrics,
    )
    state.analyses = analyses
    state.stage_metrics["create_analyses"] = analyses_metrics


def _run_upload_stage(settings: Settings, state: PipelineState) -> None:
    logger.info("Upload stage not yet implemented; skipping.")


def _run_sync_stage(settings: Settings, state: PipelineState) -> None:
    logger.info("Sync stage not yet implemented; skipping.")


def _normalize_stages(stages: Sequence[str] | None) -> List[str]:
    requested = [stage.lower() for stage in stages if stage] if stages else list(CANONICAL_STAGES)
    invalid = [stage for stage in requested if stage not in CANONICAL_STAGES]
    if invalid:
        raise ValueError(f"Unknown stages requested: {', '.join(sorted(set(invalid)))}")
    requested_set = set(requested) or set(CANONICAL_STAGES)
    return [stage for stage in CANONICAL_STAGES if stage in requested_set]


def _seed_identifiers_from_manifest(
    settings: Settings,
    stages: Sequence[str],
    state: PipelineState,
) -> None:
    if "gather" in stages:
        return
    if settings.manifest_path is None:
        raise ValueError(
            "Gather stage not selected. Please provide manifest_path in "
            "settings or via --manifest."
        )
    state.identifiers = _load_identifiers_from_manifest(settings)


def _ensure_identifiers(settings: Settings, state: PipelineState) -> None:
    if state.identifiers is not None:
        return
    if settings.manifest_path:
        state.identifiers = _load_identifiers_from_manifest(settings)
        return
    raise ValueError(
        "Identifiers are required for this stage. Run the gather stage or provide a manifest."
    )


def _ensure_downloads(settings: Settings, state: PipelineState) -> bool:
    if state.downloads is not None:
        return True
    if not settings.use_cached_inputs:
        logger.error(
            "Download results are required but missing. "
            "Re-run the download stage or enable cached inputs."
        )
        return False
    try:
        _ensure_identifiers(settings, state)
    except ValueError as exc:
        logger.error("Unable to hydrate downloads from cache: %s", exc)
        return False
    hydrated = _hydrate_downloads_from_cache(settings, state.identifiers)
    if not hydrated:
        logger.error(
            "No cached download results were found for the provided "
            "identifiers. Re-run the download stage."
        )
        return False
    state.downloads = hydrated
    return True


def _ensure_bundles(settings: Settings, state: PipelineState) -> bool:
    if state.bundles is not None:
        return True
    if not settings.use_cached_inputs:
        logger.error(
            "Extraction bundles are required but missing. "
            "Re-run the extract stage or enable cached inputs."
        )
        return False
    if not _ensure_downloads(settings, state):
        return False
    hydrated = _hydrate_bundles_from_cache(
        settings,
        state.downloads or [],
    )
    if not hydrated:
        logger.error("No cached extraction bundles were found. Re-run the extract stage.")
        return False
    state.bundles = hydrated
    return True


def _log_stage_summary(stage: str, state: PipelineState) -> None:
    if stage == "gather":
        count = len(state.identifiers.identifiers) if state.identifiers else 0
        logger.info(
            "Gather stage identifiers: %d",
            count,
            extra=console_kwargs(),
        )
    elif stage == "download":
        metrics = state.stage_metrics.get("download")
        produced = (
            metrics.produced if metrics else (len(state.downloads) if state.downloads else 0)
        )
        cache_hits = metrics.cache_hits if metrics else 0
        logger.info(
            "Download stage results: %d (cache hits: %d)",
            produced,
            cache_hits,
            extra=console_kwargs(),
        )
        if metrics and metrics.cache_hits == 0:
            logger.warning(
                "Download stage loaded zero items from cache.",
                extra=console_kwargs(),
            )
    elif stage == "extract":
        metrics = state.stage_metrics.get("extract")
        produced = metrics.produced if metrics else (len(state.bundles) if state.bundles else 0)
        cache_hits = metrics.cache_hits if metrics else 0
        logger.info(
            "Extract stage bundles: %d (cache hits: %d)",
            produced,
            cache_hits,
            extra=console_kwargs(),
        )
        if metrics and metrics.cache_hits == 0:
            logger.warning(
                "Extract stage loaded zero bundles from cache.",
                extra=console_kwargs(),
            )
    elif stage == "create_analyses":
        metrics = state.stage_metrics.get("create_analyses")
        articles = len(state.analyses) if state.analyses else 0
        collections = (
            sum(len(collections) for collections in state.analyses.values())
            if state.analyses
            else 0
        )
        cache_hits = metrics.cache_hits if metrics else 0
        logger.info(
            "Create analyses stage: %d articles, %d collections (cache hits: %d)",
            articles,
            collections,
            cache_hits,
            extra=console_kwargs(),
        )
        if metrics and metrics.cache_hits == 0:
            logger.warning(
                "Create analyses stage loaded zero tables from cache.",
                extra=console_kwargs(),
            )


def _load_identifiers_from_manifest(settings: Settings) -> Identifiers:
    manifest = settings.manifest_path
    if manifest is None:
        raise ValueError("manifest_path must be provided when skipping gather.")
    manifest_path = Path(manifest)
    if not manifest_path.is_absolute():
        manifest_path = settings.data_root / manifest_path
    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest file not found: {manifest_path}")
    identifiers = Identifiers.load(manifest_path)
    logger.info(
        "Loaded %d identifiers from manifest %s",
        len(identifiers.identifiers),
        manifest_path,
    )
    return identifiers


def _hydrate_downloads_from_cache(
    settings: Settings,
    identifiers: Identifiers | None,
) -> List[DownloadResult]:
    if identifiers is None or not identifiers.identifiers:
        return []
    hydrated: Dict[str, DownloadResult] = {}
    for source_name in settings.download_sources:
        index = cache.load_download_index(settings, source_name)
        for identifier in identifiers.identifiers:
            entry = index.get_download(identifier.slug)
            if entry is None:
                continue
            hydrated[identifier.slug] = entry.result
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

    for source_name, download_list in downloads_by_source.items():
        index = cache.load_extractor_index(settings, source_name)
        for download in download_list:
            entry = index.get_extraction(download.identifier.slug)
            if entry is None:
                continue
            content = entry.content
            metadata = _build_placeholder_metadata(download.identifier)
            bundles[download.identifier.slug] = ArticleExtractionBundle(
                article_data=content,
                article_metadata=metadata,
            )

    return list(bundles.values())


def _build_placeholder_metadata(identifier: Identifier) -> ArticleMetadata:
    for candidate in (identifier.doi, identifier.pmid, identifier.pmcid):
        if candidate:
            return ArticleMetadata(title=str(candidate))
    label = " / ".join(part for part in identifier.slug.split("|") if part)
    if label:
        return ArticleMetadata(title=label)
    return ArticleMetadata(title=identifier.slug or "Unknown Identifier")


def _configure_logging_for_run(settings: Settings) -> None:
    log_path: Path | None = settings.log_file
    if log_path is None:
        log_path = settings.data_root / "logs" / "pipeline.log"
    elif not log_path.is_absolute():
        log_path = settings.data_root / log_path

    configure_logging(
        log_to_file=settings.log_to_file,
        log_file=log_path if settings.log_to_file else None,
        log_to_console=settings.log_to_console,
    )


__all__ = ["PipelineState", "run_pipeline"]
