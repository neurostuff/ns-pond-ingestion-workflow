"""
Create analyses for extracted tables.

This workflow step coordinates the :class:`CreateAnalysesService` so every
``ArticleExtractionBundle`` yields an ``AnalysisCollection`` per table.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, replace
from typing import Callable, Dict, List, Sequence, Tuple

from ingestion_workflow.config import Settings
from ingestion_workflow.models import (
    AnalysisCollection,
    ArticleExtractionBundle,
    CreateAnalysesResult,
    ExtractedTable,
)
from ingestion_workflow.services import cache
from ingestion_workflow.services.create_analyses import (
    CreateAnalysesService,
    sanitize_table_id,
)
from ingestion_workflow.services.export import ExportService
from ingestion_workflow.services.logging import console_kwargs
from ingestion_workflow.utils.progress import progress_callback
from ingestion_workflow.workflow.common import (
    create_progress_bar,
    log_success,
    maybe_export,
    resolve_settings,
)
from ingestion_workflow.workflow.stats import StageMetrics


logger = logging.getLogger(__name__)


def run_create_analyses(
    bundles: Sequence[ArticleExtractionBundle],
    *,
    settings: Settings | None = None,
    extractor_name: str | None = None,
    metrics: StageMetrics | None = None,
) -> Dict[str, Dict[str, AnalysisCollection]]:
    """
    Run the create-analyses step for a sequence of bundles.

    Returns
    -------
    dict
        Mapping of article slugs to table-id-indexed AnalysisCollections.
    """

    if not bundles:
        return {}

    resolved_settings = resolve_settings(settings)

    bundle_metadata: Dict[str, ArticleExtractionBundle] = {
        bundle.article_data.slug: bundle for bundle in bundles
    }
    cache.cache_article_metadata(
        resolved_settings,
        {slug: bundle.article_metadata for slug, bundle in bundle_metadata.items()},
        identifiers={slug: bundle.article_data.identifier for slug, bundle in bundle_metadata.items()},
        sources_queried=["create_analyses"],
    )

    results: Dict[str, Dict[str, AnalysisCollection]] = {}
    cache_candidates: List[CreateAnalysesResult] = []
    bundle_results: Dict[str, List[CreateAnalysesResult]] = {}
    pending_jobs: List["PendingJob"] = []
    cached_tables = 0
    pending_tables = 0
    skipped_tables = 0
    slug_to_bundle = {bundle.article_data.slug: bundle for bundle in bundles}

    for bundle in bundles:
        article_slug = bundle.article_data.slug
        logger.info("Creating analyses for article %s", article_slug)
        (
            per_table,
            per_bundle_results,
            pending_job,
            stats,
        ) = _run_bundle_with_cache(
            bundle,
            article_slug,
            resolved_settings,
            extractor_name,
        )
        results[article_slug] = per_table
        bundle_results[article_slug] = per_bundle_results
        cached_tables += stats.cached
        pending_tables += stats.pending
        skipped_tables += stats.skipped
        if pending_job is not None:
            pending_jobs.append(pending_job)

    if cached_tables:
        logger.info(
            "create_analyses loaded %d tables from cache",
            cached_tables,
            extra=console_kwargs(),
        )
        if metrics is not None:
            metrics.record_cache_hits(cached_tables)
    if skipped_tables:
        logger.info(
            "create_analyses skipped %d tables without coordinates",
            skipped_tables,
            extra=console_kwargs(),
        )

    if pending_jobs:
        max_workers = max(1, resolved_settings.n_llm_workers)
        logger.info(
            "Dispatching %d bundles to %d LLM workers",
            len(pending_jobs),
            max_workers,
        )
        logger.info(
            "Processing %d tables via LLM",
            pending_tables,
            extra=console_kwargs(),
        )
        progress = create_progress_bar(
            resolved_settings,
            pending_tables,
            "CreateAnalyses",
            unit="table",
        )
        progress_hook = progress_callback(progress)
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_job = {
                executor.submit(
                    _process_pending_job,
                    job,
                    resolved_settings,
                    extractor_name,
                    progress_hook,
                ): job
                for job in pending_jobs
            }
            for future in as_completed(future_to_job):
                job = future_to_job[future]
                produced = future.result()
                cache_candidates.extend(produced)
        if progress is not None:
            progress.close()

    if cache_candidates:
        cache.cache_create_analyses_results(
            resolved_settings,
            extractor_name,
            cache_candidates,
        )

    def _export_payload(
        exporter: ExportService, item: Tuple[str, List[CreateAnalysesResult]]
    ) -> None:
        article_slug, per_bundle = item
        bundle = slug_to_bundle.get(article_slug)
        if bundle is None:
            return
        exporter.export(bundle, per_bundle)

    maybe_export(
        resolved_settings.export,
        lambda: ExportService(
            resolved_settings,
            overwrite=resolved_settings.export_overwrite,
        ),
        bundle_results.items(),
        export_fn=_export_payload,
    )

    total_success_tables = sum(len(entries) for entries in bundle_results.values())
    total_target_tables = cached_tables + pending_tables
    if total_target_tables:
        log_success("create_analyses", total_success_tables, total_target_tables)
    if metrics is not None:
        metrics.record_produced(total_success_tables)
        if skipped_tables:
            metrics.record_skipped(skipped_tables)

    return results


def _run_bundle_with_cache(
    bundle: ArticleExtractionBundle,
    article_slug: str,
    settings: Settings,
    extractor_name: str | None,
) -> Tuple[
    Dict[str, AnalysisCollection],
    List[CreateAnalysesResult],
    "PendingJob | None",
    "BundleCacheStats",
]:
    table_results: Dict[str, AnalysisCollection] = {}
    pending_tables: List[ExtractedTable] = []
    pending_info: Dict[str, Dict[str, object]] = {}
    bundle_results: List[CreateAnalysesResult] = []
    stats = BundleCacheStats()

    for index, table in enumerate(bundle.article_data.tables):
        if not table.contains_coordinates and not table.coordinates:
            stats.skipped += 1
            continue

        sanitized_table_id = sanitize_table_id(table.table_id, index)
        table_key = table.table_id or sanitized_table_id
        cache_key = _compose_cache_key(article_slug, sanitized_table_id)
        cached = None
        ignore_cache = "create_analyses" in getattr(settings, "ignore_cache_stages", [])
        if not ignore_cache:
            cached = cache.get_cached_create_analyses_result(
                settings,
                cache_key,
                extractor_name,
                identifier=bundle.article_data.identifier,
                sanitized_table_id=sanitized_table_id,
            )
            if cached:
                table_results[table_key] = cached.analysis_collection
                bundle_results.append(cached)
                stats.cached += 1
                continue
        pending_tables.append(table)
        stats.pending += 1
        pending_info[table_key] = {
            "sanitized": sanitized_table_id,
            "cache_key": cache_key,
            "table_number": table.table_number,
            "table_metadata": dict(table.metadata),
        }

    if not pending_tables:
        return table_results, bundle_results, None, stats

    pruned_content = replace(bundle.article_data, tables=list(pending_tables))
    pruned_bundle = ArticleExtractionBundle(
        article_data=pruned_content,
        article_metadata=bundle.article_metadata,
    )

    pending_job = PendingJob(
        article_slug=article_slug,
        bundle=pruned_bundle,
        pending_info=pending_info,
        table_results=table_results,
        bundle_records=bundle_results,
    )
    return table_results, bundle_results, pending_job, stats


def _compose_cache_key(article_slug: str, sanitized_table_id: str) -> str:
    return f"{article_slug}::{sanitized_table_id}"


@dataclass
class PendingJob:
    article_slug: str
    bundle: ArticleExtractionBundle
    pending_info: Dict[str, Dict[str, object]]
    table_results: Dict[str, AnalysisCollection]
    bundle_records: List[CreateAnalysesResult]


@dataclass
class BundleCacheStats:
    cached: int = 0
    pending: int = 0
    skipped: int = 0


def _process_pending_job(
    job: PendingJob,
    settings: Settings,
    extractor_name: str | None,
    progress_hook: Callable[[int], None] | None,
) -> List[CreateAnalysesResult]:
    service = CreateAnalysesService(
        settings,
        extractor_name=extractor_name,
    )
    new_results = service.run(job.bundle, progress_hook=progress_hook)
    produced: List[CreateAnalysesResult] = []
    for table_key, collection in new_results.items():
        info = job.pending_info.get(table_key)
        if info is None:
            continue
        job.table_results[table_key] = collection
        result_obj = CreateAnalysesResult(
            slug=info["cache_key"],
            article_slug=job.article_slug,
            table_id=table_key,
            sanitized_table_id=info["sanitized"],
            analysis_collection=collection,
            metadata={
                "table_metadata": info["table_metadata"],
                "table_number": info["table_number"],
            },
        )
        job.bundle_records.append(result_obj)
        produced.append(result_obj)
    return produced


__all__ = [
    "run_create_analyses",
]
