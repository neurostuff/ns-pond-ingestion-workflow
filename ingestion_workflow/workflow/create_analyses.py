"""
Create analyses for extracted tables.

This workflow step coordinates the :class:`CreateAnalysesService` so every
``ArticleExtractionBundle`` yields an ``AnalysisCollection`` per table.
"""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, replace
from typing import Dict, List, Sequence, Tuple

from ingestion_workflow.config import Settings, load_settings
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


logger = logging.getLogger(__name__)


def run_create_analyses(
    bundles: Sequence[ArticleExtractionBundle],
    *,
    settings: Settings | None = None,
    extractor_name: str | None = None,
) -> Dict[str, Dict[str, AnalysisCollection]]:
    """
    Run the create-analyses step for a sequence of bundles.

    Returns
    -------
    dict
        Mapping of article hash IDs to table-id-indexed AnalysisCollections.
    """

    if not bundles:
        return {}

    resolved_settings = settings or load_settings()

    results: Dict[str, Dict[str, AnalysisCollection]] = {}
    cache_candidates: List[CreateAnalysesResult] = []
    bundle_results: Dict[str, List[CreateAnalysesResult]] = {}
    pending_jobs: List["PendingJob"] = []
    hash_to_bundle = {bundle.article_data.hash_id: bundle for bundle in bundles}

    for bundle in bundles:
        article_hash = bundle.article_data.hash_id
        logger.info("Creating analyses for article %s", article_hash)
        per_table, per_bundle_results, pending_job = _run_bundle_with_cache(
            bundle,
            article_hash,
            resolved_settings,
            extractor_name,
        )
        results[article_hash] = per_table
        bundle_results[article_hash] = per_bundle_results
        if pending_job is not None:
            pending_jobs.append(pending_job)

    if pending_jobs:
        max_workers = max(1, resolved_settings.n_llm_workers)
        logger.info(
            "Dispatching %d bundles to %d LLM workers",
            len(pending_jobs),
            max_workers,
        )
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_job = {
                executor.submit(
                    _process_pending_job,
                    job,
                    resolved_settings,
                    extractor_name,
                ): job
                for job in pending_jobs
            }
            for future in as_completed(future_to_job):
                produced = future.result()
                cache_candidates.extend(produced)

    if cache_candidates:
        cache.cache_create_analyses_results(
            resolved_settings,
            extractor_name,
            cache_candidates,
        )

    if resolved_settings.export:
        exporter = ExportService(
            resolved_settings,
            overwrite=resolved_settings.export_overwrite,
        )
        for article_hash, per_bundle in bundle_results.items():
            bundle = hash_to_bundle.get(article_hash)
            if bundle is None:
                continue
            exporter.export(bundle, per_bundle)

    return results


def _run_bundle_with_cache(
    bundle: ArticleExtractionBundle,
    article_hash: str,
    settings: Settings,
    extractor_name: str | None,
) -> Tuple[
    Dict[str, AnalysisCollection],
    List[CreateAnalysesResult],
    "PendingJob | None",
]:
    table_results: Dict[str, AnalysisCollection] = {}
    pending_tables: List[ExtractedTable] = []
    pending_info: Dict[str, Dict[str, object]] = {}
    bundle_results: List[CreateAnalysesResult] = []

    for index, table in enumerate(bundle.article_data.tables):
        sanitized_table_id = sanitize_table_id(table.table_id, index)
        table_key = table.table_id or sanitized_table_id
        cache_key = _compose_cache_key(article_hash, sanitized_table_id)
        cached = cache.get_cached_create_analyses_result(
            settings,
            cache_key,
            extractor_name,
        )
        if cached:
            table_results[table_key] = cached.analysis_collection
            bundle_results.append(cached)
            continue
        pending_tables.append(table)
        pending_info[table_key] = {
            "sanitized": sanitized_table_id,
            "cache_key": cache_key,
            "table_number": table.table_number,
            "table_metadata": dict(table.metadata),
        }

    if not pending_tables:
        return table_results, bundle_results, None

    pruned_content = replace(bundle.article_data, tables=list(pending_tables))
    pruned_bundle = ArticleExtractionBundle(
        article_data=pruned_content,
        article_metadata=bundle.article_metadata,
    )

    pending_job = PendingJob(
        article_hash=article_hash,
        bundle=pruned_bundle,
        pending_info=pending_info,
        table_results=table_results,
        bundle_records=bundle_results,
    )
    return table_results, bundle_results, pending_job


def _compose_cache_key(article_hash: str, sanitized_table_id: str) -> str:
    return f"{article_hash}::{sanitized_table_id}"


@dataclass
class PendingJob:
    article_hash: str
    bundle: ArticleExtractionBundle
    pending_info: Dict[str, Dict[str, object]]
    table_results: Dict[str, AnalysisCollection]
    bundle_records: List[CreateAnalysesResult]


def _process_pending_job(
    job: PendingJob,
    settings: Settings,
    extractor_name: str | None,
) -> List[CreateAnalysesResult]:
    service = CreateAnalysesService(
        settings,
        extractor_name=extractor_name,
    )
    new_results = service.run(job.bundle)
    produced: List[CreateAnalysesResult] = []
    for table_key, collection in new_results.items():
        info = job.pending_info.get(table_key)
        if info is None:
            continue
        job.table_results[table_key] = collection
        result_obj = CreateAnalysesResult(
            hash_id=info["cache_key"],
            article_hash=job.article_hash,
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
