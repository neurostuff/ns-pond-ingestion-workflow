"""Extraction workflow orchestration.

This module coordinates the extraction stage of the ingestion workflow.
It currently performs basic validation on download outputs and dispatches
successful downloads to the appropriate extractor implementation. Caching,
metadata enrichment, and support for additional extractors will be layered in
subsequent iterations.
"""

from __future__ import annotations

import logging
from typing import Dict, List, Sequence, Tuple

from ingestion_workflow.config import Settings
from ingestion_workflow.extractors.base import BaseExtractor
from ingestion_workflow.models import (
    ArticleExtractionBundle,
    DownloadResult,
    DownloadSource,
    ExtractionResult,
    FileType,
)
from ingestion_workflow.models.metadata import ArticleMetadata
from ingestion_workflow.services import cache
from ingestion_workflow.services.export import ExportService
from ingestion_workflow.services.metadata import MetadataService
from ingestion_workflow.services.logging import console_kwargs
from ingestion_workflow.utils.progress import progress_callback
from ingestion_workflow.workflow.common import (
    create_progress_bar,
    ensure_successful_download as ensure_base_download,
    log_cache_hits,
    log_success,
    maybe_export,
    resolve_settings,
)
from ingestion_workflow.workflow.download import _resolve_extractor
from ingestion_workflow.workflow.stats import StageMetrics


logger = logging.getLogger(__name__)


def _ensure_successful_download(download_result: DownloadResult) -> None:
    """Validate that a download result is suitable for extraction."""

    ensure_base_download(download_result)

    if download_result.source is DownloadSource.ACE:
        html_file = next(
            (
                downloaded
                for downloaded in download_result.files
                if downloaded.file_type is FileType.HTML
            ),
            None,
        )
        if html_file is None:
            raise ValueError("ACE downloads must include an HTML file for extraction.")

    if download_result.source is DownloadSource.PUBGET:
        article_xml = next(
            (
                downloaded
                for downloaded in download_result.files
                if downloaded.file_type is FileType.XML
                and downloaded.file_path.name == "article.xml"
            ),
            None,
        )
        tables_xml = next(
            (
                downloaded
                for downloaded in download_result.files
                if downloaded.file_type is FileType.XML
                and downloaded.file_path.name == "tables.xml"
            ),
            None,
        )
        if article_xml is None or tables_xml is None:
            raise ValueError(
                "Pubget downloads must include article.xml and tables/tables.xml for extraction."
            )

    if download_result.source is DownloadSource.ELSEVIER:
        xml_file = next(
            (
                downloaded
                for downloaded in download_result.files
                if downloaded.file_type is FileType.XML
                and downloaded.file_path.name.startswith("content.")
            ),
            None,
        )
        metadata_file = next(
            (
                downloaded
                for downloaded in download_result.files
                if downloaded.file_type is FileType.JSON
                and downloaded.file_path.name == "metadata.json"
            ),
            None,
        )
        if xml_file is None:
            raise ValueError(
                "Elsevier downloads must include XML content for extraction; "
                "PDF-only articles are not supported."
            )
        if metadata_file is None:
            raise ValueError("Elsevier downloads must include metadata.json for extraction.")


def _group_by_source(
    download_results: Sequence[DownloadResult],
) -> Dict[DownloadSource, List[Tuple[int, DownloadResult]]]:
    grouped: Dict[DownloadSource, List[Tuple[int, DownloadResult]]] = {}
    for index, download_result in enumerate(download_results):
        _ensure_successful_download(download_result)
        grouped.setdefault(download_result.source, []).append((index, download_result))
    return grouped


def _resolve_extractor_for_source(
    source: DownloadSource,
    settings: Settings,
) -> BaseExtractor:
    extractor = _resolve_extractor(source, settings)
    if extractor is None:
        raise ValueError(f"No extractor available for source {source}.")
    return extractor


def run_extraction(
    download_results: Sequence[DownloadResult],
    *,
    settings: Settings | None = None,
    metrics: StageMetrics | None = None,
) -> List[ArticleExtractionBundle]:
    """
    Execute extraction for previously downloaded articles.

    Returns
    -------
    list
        Ordered list of ArticleExtractionBundle instances pairing each
        extraction result with its metadata.
    """

    if not download_results:
        return []

    resolved_settings = resolve_settings(settings)

    grouped = _group_by_source(download_results)
    ordered_results: List[ExtractionResult | None] = [None] * len(download_results)

    processed_count = 0
    for source, entries in grouped.items():
        extractor = _resolve_extractor_for_source(source, resolved_settings)
        subset = [download_result for _, download_result in entries]

        if resolved_settings.force_reextract:
            cached_slots = [None] * len(subset)
            missing_results = list(subset)
        else:
            cached_slots, missing_results = cache.partition_cached_extractions(
                resolved_settings,
                source.value,
                subset,
            )

        cached_count = sum(1 for item in cached_slots if item is not None)
        missing_total = len(missing_results)
        processed_count += cached_count
        if cached_count:
            log_cache_hits(f"Extract[{source.value}]", cached_count)
            if metrics is not None:
                metrics.record_cache_hits(cached_count)
        pending_entries: List[Tuple[int, DownloadResult]] = []
        missing_index = 0

        for (index, _), cached_result in zip(entries, cached_slots):
            if cached_result is not None:
                ordered_results[index] = cached_result
                continue
            pending_result = missing_results[missing_index]
            pending_entries.append((index, pending_result))
            missing_index += 1

        if missing_index != len(missing_results):
            raise ValueError("Extraction cache bookkeeping mismatch detected.")

        if not pending_entries:
            logger.info(
                "Extract[%s] successes: %d/%d (cache)",
                source.value,
                cached_count,
                len(subset),
                extra=console_kwargs(),
            )
            continue

        logger.info(
            "Extract[%s] processing %d downloads",
            source.value,
            missing_total,
            extra=console_kwargs(),
        )

        pending_subset = [download_result for _, download_result in pending_entries]

        progress = create_progress_bar(
            resolved_settings,
            len(pending_subset),
            f"Extract[{source.value}]",
            unit="article",
        )
        progress_hook = progress_callback(progress)

        try:
            extracted = extractor.extract(
                pending_subset,
                progress_hook=progress_hook,
            )
        except NotImplementedError as exc:  # pragma: no cover - dev feedback
            raise NotImplementedError(
                f"Extractor for source {source.value} does not yet implement extraction."
            ) from exc
        finally:
            if progress is not None:
                progress.close()

        if len(extracted) != len(pending_subset):
            raise ValueError("Extractor returned a result set with mismatched length.")

        for (index, _), extraction_result in zip(pending_entries, extracted):
            ordered_results[index] = extraction_result
            processed_count += 1

        cache.cache_extraction_results(
            resolved_settings,
            source.value,
            extracted,
        )

        logger.info(
            "Extract[%s] successes: %d/%d",
            source.value,
            len(extracted),
            missing_total,
            extra=console_kwargs(),
        )

    final_results: List[ExtractionResult] = []
    for index, candidate in enumerate(ordered_results):
        if candidate is None:
            raise ValueError(f"Extraction result missing for download index {index}.")
        final_results.append(candidate)

    # Enrich metadata for articles with coordinates
    metadata_dict: Dict[str, ArticleMetadata] = {}
    eligible_for_metadata = [result for result in final_results if result.identifier]

    if eligible_for_metadata:
        logger.info(
            "Enriching metadata for %d articles",
            len(eligible_for_metadata),
        )
        try:
            metadata_service = MetadataService(resolved_settings)
            metadata_dict = metadata_service.enrich_metadata(eligible_for_metadata)
            logger.info(
                "Successfully enriched metadata for %d articles",
                len(metadata_dict),
            )
        except Exception as exc:
            logger.error(
                "Metadata enrichment failed: %s. Continuing without metadata.",
                exc,
            )

    bundles: List[ArticleExtractionBundle] = []
    for result in final_results:
        metadata = metadata_dict.get(result.slug)
        if metadata is None:
            metadata = _build_placeholder_metadata(result)
        bundles.append(
            ArticleExtractionBundle(
                article_data=result,
                article_metadata=metadata,
            )
        )

    maybe_export(
        resolved_settings.export,
        lambda: ExportService(
            resolved_settings,
            overwrite=resolved_settings.export_overwrite,
        ),
        bundles,
        export_fn=lambda exporter, bundle: exporter.export(bundle),
    )

    if download_results:
        log_success("extract", processed_count, len(download_results))
    if metrics is not None:
        metrics.record_produced(len(bundles))

    return bundles


def _build_placeholder_metadata(result: ExtractionResult) -> ArticleMetadata:
    identifier = result.identifier
    if identifier:
        for candidate in (
            identifier.doi,
            identifier.pmid,
            identifier.pmcid,
        ):
            if candidate:
                return ArticleMetadata(title=str(candidate))
        identifier_label = " / ".join(part for part in identifier.slug.split("|") if part)
        if identifier_label:
            return ArticleMetadata(title=identifier_label)
    return ArticleMetadata(title=result.slug)


__all__ = [
    "run_extraction",
]
