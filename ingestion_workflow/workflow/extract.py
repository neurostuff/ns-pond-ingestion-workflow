"""Extraction workflow orchestration.

This module coordinates the extraction stage of the ingestion workflow.
It currently performs basic validation on download outputs and dispatches
successful downloads to the appropriate extractor implementation. Caching,
metadata enrichment, and support for additional extractors will be layered in
subsequent iterations.
"""

from __future__ import annotations

from typing import Dict, List, Sequence, Tuple

from ingestion_workflow.config import Settings, load_settings
from ingestion_workflow.extractors.base import BaseExtractor
from ingestion_workflow.models import (
    DownloadResult,
    DownloadSource,
    ExtractionResult,
    FileType,
)
from ingestion_workflow.services import cache
from ingestion_workflow.workflow.download import _resolve_extractor


def _ensure_successful_download(download_result: DownloadResult) -> None:
    """Validate that a download result is suitable for extraction."""

    if not download_result.success:
        raise ValueError(
            "Extraction workflow received an unsuccessful download result; "
            "ensure downloads are filtered before extraction."
        )

    if not download_result.files:
        raise ValueError(
            "Successful downloads must include persisted files for "
            "extraction."
        )

    missing_paths = [
        downloaded.file_path
        for downloaded in download_result.files
        if not downloaded.file_path.exists()
    ]
    if missing_paths:
        missing_str = ", ".join(str(path) for path in missing_paths)
        raise ValueError(
            "Extraction workflow found download payloads missing on disk: "
            f"{missing_str}"
        )

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
            raise ValueError(
                "ACE downloads must include an HTML file for extraction."
            )


def _group_by_source(
    download_results: Sequence[DownloadResult],
) -> Dict[DownloadSource, List[Tuple[int, DownloadResult]]]:
    grouped: Dict[DownloadSource, List[Tuple[int, DownloadResult]]] = {}
    for index, download_result in enumerate(download_results):
        _ensure_successful_download(download_result)
        grouped.setdefault(download_result.source, []).append(
            (index, download_result)
        )
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
) -> List[ExtractionResult]:
    """Execute extraction for previously downloaded articles."""

    if not download_results:
        return []

    resolved_settings = settings or load_settings()

    grouped = _group_by_source(download_results)
    ordered_results: List[ExtractionResult | None] = [
        None
    ] * len(download_results)

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

        pending_entries: List[Tuple[int, DownloadResult]] = []
        missing_index = 0

        for (index, _), cached_result in zip(
            entries, cached_slots
        ):
            if cached_result is not None:
                ordered_results[index] = cached_result
                continue
            pending_result = missing_results[missing_index]
            pending_entries.append((index, pending_result))
            missing_index += 1

        if missing_index != len(missing_results):
            raise ValueError(
                "Extraction cache bookkeeping mismatch detected."
            )

        if not pending_entries:
            continue

        pending_subset = [
            download_result
            for _, download_result in pending_entries
        ]

        try:
            extracted = extractor.extract(pending_subset)
        except NotImplementedError as exc:  # pragma: no cover - dev feedback
            raise NotImplementedError(
                f"Extractor for source {source.value} does not yet implement "
                "extraction."
            ) from exc

        if len(extracted) != len(pending_subset):
            raise ValueError(
                "Extractor returned a result set with mismatched length."
            )

        for (index, _), extraction_result in zip(pending_entries, extracted):
            ordered_results[index] = extraction_result

        cache.cache_extraction_results(
            resolved_settings,
            source.value,
            extracted,
        )

    final_results: List[ExtractionResult] = []
    for index, candidate in enumerate(ordered_results):
        if candidate is None:
            raise ValueError(
                f"Extraction result missing for download index {index}."
            )
        final_results.append(candidate)

    return final_results


__all__ = [
    "run_extraction",
]
