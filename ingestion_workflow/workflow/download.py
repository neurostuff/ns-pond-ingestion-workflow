"""Download workflow stage with shared caching logic."""

from __future__ import annotations

import logging
from typing import Callable, Dict, Iterable, List, Sequence

from ingestion_workflow.config import Settings
from ingestion_workflow.extractors import (
    ACEExtractor,
    ElsevierExtractor,
    PubgetExtractor,
)
from ingestion_workflow.extractors.base import BaseExtractor
from ingestion_workflow.models import (
    DownloadResult,
    DownloadSource,
    Identifier,
    Identifiers,
)
from ingestion_workflow.services import cache
from ingestion_workflow.services.logging import console_kwargs
from ingestion_workflow.utils.progress import progress_callback
from ingestion_workflow.workflow.common import (
    create_progress_bar,
    log_success,
    resolve_settings,
)
from ingestion_workflow.workflow.stats import StageMetrics


ExtractorFactory = Callable[[Settings], BaseExtractor]
logger = logging.getLogger(__name__)


def _elsevier_factory(settings: Settings) -> BaseExtractor:
    """Construct the Elsevier extractor with the resolved settings."""
    return ElsevierExtractor(settings=settings)


def _pubget_factory(settings: Settings) -> BaseExtractor:
    """Construct the Pubget extractor with the resolved settings."""
    return PubgetExtractor(settings=settings)


def _ace_factory(settings: Settings) -> BaseExtractor:
    """Construct the ACE extractor with the resolved settings."""
    return ACEExtractor(settings=settings)


EXTRACTOR_FACTORIES: Dict[DownloadSource, ExtractorFactory] = {
    DownloadSource.ELSEVIER: _elsevier_factory,
    DownloadSource.PUBGET: _pubget_factory,
    DownloadSource.ACE: _ace_factory,
}


def _resolve_extractor(source: DownloadSource, settings: Settings) -> BaseExtractor:
    """Resolve the configured extractor implementation for a download source."""
    try:
        factory = EXTRACTOR_FACTORIES[source]
    except KeyError as exc:  # pragma: no cover - defensive guard
        message = f"No extractor registered for source: {source}"
        raise ValueError(message) from exc
    return factory(settings)


def _successful_slugs(results: Sequence[DownloadResult]) -> set[str]:
    """Return the slugs for successful download results."""
    return {result.identifier.slug for result in results if result.success}


def _identifiers_from_slugs(
    pending: Iterable[Identifier],
    success_slugs: set[str],
) -> List[Identifier]:
    """Filter pending identifiers to those whose slugs are not in the successes set."""
    return [identifier for identifier in pending if identifier.slug not in success_slugs]


def _partition_supported_identifiers(
    extractor: BaseExtractor, identifiers: Iterable[Identifier]
) -> tuple[list[Identifier], list[Identifier]]:
    """Split identifiers into extractor-supported and unsupported collections."""
    supported_fields = getattr(extractor, "_SUPPORTED_IDS", None)
    identifiers_list = list(identifiers)

    if not supported_fields:
        return identifiers_list, []

    field_set = {str(field) for field in supported_fields}
    supported: list[Identifier] = []
    unsupported: list[Identifier] = []

    for identifier in identifiers_list:
        if any(getattr(identifier, field, None) for field in field_set):
            supported.append(identifier)
        else:
            unsupported.append(identifier)

    return supported, unsupported


def run_downloads(
    identifiers: Identifiers,
    *,
    settings: Settings | None = None,
    metrics: StageMetrics | None = None,
) -> List[DownloadResult]:
    """
    Run configured download sources in order,
    using cache where possible,
    and persist successful payloads.
    """

    resolved_settings = resolve_settings(settings)
    remaining: List[Identifier] = list(identifiers.identifiers)
    collected_results: List[DownloadResult] = []
    total_requested = len(remaining)
    successful_slugs: set[str] = set()
    ignore_cache = "download" in getattr(resolved_settings, "ignore_cache_stages", [])

    for source_name in resolved_settings.download_sources:
        if not remaining:
            break

        source = DownloadSource(source_name)
        extractor = _resolve_extractor(source, resolved_settings)

        supported, unsupported = _partition_supported_identifiers(extractor, remaining)
        if not supported:
            remaining = list(unsupported)
            continue

        extractor_identifiers = Identifiers(list(supported))
        if resolved_settings.force_redownload or ignore_cache:
            cached_results = []
            missing = list(extractor_identifiers.identifiers)
        else:
            cached_results, missing = cache.partition_cached_downloads(
                resolved_settings,
                extractor_name=source.value,
                identifiers=extractor_identifiers,
            )
        collected_results.extend(cached_results)
        successful_slugs.update(
            result.identifier.slug
            for result in cached_results
            if result.success and result.identifier
        )

        cached_count = len(cached_results)
        if metrics is not None and cached_count:
            metrics.record_cache_hits(cached_count)
        unsupported_count = len(unsupported)
        pending_count = len(missing)
        if cached_count:
            logger.info(
                "Download[%s] loaded %d identifiers from cache",
                source.value,
                cached_count,
                extra=console_kwargs(),
            )
        if unsupported_count:
            logger.info(
                "Download[%s] skipping %d unsupported identifiers",
                source.value,
                unsupported_count,
                extra=console_kwargs(),
            )

        next_remaining: List[Identifier] = list(unsupported)

        if not missing:
            remaining = next_remaining
            continue

        if resolved_settings.cache_only_mode:
            if pending_count:
                logger.info(
                    "Download[%s] skipping %d identifiers (cache-only mode)",
                    source.value,
                    pending_count,
                    extra=console_kwargs(),
                )
            next_remaining.extend(missing)
            remaining = next_remaining
            continue

        logger.info(
            "Download[%s] processing %d identifiers",
            source.value,
            pending_count,
            extra=console_kwargs(),
        )

        progress = create_progress_bar(
            resolved_settings,
            pending_count,
            f"Download[{source.value}]",
            unit="article",
        )

        progress_hook = progress_callback(progress)
        try:
            download_results = extractor.download(
                Identifiers(list(missing)),
                progress_hook=progress_hook,
            )
        finally:
            if progress is not None:
                progress.close()
 
        # Transient debug: log summary of download_results returned by the extractor
        try:
            success_count = sum(1 for r in download_results if r.success)
            total_count = len(download_results)
            logger.debug(
                "Extractor.download returned %d results (%d successes) for %d pending identifiers",
                total_count,
                success_count,
                pending_count,
            )
        except Exception:
            logger.exception("Failed to summarize download_results for debug")
 
        for result in download_results:
            collected_results.append(result)

        successes = [result for result in download_results if result.success]
        if successes:
            cache.cache_download_results(
                resolved_settings,
                extractor_name=source.value,
                results=successes,
            )

        successful_slugs.update(
            result.identifier.slug for result in successes if result.identifier
        )

        success_slugs = _successful_slugs(successes)
        failures = _identifiers_from_slugs(missing, success_slugs)
        next_remaining.extend(failures)
        remaining = next_remaining

        logger.info(
            "Download[%s] successes: %d/%d",
            source.value,
            len(successes),
            pending_count,
            extra=console_kwargs(),
        )

    if total_requested:
        log_success("download", len(successful_slugs), total_requested)
    if metrics is not None:
        metrics.record_produced(len(collected_results))

    return collected_results


__all__ = ["run_downloads"]
