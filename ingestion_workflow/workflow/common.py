"""Shared helpers for workflow orchestration stages."""

from __future__ import annotations

import logging
from concurrent.futures import Executor, ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Callable, Iterable, List, Sequence, Tuple, TypeVar

from tqdm.auto import tqdm

from ingestion_workflow.config import Settings, load_settings
from ingestion_workflow.models import DownloadResult, ExtractionResult
from ingestion_workflow.services.logging import console_kwargs
from ingestion_workflow.utils.progress import emit_progress, progress_callback

logger = logging.getLogger(__name__)

TItem = TypeVar("TItem")
TResult = TypeVar("TResult")


def resolve_settings(settings: Settings | None, *, ensure_dirs: bool = True) -> Settings:
    """Load settings when absent and optionally ensure directories are created."""
    resolved = settings or load_settings()
    if ensure_dirs:
        resolved.ensure_directories()
    return resolved


def create_progress_bar(
    settings: Settings,
    total: int,
    desc: str,
    *,
    unit: str = "item",
) -> tqdm | None:
    """Create a tqdm progress bar if console display is enabled."""
    if not settings.show_progress or total <= 0:
        return None
    return tqdm(
        total=total,
        desc=desc,
        leave=False,
        unit=unit,
    )


def maybe_export(
    enabled: bool,
    exporter_factory: Callable[[], TItem],
    payloads: Iterable[TResult],
    *,
    export_fn: Callable[[TItem, TResult], None],
) -> None:
    """Export payloads when flagged without duplicating conditional boilerplate."""
    if not enabled:
        return
    exporter = exporter_factory()
    for payload in payloads:
        export_fn(exporter, payload)


def run_with_executor(
    items: Sequence[TItem],
    *,
    worker: Callable[[TItem], TResult],
    executor_factory: Callable[[int], Executor],
    max_workers: int,
    progress_desc: str,
    settings: Settings,
    unit: str = "item",
) -> List[TResult]:
    """Execute work with optional concurrency and tqdm tracking."""
    if not items:
        return []

    progress = create_progress_bar(settings, len(items), progress_desc, unit=unit)
    progress_hook = progress_callback(progress)

    def _run_item(item: TItem) -> TResult:
        result = worker(item)
        emit_progress(progress_hook)
        return result

    results: List[TResult] = []
    if max_workers <= 1 or len(items) == 1:
        for item in items:
            results.append(_run_item(item))
    else:
        with executor_factory(max_workers) as executor:
            future_map = {executor.submit(worker, item): idx for idx, item in enumerate(items)}
            for future in as_completed(future_map):
                result = future.result()
                results.append(result)
                emit_progress(progress_hook)

    if progress is not None:
        progress.close()

    return results


def run_process_pool(
    items: Sequence[TItem],
    *,
    worker: Callable[[TItem], TResult],
    settings: Settings,
    desc: str,
    max_workers: int,
    unit: str = "item",
) -> List[TResult]:
    """Convenience wrapper for process-based concurrency."""
    return run_with_executor(
        items,
        worker=worker,
        executor_factory=lambda max_w: ProcessPoolExecutor(max_workers=max_w),
        max_workers=max_workers,
        progress_desc=desc,
        settings=settings,
        unit=unit,
    )


def run_thread_pool(
    items: Sequence[TItem],
    *,
    worker: Callable[[TItem], TResult],
    settings: Settings,
    desc: str,
    max_workers: int,
    unit: str = "item",
) -> List[TResult]:
    """Convenience wrapper for thread-based concurrency."""
    return run_with_executor(
        items,
        worker=worker,
        executor_factory=lambda max_w: ThreadPoolExecutor(max_workers=max_w),
        max_workers=max_workers,
        progress_desc=desc,
        settings=settings,
        unit=unit,
    )


def reorder_results(
    total: int,
    indexed_results: Iterable[Tuple[int, TResult]],
) -> List[TResult]:
    """Place results back into their original order by index."""
    ordered: List[TResult | None] = [None] * total
    for index, result in indexed_results:
        ordered[index] = result
    return [result for result in ordered if result is not None]


def ensure_successful_download(download_result: DownloadResult) -> bool:
    """Shared validation for download results before extraction.

    Returns
    -------
    bool
        True when the download is usable; False when missing/invalid with an error logged.
    """
    ident = getattr(download_result.identifier, "slug", None) or "unknown"
    source = getattr(download_result, "source", None)
    source_label = source.value if hasattr(source, "value") else str(source)
    context = f"[download:{source_label} id={ident}]"

    if not download_result.success:
        logger.error("%s marked unsuccessful%s", context, _format_error_suffix(download_result))
        return False

    if not download_result.files:
        logger.error("%s missing persisted files for processing", context)
        return False

    missing = [
        downloaded.file_path
        for downloaded in download_result.files
        if not downloaded.file_path.exists()
    ]
    if missing:
        logger.error(
            "%s payloads missing on disk: %s",
            context,
            ", ".join(str(path) for path in missing),
        )
        return False

    return True


def _format_error_suffix(download_result: DownloadResult) -> str:
    if download_result.error_message:
        return f" ({download_result.error_message})"
    return ""


def log_cache_hits(stage: str, count: int) -> None:
    if count:
        logger.info(
            "%s loaded %d items from cache",
            stage,
            count,
            extra=console_kwargs(),
        )


def log_success(stage: str, produced: int, total: int) -> None:
    logger.info(
        "[%s] successes: %d/%d",
        stage,
        produced,
        total,
        extra=console_kwargs(),
    )


__all__ = [
    "resolve_settings",
    "create_progress_bar",
    "maybe_export",
    "run_process_pool",
    "run_thread_pool",
    "reorder_results",
    "ensure_successful_download",
    "log_cache_hits",
    "log_success",
]
