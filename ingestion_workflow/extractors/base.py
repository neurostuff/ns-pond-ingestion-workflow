from __future__ import annotations

import logging
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Callable, Dict, List, Sequence, Tuple

from ingestion_workflow.models import DownloadResult, ExtractionResult, Identifier, Identifiers
from ingestion_workflow.utils.progress import emit_progress

logger = logging.getLogger(__name__)

ExtractionWorker = Callable[[DownloadResult, Path | str], ExtractionResult]
FailureBuilder = Callable[[DownloadResult, str], ExtractionResult]


class BaseExtractor:
    """Shared interface for extractor implementations."""

    #: Start method for the extraction pool. `spawn` re-imports this package in
    #: every worker, which costs ~3.2s each because `pubget` pulls in nilearn
    #: and sklearn; only an extractor that touches CUDA needs to pay it.
    mp_start_method: str = "fork"

    def download(
        self,
        identifiers: Identifiers,
        progress_hook: Callable[[int], None] | None = None,
    ) -> List[DownloadResult]:
        raise NotImplementedError("Subclasses must implement this method.")

    def extract(self, download_result: List[DownloadResult]) -> List[ExtractionResult]:
        raise NotImplementedError("Subclasses must implement this method.")

    @staticmethod
    def _ordered_results(
        identifiers: Identifiers,
        results_by_index: Dict[int, DownloadResult],
        default_builder: Callable[[Identifier], DownloadResult],
        *,
        progress_hook: Callable[[int], None] | None = None,
    ) -> List[DownloadResult]:
        ordered: List[DownloadResult] = []
        for index, identifier in enumerate(identifiers):
            result = results_by_index.get(index)
            if result is None:
                result = default_builder(identifier)
            ordered.append(result)
            emit_progress(progress_hook)
        return ordered

    def _run_extraction_pipeline(
        self,
        download_results: Sequence[DownloadResult],
        *,
        extraction_root: Path,
        worker: ExtractionWorker,
        worker_count: int,
        source_name: str,
        failure_message: str,
        failure_builder: FailureBuilder,
        progress_hook: Callable[[int], None] | None = None,
        worker_initializer: Callable[..., None] | None = None,
        worker_initargs: Tuple[Any, ...] = (),
        min_batch_for_pool: int = 2,
    ) -> List[ExtractionResult]:
        if not download_results:
            return []

        for download_result in download_results:
            if not download_result.success:
                raise ValueError(
                    f"{source_name}Extractor.extract received an unsuccessful download "
                    "result; validate downloads before invoking extraction."
                )

        extraction_root.mkdir(parents=True, exist_ok=True)
        ordered_results: List[ExtractionResult | None] = [None] * len(download_results)
        worker_count = max(1, worker_count)

        if worker_count == 1 or len(download_results) < max(2, min_batch_for_pool):
            for index, download_result in enumerate(download_results):
                ordered_results[index] = worker(download_result, extraction_root)
                emit_progress(progress_hook)
        else:
            root_arg = str(extraction_root)
            with ProcessPoolExecutor(
                max_workers=worker_count,
                mp_context=multiprocessing.get_context(self.mp_start_method),
                initializer=worker_initializer,
                initargs=worker_initargs,
            ) as executor:
                future_map = {
                    executor.submit(worker, download_result, root_arg): index
                    for index, download_result in enumerate(download_results)
                }
                for future in as_completed(future_map):
                    index = future_map[future]
                    download_result = download_results[index]
                    try:
                        ordered_results[index] = future.result()
                    except Exception as exc:  # pragma: no cover - defensive guard
                        logger.exception(
                            "%s extraction raised exception for %s",
                            source_name,
                            download_result.identifier.slug,
                        )
                        ordered_results[index] = failure_builder(
                            download_result,
                            f"{source_name} extraction raised an exception: {exc}",
                        )
                    finally:
                        emit_progress(progress_hook)

        final_results: List[ExtractionResult] = []
        for index, download_result in enumerate(download_results):
            result = ordered_results[index]
            if result is None:
                result = failure_builder(download_result, failure_message)
            final_results.append(result)
        return final_results
