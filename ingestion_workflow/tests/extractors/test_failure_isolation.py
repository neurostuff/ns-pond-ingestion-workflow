"""One article that raises must not take the batch with it.

ExtractStage.execute catches at batch level, so anything escaping
_run_extraction_pipeline fails every article in that batch -- up to 500.
"""

from __future__ import annotations

import pytest
from ingestion_workflow.extractors.base import BaseExtractor
from ingestion_workflow.models import (
    DownloadResult,
    DownloadSource,
    ExtractedContent,
    Identifier,
)


def downloads(n: int):
    return [
        DownloadResult(
            identifier=Identifier(pmid=str(i)), source=DownloadSource.PUBGET, success=True
        )
        for i in range(n)
    ]


def failure_builder(download, message):
    return ExtractedContent(
        slug=download.identifier.slug, source=download.source, error_message=message
    )


#: Module level, not a closure: ProcessPoolExecutor pickles the callable, so a
#: closure fails in the worker rather than in the article under test.
BREAKS_ON = "2"


def breaking_worker(download, root):
    if download.identifier.pmid == BREAKS_ON:
        raise ValueError("malformed XML: premature end of data")
    return ExtractedContent(slug=download.identifier.slug, source=download.source)


def run(extractor, batch, workers, tmp_path, **kw):
    return extractor._run_extraction_pipeline(
        batch,
        extraction_root=tmp_path / "out",
        worker=breaking_worker,
        worker_count=workers,
        source_name="Test",
        failure_message="no result",
        failure_builder=failure_builder,
        **kw,
    )


@pytest.mark.parametrize("workers", [1, 4])
def test_a_raising_article_is_isolated(workers, tmp_path):
    """Serial and pool paths must behave the same. Only the serial path
    lacked this, so a batch of one bad article failed all of them."""
    results = run(BaseExtractor(), downloads(5), workers, tmp_path)

    assert len(results) == 5
    failed = [r for r in results if r.error_message]
    assert len(failed) == 1
    assert "malformed XML" in failed[0].error_message
    assert sum(1 for r in results if not r.error_message) == 4


def test_the_serial_path_survives_a_batch_that_is_all_bad(tmp_path):
    only_bad = [
        DownloadResult(
            identifier=Identifier(pmid="2"), source=DownloadSource.PUBGET, success=True
        )
    ]
    results = run(BaseExtractor(), only_bad, 1, tmp_path)
    assert len(results) == 1
    assert "malformed XML" in results[0].error_message


def test_progress_is_still_reported_for_a_failed_article(tmp_path):
    """The bar must not stall on a failure -- these runs are unattended."""
    seen = []
    run(BaseExtractor(), downloads(5), 1, tmp_path, progress_hook=seen.append)
    assert sum(seen) == 5


def test_results_stay_in_input_order(tmp_path):
    results = run(BaseExtractor(), downloads(5), 1, tmp_path)
    assert [r.slug for r in results] == [Identifier(pmid=str(i)).slug for i in range(5)]
    assert results[2].error_message is not None
