"""Only the CUDA-using extractor should pay for `spawn`."""

from __future__ import annotations

import pytest
from ingestion_workflow.config import Settings
from ingestion_workflow.extractors.base import BaseExtractor


@pytest.fixture()
def settings(tmp_path):
    return Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c")


def test_base_default_is_fork():
    """`spawn` re-imports this package per worker, which drags in nilearn and
    sklearn via pubget and costs seconds. Only CUDA needs it."""
    assert BaseExtractor.mp_start_method == "fork"


def test_pdf_extractor_keeps_spawn(settings):
    from ingestion_workflow.extractors.pdf_extractor import PdfExtractor

    assert PdfExtractor.mp_start_method == "spawn"


@pytest.mark.parametrize(
    "module_name,class_name",
    [
        ("pubget_extractor", "PubgetExtractor"),
        ("elsevier_extractor", "ElsevierExtractor"),
        ("ace_extractor", "ACEExtractor"),
    ],
)
def test_xml_extractors_use_fork(module_name, class_name):
    import importlib

    module = importlib.import_module(f"ingestion_workflow.extractors.{module_name}")
    assert getattr(module, class_name).mp_start_method == "fork"


def test_a_single_item_never_starts_a_pool(settings, monkeypatch):
    """Below the pool threshold the work runs inline, so a one-article run does
    not pay any process startup at all."""
    import multiprocessing

    from ingestion_workflow.models import (
        DownloadResult,
        DownloadSource,
        ExtractedContent,
        Identifier,
    )

    def boom(*args, **kwargs):  # pragma: no cover - must not be reached
        raise AssertionError("a pool was started for a single item")

    monkeypatch.setattr(multiprocessing, "get_context", boom)

    extractor = BaseExtractor()
    download = DownloadResult(
        identifier=Identifier(pmid="1"), source=DownloadSource.PUBGET, success=True
    )
    calls = []

    def worker(result, root):
        calls.append(result)
        return ExtractedContent(slug="1", source=DownloadSource.PUBGET)

    out = extractor._run_extraction_pipeline(
        [download],
        extraction_root=settings.data_root / "x",
        worker=worker,
        worker_count=4,
        source_name="Test",
        failure_message="no",
        failure_builder=lambda d, m: ExtractedContent(slug="1", source=d.source, error_message=m),
    )
    assert len(out) == 1
    assert len(calls) == 1
