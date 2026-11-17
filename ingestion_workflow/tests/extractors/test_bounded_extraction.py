"""A PDF Docling cannot finish fails alone; its batch is still recorded.

7rsekeh66ykb (PMID 18595208, a 7-page CJK review) grew Docling's memory without
bound on CPU and GPU, until the kernel killed the extract run at 47 GB and the
~380 articles batched with it were never recorded. The fakes below stand in for
Docling: one grows the same way, one never returns.
"""

from __future__ import annotations

import multiprocessing
import os
import time
from pathlib import Path

# Imported here, not in the worker, so a child pays for them while it starts up and
# not inside an article's time limit.
import pubget._coordinate_space  # noqa: F401
import pubget._coordinates  # noqa: F401
import pytest
from ingestion_workflow.catalog import Catalog, Outcome, Status
from ingestion_workflow.config import Settings
from ingestion_workflow.extractors import pdf_extractor
from ingestion_workflow.extractors.bounded import Limits, bounded_map
from ingestion_workflow.models import DownloadResult, DownloadSource, Identifier
from ingestion_workflow.pipeline import Context, run_stages
from ingestion_workflow.pipeline.stages import DownloadStage, ExtractStage

MEMORY_MB = 1500
TIMEOUT_S = 10.0


def _grow():
    held = []
    while True:
        held.append(b"\x01" * (64 << 20))


class _Document:
    tables: list = []
    pictures: list = []

    def export_to_text(self) -> str:
        return "A normal article's text."


def fake_convert(path):
    kind = Path(path).read_bytes()[len(pdf_extractor.PDF_MAGIC):]
    if kind == b"grows":
        _grow()
    if kind == b"hangs":
        time.sleep(3600)
    return _Document()


def with_fake_converter(download, root):
    """The real per-article worker, with Docling replaced in the child that runs it."""
    from ingestion_workflow.extractors import docling_convert

    docling_convert.convert_pdf = fake_convert
    return pdf_extractor._extract_pdf_article(download, root)


@pytest.fixture()
def env(tmp_path, monkeypatch, evidence):
    monkeypatch.setattr(pdf_extractor, "_extract_pdf_article", with_fake_converter)
    monkeypatch.setattr(pdf_extractor, "usable_cuda_devices", lambda: [])
    settings = Settings(
        data_root=tmp_path / "d",
        cache_root=tmp_path / "c",
        catalog_root=tmp_path / "k",
        download_sources=["pdf"],
        pdf_engines=["docling"],
        pdf_extract_memory_mb=MEMORY_MB,
        pdf_extract_timeout_s=TIMEOUT_S,
    )
    with Catalog.open(settings.catalog_root) as catalog:
        refs = catalog.register_many(
            [Identifier(pmcid=f"PMC{n}") for n in (1, 2, 3)], evidence
        )
        for ref, kind in zip(refs, (b"grows", b"normal", b"hangs")):
            downloaded(settings, catalog, ref, pdf_extractor.PDF_MAGIC + kind)
        yield settings, catalog, refs


def downloaded(settings, catalog, ref, payload):
    file = pdf_extractor.store_pdf(
        settings.get_cache_dir("pdf"), ref.identifier, payload, DownloadSource.PDF
    )
    result = DownloadResult(ref.identifier, DownloadSource.PDF, True, files=[file])
    catalog.record([Outcome(
        ref.id, "download", "pdf", Status.OK,
        DownloadStage(settings).fingerprint_for(DownloadSource.PDF), result.to_dict())])


def test_a_pdf_over_its_limits_fails_alone_and_its_batch_is_recorded(env):
    settings, catalog, refs = env
    grows, normal, hangs = (ref.id for ref in refs)

    report = run_stages(Context(settings, catalog), [ExtractStage(settings)], refs)

    assert (report.stages["extract"].ok, report.stages["extract"].failed) == (1, 2)
    extractions = {
        article: by_source["pdf"]
        for article, by_source in catalog.artifacts([r.id for r in refs], "extract").items()
    }
    assert extractions[normal].status is Status.OK
    assert extractions[normal].summary["has_text"] is True
    assert extractions[grows].status is Status.FAILED
    assert f"over its {MEMORY_MB:,} MB memory limit" in extractions[grows].error
    assert extractions[hangs].status is Status.FAILED
    assert f"killed after {TIMEOUT_S:g} s, its time limit" in extractions[hangs].error
    # each counted as an attempt, as any other failed extraction is
    attempts = catalog.attempt_counts([grows, hangs], "extract", "pdf")
    assert {article: count for article, (count, _) in attempts.items()} == {
        grows: 1, hangs: 1,
    }


def report_device(item):
    if item == "grows":
        _grow()
    return os.environ.get("CUDA_VISIBLE_DEVICES")


def test_a_killed_workers_replacement_keeps_its_gpu():
    """Two workers on one card would each hold a converter on 8 GB of VRAM."""
    answers = bounded_map(
        ["grows", "after"],
        report_device,
        (),
        workers=1,
        limits=Limits(memory_mb=MEMORY_MB, timeout_s=TIMEOUT_S),
        context=multiprocessing.get_context("spawn"),
        initializer=pdf_extractor._pin_worker_to_gpu,
        initargs=([5, 7],),
    )
    by_item = {index: (result, error) for index, result, error in answers}

    assert "memory limit" in str(by_item[0][1])
    assert by_item[1] == ("5", None)


def start_and_grow(pid_file):
    """Starts a process that outlives this one unless killed with it, as vLLM's engine does."""
    import subprocess
    import sys

    engine = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(600)"])
    Path(pid_file).write_text(str(engine.pid))
    _grow()


def _gone(pid: int) -> bool:
    try:
        with open(f"/proc/{pid}/stat") as handle:
            return handle.read().split()[2] == "Z"
    except FileNotFoundError:
        return True


@pytest.mark.skipif(not Path("/proc").is_dir(), reason="reads /proc")
def test_a_killed_worker_takes_what_it_started_with_it(tmp_path):
    """MinerU's vLLM engine, orphaned, kept 5.4 GB of its GPU."""
    pid_file = tmp_path / "engine.pid"

    # Killed over its memory, not its time: a slow runner may take longer than any
    # short time limit to start the worker at all.
    ((_, _, error),) = bounded_map(
        [str(pid_file)], start_and_grow, (), workers=1,
        limits=Limits(memory_mb=MEMORY_MB, timeout_s=60.0),
        context=multiprocessing.get_context("spawn"),
    )

    assert "memory limit" in str(error)
    engine = int(pid_file.read_text())
    deadline = time.monotonic() + 5
    while not _gone(engine) and time.monotonic() < deadline:
        time.sleep(0.1)
    assert _gone(engine)
