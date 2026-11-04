"""MinerU as the first PDF engine: its middle JSON read into text and tables, its workers,
and the hand-over to Docling.

`data/mineru/ymdzsl62jjjp.json` is MinerU 4.0.12's middle JSON (`--tier advanced
--ocr-mode ocr`) for PMC4733342, cut to a few blocks of each kind.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from ingestion_workflow.clients.model_server import ModelUnavailable
from ingestion_workflow.config import Settings
from ingestion_workflow.extractors import mineru_convert, pdf_extractor
from ingestion_workflow.extractors.mineru_convert import mark_header_rows, read_middle_json
from ingestion_workflow.extractors.pdf_extractor import PdfExtractor, _extract_mineru_article
from ingestion_workflow.models import (
    DownloadedFile,
    DownloadResult,
    DownloadSource,
    ExtractedContent,
    FileType,
    Identifier,
)

MIDDLE = json.loads(
    (Path(__file__).parents[1] / "data" / "mineru" / "ymdzsl62jjjp.json").read_text())


def _download(path: Path, doi: str = "10.1093/scan/nsv114") -> DownloadResult:
    return DownloadResult(
        identifier=Identifier(doi=doi),
        source=DownloadSource.PDF,
        success=True,
        files=[DownloadedFile(file_path=path, file_type=FileType.PDF,
                              content_type="application/pdf", source=DownloadSource.PDF)],
    )


def _pdf(path: Path) -> Path:
    import pypdfium2

    document = pypdfium2.PdfDocument.new()
    document.new_page(200, 200)
    document.save(str(path))
    return path


# -- middle JSON ----------------------------------------------------------------


def test_text_keeps_the_article_and_leaves_page_furniture_out():
    text = read_middle_json(MIDDLE).text

    assert "Reading fiction and reading minds" in text
    assert "F(1, 25) = 4.38" in text  # an inline equation, as its LaTeX
    assert "Addis, D. R., Wong, A. T." in text  # a list item
    for furniture in ("Advance Access", "Downloaded from", "Received: 25 May",
                      "Vol. 0, No. 0"):
        assert furniture not in text


def test_a_figure_gives_its_legend_and_nothing_to_the_text():
    document = read_middle_json(MIDDLE)

    assert [legend[:31] for _, legend in document.captions] == ["Fig 1. BOLD differences for mai"]
    assert "BOLD differences" not in document.text


def test_a_table_gives_its_caption_rows_and_header():
    document = read_middle_json(MIDDLE)

    first = document.tables[0]
    assert first.caption.startswith("Table 1. Peak voxel and cluster size")
    assert "<th>Anatomic label</th>" in first.html
    # A lone cell under the header labels the rows below it; it names no column.
    assert "<th colspan=\"6\">Social" not in first.html
    assert "Anterior temporal pole | 34 | 31 | -42 | 5362 | 6.99" in document.text
    assert [t.caption for t in document.tables][1] == "Table 1. (continued)"


@pytest.mark.parametrize(
    "html, marked",
    [
        ("<table><tr><td>Region</td><td>x</td></tr><tr><td>A</td><td>-4</td></tr></table>",
         "<th>Region</th><th>x</th>"),
        # The first row has a digit: there is no header to mark.
        ("<table><tr><td>BA 4</td><td>x</td></tr><tr><td>A</td><td>-4</td></tr></table>", None),
        # No row has a digit: there is no body to tell a header from.
        ("<table><tr><td>Region</td></tr><tr><td>Left</td></tr></table>", None),
    ],
)
def test_mark_header_rows(html, marked):
    out = mark_header_rows(html)
    if marked is None:
        assert out == html
    else:
        assert marked in out and "<td>-4</td>" in out


def test_the_article_keeps_signed_coordinates(tmp_path):
    content = _extract_mineru_article(_download(tmp_path / "a.pdf"), MIDDLE, tmp_path)

    assert content.symbols_as_digits is None
    points = {(c.x, c.y, c.z) for c in content.tables[0].coordinates}
    assert {(34, 31, -42), (-44, 9, -36)} <= points
    assert content.full_text_path.read_text().split("Figure legends")[1].strip().startswith(
        "Fig 1. BOLD")
    assert Path(content.tables[0].raw_content_path).suffix == ".html"


# -- the workers -------------------------------------------------------------------


def no_parser(slot, gpus):
    """Stands in for a worker's start: no GPU, no model."""


def fake_mineru(download, root):
    """Stands in for a worker's parse: MIDDLE for a PDF named good*, else MinerU's error."""
    path = Path(download.files[0].file_path)
    if not path.name.startswith("good"):
        raise ValueError(f"MinerU cannot read {path.name}")
    return _extract_mineru_article(download, MIDDLE, root)


def _extractor(tmp_path, monkeypatch, engines, docling=None):
    monkeypatch.setattr(mineru_convert, "mineru_gpus", lambda: [0])
    monkeypatch.setattr(pdf_extractor, "_start_mineru_worker", no_parser)
    monkeypatch.setattr(pdf_extractor, "_extract_mineru_pdf", fake_mineru)
    settings = Settings(cache_root=tmp_path / "cache", data_root=tmp_path / "data",
                        pdf_engines=engines)
    extractor = PdfExtractor(settings=settings)
    called = []

    def fake_docling(batch, root):
        called.extend(d.identifier.doi for d in batch)
        return [docling(d) for d in batch]

    monkeypatch.setattr(extractor, "_extract_with_docling", fake_docling)
    return extractor, called


def _read_by_docling(download):
    return ExtractedContent(slug=download.identifier.slug, source=DownloadSource.PDF)


def test_no_free_gpu_is_unavailable(monkeypatch):
    monkeypatch.setattr(mineru_convert.importlib.util, "find_spec", lambda name: object())
    monkeypatch.setattr(mineru_convert, "free_cuda_devices", lambda min_free_mib: [])

    with pytest.raises(ModelUnavailable, match="no GPU"):
        mineru_convert.mineru_gpus()


def test_minerU_not_installed_is_unavailable(monkeypatch):
    monkeypatch.setattr(mineru_convert.importlib.util, "find_spec", lambda name: None)

    with pytest.raises(ModelUnavailable, match="not installed"):
        mineru_convert.mineru_gpus()


# -- the engines in order ---------------------------------------------------------


def test_docling_reads_only_what_mineru_could_not(tmp_path, monkeypatch):
    extractor, called = _extractor(tmp_path, monkeypatch, ["mineru", "docling"],
                                   docling=_read_by_docling)
    downloads = [_download(_pdf(tmp_path / "good.pdf"), "10.1/a"),
                 _download(_pdf(tmp_path / "bad.pdf"), "10.1/b")]

    first, second = extractor.extract(downloads)

    assert called == ["10.1/b"]
    assert first.text_source == {"method": "mineru", "reason": "first of pdf_engines"}
    assert first.tables
    assert second.text_source["method"] == "docling"
    assert second.text_source["reason"].startswith("after mineru: MinerU extraction raised")
    assert "MinerU cannot read bad.pdf" in second.text_source["reason"]


def test_docling_alone_never_runs_mineru(tmp_path, monkeypatch):
    extractor, called = _extractor(tmp_path, monkeypatch, ["docling"],
                                   docling=_read_by_docling)
    monkeypatch.setattr(mineru_convert, "mineru_gpus", lambda: pytest.fail("MinerU ran"))

    (content,) = extractor.extract([_download(_pdf(tmp_path / "a.pdf"), "10.1/a")])

    assert called == ["10.1/a"]
    assert content.text_source == {"method": "docling", "reason": "first of pdf_engines"}


def test_minerU_unavailable_stops_rather_than_handing_everything_to_docling(
        tmp_path, monkeypatch):
    def unavailable():
        raise ModelUnavailable("MinerU: no GPU has 7000 MiB free")

    extractor, called = _extractor(tmp_path, monkeypatch, ["mineru", "docling"])
    monkeypatch.setattr(mineru_convert, "mineru_gpus", unavailable)

    with pytest.raises(ModelUnavailable):
        extractor.extract([_download(_pdf(tmp_path / "a.pdf"))])
    assert called == []


def test_minerU_that_reads_none_of_a_batch_is_unavailable(tmp_path, monkeypatch):
    """A worker that cannot start fails every PDF alike: that is MinerU's fault, not theirs."""
    extractor, called = _extractor(tmp_path, monkeypatch, ["mineru", "docling"])
    downloads = [_download(_pdf(tmp_path / f"bad{n}.pdf"), f"10.1/{n}") for n in range(2)]

    with pytest.raises(ModelUnavailable, match="read none of 2 PDFs"):
        extractor.extract(downloads)
    assert called == []
