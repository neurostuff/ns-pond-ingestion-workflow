"""MinerU as the first PDF engine: its middle JSON read into text and tables, the runs of
its CLI, and the hand-over to Docling.

`data/mineru/ymdzsl62jjjp.json` is MinerU 4.0.12's middle JSON (`--tier advanced
--ocr-mode ocr`) for PMC4733342, cut to a few blocks of each kind.
"""

from __future__ import annotations

import json
import stat
import sys
from pathlib import Path

import pytest
from ingestion_workflow.clients.model_server import ModelUnavailable
from ingestion_workflow.config import Settings
from ingestion_workflow.extractors import mineru_convert
from ingestion_workflow.extractors.mineru_convert import (
    mark_header_rows,
    read_middle_json,
    run_mineru,
)
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


def test_the_article_keeps_signed_coordinates_and_minerus_answer(tmp_path):
    content = _extract_mineru_article(_download(tmp_path / "a.pdf"), MIDDLE, tmp_path)

    assert content.symbols_as_digits is None
    points = {(c.x, c.y, c.z) for c in content.tables[0].coordinates}
    assert {(34, 31, -42), (-44, 9, -36)} <= points
    assert content.full_text_path.read_text().split("Figure legends")[1].strip().startswith(
        "Fig 1. BOLD")
    assert Path(content.tables[0].raw_content_path).suffix == ".html"
    assert (content.full_text_path.parent / "mineru.json.gz").is_file()


# -- the CLI --------------------------------------------------------------------

#: Reads its inputs in order and stops on one whose target is named bad*, as
#: `mineru-kit` 4.0.12 stops on a PDF it cannot read; each run is logged.
FAKE_MINERU = """#!{python}
import json, pathlib, sys
args = sys.argv[2:]
out = pathlib.Path(args[args.index("--output") + 1])
inputs = [pathlib.Path(a) for a in args[:args.index("--output")]]
with open(pathlib.Path(sys.argv[0]).with_suffix(".runs"), "a") as runs:
    runs.write(" ".join(p.stem for p in inputs) + "\\n")
for pdf in inputs:
    if pdf.resolve().name.startswith("{fail}"):
        print("Error: Failed to parse", pdf)
        sys.exit(1)
    answer = {{"pages": [], "from": pdf.resolve().name}}
    (out / (pdf.stem + ".json")).write_text(json.dumps(answer))
"""


def _fake_mineru(tmp_path: Path, fail: str = "bad") -> Path:
    command = tmp_path / "bin" / "mineru-kit"
    command.parent.mkdir()
    command.write_text(FAKE_MINERU.format(python=sys.executable, fail=fail))
    command.chmod(command.stat().st_mode | stat.S_IEXEC)
    return command


def _runs(command: Path) -> list:
    return command.with_suffix(".runs").read_text().splitlines()


def test_a_run_that_stops_on_a_pdf_is_started_again_without_it(tmp_path):
    command = _fake_mineru(tmp_path)
    pdfs = [_pdf(tmp_path / name) for name in ("good0.pdf", "bad1.pdf", "good2.pdf")]

    read = run_mineru(pdfs, tmp_path / "work", str(command), gpu=0)

    assert read[0]["from"] == "good0.pdf" and read[2]["from"] == "good2.pdf"
    assert read[1].startswith("MinerU stopped on this PDF")
    assert _runs(command) == ["0 1 2", "2"]


def test_a_file_pdfium_cannot_open_never_reaches_minerU(tmp_path):
    command = _fake_mineru(tmp_path)
    broken = tmp_path / "broken.pdf"
    broken.write_bytes(b"%PDF-1.4 not a document")

    read = run_mineru([broken, _pdf(tmp_path / "good.pdf")], tmp_path / "work", str(command), 0)

    assert read[0].startswith("PDFium cannot open the file")
    assert _runs(command) == ["1"]


def test_minerU_that_reads_nothing_twice_is_unavailable(tmp_path):
    command = _fake_mineru(tmp_path, fail="")  # every name starts with ""
    pdfs = [_pdf(tmp_path / f"p{n}.pdf") for n in range(3)]

    with pytest.raises(ModelUnavailable, match="two runs in a row"):
        run_mineru(pdfs, tmp_path / "work", str(command), gpu=0)
    assert len(_runs(command)) == 2


def test_a_missing_command_is_unavailable(tmp_path):
    with pytest.raises(ModelUnavailable, match="not found"):
        run_mineru([_pdf(tmp_path / "a.pdf")], tmp_path / "work",
                   str(tmp_path / "no-such-mineru"), gpu=0)


def test_no_free_gpu_is_unavailable(monkeypatch):
    monkeypatch.setattr(mineru_convert, "free_cuda_devices", lambda min_free_mib: [])

    with pytest.raises(ModelUnavailable, match="no GPU"):
        mineru_convert.mineru_gpus()


# -- the engines in order ---------------------------------------------------------


def _extractor(tmp_path, monkeypatch, engines, docling=None):
    settings = Settings(cache_root=tmp_path / "cache", data_root=tmp_path / "data",
                        pdf_engines=engines, mineru_command="mineru-kit")
    extractor = PdfExtractor(settings=settings)
    called = []

    def fake_docling(batch, root):
        called.extend(d.identifier.doi for d in batch)
        return [docling(d) for d in batch]

    monkeypatch.setattr(extractor, "_extract_with_docling", fake_docling)
    return extractor, called


def test_docling_reads_only_what_mineru_could_not(tmp_path, monkeypatch):
    monkeypatch.setattr(mineru_convert, "run_on_gpus",
                        lambda pdfs, work, command: {0: MIDDLE, 1: "MinerU stopped on this PDF"})
    extractor, called = _extractor(
        tmp_path, monkeypatch, ["mineru", "docling"],
        docling=lambda d: ExtractedContent(slug=d.identifier.slug, source=DownloadSource.PDF))
    downloads = [_download(_pdf(tmp_path / "a.pdf"), "10.1/a"),
                 _download(_pdf(tmp_path / "b.pdf"), "10.1/b")]

    first, second = extractor.extract(downloads)

    assert called == ["10.1/b"]
    assert first.text_source == {"method": "mineru", "reason": "first of pdf_engines"}
    assert first.tables
    assert second.text_source == {"method": "docling",
                                  "reason": "after mineru: MinerU stopped on this PDF"}


def test_docling_alone_never_runs_mineru(tmp_path, monkeypatch):
    def no_mineru(*args):
        raise AssertionError("MinerU ran")

    monkeypatch.setattr(mineru_convert, "run_on_gpus", no_mineru)
    extractor, called = _extractor(
        tmp_path, monkeypatch, ["docling"],
        docling=lambda d: ExtractedContent(slug=d.identifier.slug, source=DownloadSource.PDF))

    (content,) = extractor.extract([_download(_pdf(tmp_path / "a.pdf"), "10.1/a")])

    assert called == ["10.1/a"]
    assert content.text_source == {"method": "docling", "reason": "first of pdf_engines"}


def test_minerU_unavailable_stops_rather_than_handing_everything_to_docling(
        tmp_path, monkeypatch):
    def unavailable(*args):
        raise ModelUnavailable("MinerU: no GPU has 7000 MiB free")

    monkeypatch.setattr(mineru_convert, "run_on_gpus", unavailable)
    extractor, called = _extractor(tmp_path, monkeypatch, ["mineru", "docling"])

    with pytest.raises(ModelUnavailable):
        extractor.extract([_download(_pdf(tmp_path / "a.pdf"))])
    assert called == []
