"""Tests for the open-access PDF download/extract source."""

from __future__ import annotations

import pandas as pd
import pytest
from ingestion_workflow.config import Settings
from ingestion_workflow.extractors.docling_convert import (
    _cell_text_from_layer,
    _is_unusable,
    normalize_text_tokens,
)
from ingestion_workflow.extractors.pdf_extractor import (
    PdfExtractor,
    _has_suspect_numerics,
    _merge_split_coordinate_columns,
    _table_label,
)
from ingestion_workflow.models import (
    DownloadSource,
    FileType,
    Identifier,
    Identifiers,
)

MINIMAL_PDF = b"%PDF-1.4\n%%EOF\n"


def _settings(tmp_path) -> Settings:
    return Settings(
        cache_root=tmp_path / "cache",
        data_root=tmp_path / "data",
        pdf_cache_root=tmp_path / "pdfs",
        max_workers=1,
    )


class _FakeResponse:
    def __init__(self, content: bytes, status: int = 200) -> None:
        self.content = content
        self._status = status

    def raise_for_status(self) -> None:
        if self._status >= 400:
            raise RuntimeError(f"HTTP {self._status}")


class _FakeSession:
    def __init__(self, responses: dict[str, _FakeResponse]) -> None:
        self.responses = responses
        self.requested: list[str] = []

    def get(self, url, **kwargs):
        self.requested.append(url)
        if url not in self.responses:
            raise RuntimeError("connection refused")
        return self.responses[url]


def test_download_writes_pdf_and_reports_success(monkeypatch, tmp_path):
    identifiers = Identifiers([Identifier(doi="10.1234/has-pdf")])
    session = _FakeSession({"https://example.org/a.pdf": _FakeResponse(MINIMAL_PDF)})
    extractor = PdfExtractor(settings=_settings(tmp_path), session=session)

    monkeypatch.setattr(
        PdfExtractor,
        "_resolve_pdf_urls",
        lambda self, ids: {"10-1234-has-pdf": "https://example.org/a.pdf"},
    )

    results = extractor.download(identifiers)

    assert len(results) == 1
    result = results[0]
    assert result.success
    assert result.source is DownloadSource.PDF
    assert len(result.files) == 1
    assert result.files[0].file_type is FileType.PDF
    assert result.files[0].file_path.read_bytes() == MINIMAL_PDF


def test_download_fails_without_a_pdf_url(monkeypatch, tmp_path):
    identifiers = Identifiers([Identifier(doi="10.1234/no-pdf")])
    extractor = PdfExtractor(settings=_settings(tmp_path), session=_FakeSession({}))
    monkeypatch.setattr(PdfExtractor, "_resolve_pdf_urls", lambda self, ids: {})

    result = extractor.download(identifiers)[0]

    assert not result.success
    assert "No open-access PDF URL" in result.error_message


def test_download_rejects_non_pdf_payloads(monkeypatch, tmp_path):
    """Publisher landing pages and paywalls answer 200 with HTML."""
    identifiers = Identifiers([Identifier(doi="10.1234/landing-page")])
    session = _FakeSession(
        {"https://example.org/landing": _FakeResponse(b"<html>Subscribe</html>")}
    )
    extractor = PdfExtractor(settings=_settings(tmp_path), session=session)
    monkeypatch.setattr(
        PdfExtractor,
        "_resolve_pdf_urls",
        lambda self, ids: {"10-1234-landing-page": "https://example.org/landing"},
    )

    result = extractor.download(identifiers)[0]

    assert not result.success
    assert "not a PDF" in result.error_message


def test_resolve_pdf_urls_falls_back_to_next_provider(monkeypatch, tmp_path):
    identifiers = Identifiers(
        [Identifier(doi="10.1234/from-s2"), Identifier(doi="10.1234/from-openalex")]
    )
    extractor = PdfExtractor(settings=_settings(tmp_path))

    monkeypatch.setattr(
        PdfExtractor,
        "_semantic_scholar_pdf_urls",
        lambda self, ids: {"10-1234-from-s2": "https://example.org/s2.pdf"},
    )

    seen_by_openalex: list[list[str]] = []

    def fake_openalex(self, ids):
        seen_by_openalex.append([identifier.slug for identifier in ids])
        return {"10-1234-from-openalex": "https://example.org/oa.pdf"}

    monkeypatch.setattr(PdfExtractor, "_openalex_pdf_urls", fake_openalex)

    urls = extractor._resolve_pdf_urls(identifiers)

    assert urls == {
        "10-1234-from-s2": "https://example.org/s2.pdf",
        "10-1234-from-openalex": "https://example.org/oa.pdf",
    }
    # OpenAlex is only asked about what Semantic Scholar could not answer.
    assert seen_by_openalex == [["10-1234-from-openalex"]]


def test_resolve_pdf_urls_survives_a_failing_provider(monkeypatch, tmp_path):
    identifiers = Identifiers([Identifier(doi="10.1234/x")])
    extractor = PdfExtractor(settings=_settings(tmp_path))

    def boom(self, ids):
        raise RuntimeError("provider is down")

    monkeypatch.setattr(PdfExtractor, "_semantic_scholar_pdf_urls", boom)
    monkeypatch.setattr(
        PdfExtractor,
        "_openalex_pdf_urls",
        lambda self, ids: {"10-1234-x": "https://example.org/oa.pdf"},
    )

    assert extractor._resolve_pdf_urls(identifiers) == {
        "10-1234-x": "https://example.org/oa.pdf"
    }


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("GLYPH<0>42", "-42"),
        ("−37", "-37"),
        ("- 42", "-42"),
        ("plain text", "plain text"),
        (None, None),
    ],
)
def test_normalize_text_tokens(raw, expected):
    assert normalize_text_tokens(raw) == expected


def test_suspect_numerics_flags_colon_for_decimal_point():
    frame = pd.DataFrame({"p": ["0 : 87", "0.05"]})
    assert _has_suspect_numerics(frame) is True


def test_suspect_numerics_ignores_clean_values():
    frame = pd.DataFrame({"p": ["0.87", "-37"]})
    assert _has_suspect_numerics(frame) is False


@pytest.mark.parametrize(
    ("caption", "expected"),
    [
        ("TABLE 3 | Regional variation of cortex thickness", "TABLE 3"),
        ("Table IV. Peak coordinates", "Table IV"),
        ("Some caption with no label", None),
    ],
)
def test_table_label(caption, expected):
    assert _table_label(caption) == expected


def test_extract_reports_failure_when_no_pdf_file(tmp_path):
    from ingestion_workflow.models import DownloadResult

    download_result = DownloadResult(
        identifier=Identifier(doi="10.1234/x"),
        source=DownloadSource.PDF,
        success=True,
        files=[],
    )
    extractor = PdfExtractor(settings=_settings(tmp_path))

    content = extractor.extract([download_result])[0]

    assert content.error_message == "No PDF file in download result."
    assert content.tables == []


class _FakeBBox:
    def __init__(self, left=10.0, bottom=20.0, right=40.0, top=30.0) -> None:
        self.l = left
        self.b = bottom
        self.r = right
        self.t = top
        self.coord_origin = "BOTTOMLEFT"


class _FakeCell:
    def __init__(self, text: str) -> None:
        self.text = text
        self.bbox = _FakeBBox()


class _FakeTextPage:
    """Returns a fixed string for any bounding box query."""

    def __init__(self, text: str, padded: str | None = None) -> None:
        self._text = text
        self._padded = padded

    def get_text_bounded(self, left, bottom, right, top):
        if self._padded is not None and left < 10.0:
            return self._padded
        return self._text


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("-48", False),
        ("", False),
        ("value\twith\ttabs", False),
        ("\x0448", True),
        ("\x0172", True),
    ],
)
def test_is_unusable_flags_control_characters(text, expected):
    assert _is_unusable(text) is expected


def test_cell_text_from_layer_keeps_docling_read_when_layer_is_corrupt():
    r"""A font with no ToUnicode for minus makes PyPDFium return "\x04" + digits.

    Returning None leaves Docling's own (correct) "-48" in place; returning the
    layer's text would strip the sign and flip the hemisphere.
    """
    cell = _FakeCell("-48")
    text_page = _FakeTextPage("\x0448")

    assert _cell_text_from_layer(cell, text_page, page_height=100.0) is None


def test_cell_text_from_layer_still_recovers_a_dropped_minus():
    """The widened read is accepted when it only adds a sign character."""
    cell = _FakeCell("48")
    text_page = _FakeTextPage("48", padded="-48")

    assert _cell_text_from_layer(cell, text_page, page_height=100.0) == "-48"


def test_merge_split_coordinate_columns_rejoins_a_broken_triplet():
    frame = pd.DataFrame(
        {
            "Region": ["R postcentral", "L intraparietal", "L precentral", "R insula"],
            "Coordinates": ["42,", "30,", "45,", "48,"],
            "Coordinates.1": ["24, 45", "2, 33", "36, 36", "0, 48"],
        }
    )

    merged = _merge_split_coordinate_columns(frame)

    assert list(merged.columns) == ["Region", "Coordinates"]
    assert merged["Coordinates"].tolist() == [
        "42, 24, 45",
        "30, 2, 33",
        "45, 36, 36",
        "48, 0, 48",
    ]


def test_merge_split_coordinate_columns_leaves_non_coordinate_tables_alone():
    """Headers that do not name coordinates are never merged."""
    frame = pd.DataFrame(
        {
            "Group": ["smokers", "controls"],
            "Age": ["31, 4", "29, 5"],
            "Sex": ["12, 8", "11, 9"],
        }
    )

    merged = _merge_split_coordinate_columns(frame)

    assert list(merged.columns) == ["Group", "Age", "Sex"]


def test_merge_split_coordinate_columns_needs_enough_whole_triplets():
    """Two coordinate columns that do not join into triplets are left as they are."""
    frame = pd.DataFrame(
        {
            "MNI coordinate": ["42,", "30,"],
            "MNI coordinate.1": ["24", "2"],
        }
    )

    merged = _merge_split_coordinate_columns(frame)

    assert list(merged.columns) == ["MNI coordinate", "MNI coordinate.1"]


class _TableCellStub:
    def __init__(self, text):
        self.text = text


class _TableDataStub:
    def __init__(self, texts):
        self.table_cells = [_TableCellStub(t) for t in texts]


class _TableItemStub:
    """Stands in for a docling TableItem, which needs the whole library."""

    def __init__(self, html, texts=(), raises=False):
        self._html = html
        self._raises = raises
        self.data = _TableDataStub(texts)

    def export_to_html(self, doc=None, add_caption=True):
        if self._raises:
            raise RuntimeError("no serializer available")
        return self._html


def test_html_is_written_for_the_serialiser_to_read(tmp_path):
    """A DataFrame has no header flag and no spans; the HTML has both."""
    from ingestion_workflow.extractors.pdf_extractor import _write_table_html

    html = ('<table><tbody><tr><th rowspan="2">Region</th>'
            '<th colspan="3">MNI</th></tr><tr><th>x</th><th>y</th><th>z</th></tr>'
            '<tr><td>L IFG</td><td>-42</td><td>18</td><td>4</td></tr></tbody></table>')
    path = _write_table_html(_TableItemStub(html), object(), tmp_path, "table_000")
    assert path is not None
    assert path.name == "table_000.html"
    assert 'colspan="3"' in path.read_text(encoding="utf-8")


def test_a_failed_html_export_falls_back_rather_than_losing_the_table(tmp_path):
    from ingestion_workflow.extractors.pdf_extractor import _write_table_html

    assert _write_table_html(_TableItemStub("", raises=True), object(),
                             tmp_path, "t") is None
    assert _write_table_html(_TableItemStub(""), object(), tmp_path, "t") is None
    assert _write_table_html(_TableItemStub("no markup here"), object(),
                             tmp_path, "t") is None
    assert not list(tmp_path.iterdir())


def test_minus_glyphs_are_normalised_on_the_document_not_just_the_frame():
    """A coordinate whose sign is a glyph token or a dash lands in the wrong
    hemisphere, and the HTML export never saw the per-frame normalisation."""
    from ingestion_workflow.extractors.docling_convert import normalize_text_tokens

    data = _TableDataStub(["\u201342", "GLYPH<0>18", "\u2212 12", "4"])
    for cell in data.table_cells:
        cell.text = normalize_text_tokens(cell.text)
    assert [c.text for c in data.table_cells] == ["-42", "-18", "-12", "4"]


def test_table_footnotes_are_carried_through(tmp_path):
    """Docling resolves footnotes exactly as it resolves captions, and a table
    footnote carries the threshold, the statistic and the laterality key."""
    from ingestion_workflow.extractors.pdf_extractor import _footnote_texts

    class _Ref:
        def __init__(self, text):
            self._text = text

        def resolve(self, doc=None):
            return type("R", (), {"text": self._text})()

    class _Broken:
        def resolve(self, doc=None):
            raise RuntimeError("dangling reference")

    class _Table:
        footnotes = [_Ref("L, left; R, right; BA, Brodmann area."), _Broken(),
                     _Ref("Threshold p<0.05 FWE.")]

    got = _footnote_texts(_Table(), object())
    assert got == ["L, left; R, right; BA, Brodmann area.", "Threshold p<0.05 FWE."]
    # a table with no footnotes yields nothing rather than failing
    assert _footnote_texts(type("T", (), {"footnotes": []})(), object()) == []
    assert _footnote_texts(object(), object()) == []
