"""Tests for the open-access PDF download/extract source."""

from __future__ import annotations

import pandas as pd
import pytest

from ingestion_workflow.config import Settings
from ingestion_workflow.extractors.docling_convert import normalize_text_tokens
from ingestion_workflow.extractors.pdf_extractor import (
    PdfExtractor,
    _has_suspect_numerics,
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
