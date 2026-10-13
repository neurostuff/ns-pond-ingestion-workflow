"""The Wiley TDM source: what it asks Wiley for, what it keeps, and how its PDFs are read."""

from __future__ import annotations

from pathlib import Path

import pytest
import vcr as vcrpy
from ingestion_workflow.catalog import Catalog, Status
from ingestion_workflow.config import Settings
from ingestion_workflow.extractors.pdf_extractor import PDF_MAGIC, PdfExtractor, store_pdf
from ingestion_workflow.extractors.utils import safe_hash_stem
from ingestion_workflow.extractors.wiley_extractor import (
    NOT_WILEY,
    REQUEST_INTERVAL_SECONDS,
    TDM_URL,
    WileyExtractor,
)
from ingestion_workflow.models import (
    DownloadResult,
    DownloadSource,
    ExtractedContent,
    FileType,
    Identifier,
    Identifiers,
)
from ingestion_workflow.pipeline import Context, run_stages
from ingestion_workflow.pipeline.stages import DownloadStage, ExtractStage
from ingestion_workflow.tests import wiley_tdm

#: A 129 KB article Wiley serves.
SERVED_DOI = "10.1111/j.1440-1614.2006.01765.x"
#: A Wiley DOI naming no article.
MISSING_DOI = "10.1002/ingestion-workflow-no-such-article"
OTHER_DOI = "10.1016/j.neuroimage.2023.120"

CASSETTES = Path(__file__).parents[1] / "cassettes"


def _settings(tmp_path, **overrides) -> Settings:
    return Settings(**{
        "cache_root": tmp_path / "cache",
        "data_root": tmp_path / "data",
        "catalog_root": tmp_path / "catalog",
        "wiley_cache_root": tmp_path / "wiley",
        "pdf_cache_root": tmp_path / "pdfs",
        "wiley_api_key": wiley_tdm.token(),
        "download_sources": ["wiley"],
        **overrides,
    })


@pytest.fixture()
def wiley(tmp_path):
    """The Wiley source, with its sleeps recorded in `.slept` against a clock they advance."""
    extractor = WileyExtractor(_settings(tmp_path))
    extractor.slept = []
    now = [1000.0]

    def sleep(delay):
        extractor.slept.append(delay)
        now[0] += delay

    extractor._sleep = sleep
    extractor._clock = lambda: now[0]
    return extractor


def _download(extractor, *dois):
    return extractor.download(Identifiers([Identifier(doi=doi) for doi in dois]))


@pytest.mark.vcr(match_on=wiley_tdm.MATCH_ON)
def test_wiley_dois_are_fetched_with_the_token_and_others_are_never_asked(wiley, tmp_path, vcr):
    served, other, missing = _download(wiley, SERVED_DOI, OTHER_DOI, MISSING_DOI)

    assert served.success and served.source is DownloadSource.WILEY
    (pdf,) = served.files
    article_dir = tmp_path / "wiley" / safe_hash_stem(served.identifier.slug)
    assert pdf.file_path == article_dir / "article.pdf"
    assert (pdf.file_type, pdf.source) == (FileType.PDF, DownloadSource.WILEY)
    assert pdf.file_path.read_bytes().startswith(PDF_MAGIC)
    assert served.article_version == "publishedVersion"
    assert served.url_attempts == [{
        "url": TDM_URL + "10.1111%2Fj.1440-1614.2006.01765.x", "provider": "wiley",
        "status": 200, "outcome": "pdf", "reason": "PDF downloaded",
    }]

    assert (other.success, other.error_message, other.url_attempts) == (False, NOT_WILEY, [])

    assert not missing.success
    assert "404" in missing.error_message
    assert missing.url_attempts[0]["outcome"] == "http_error"
    assert missing.url_attempts[0]["dead"] is True

    # one request per Wiley article, the second paced as Wiley's terms ask
    assert vcr.all_played
    assert wiley.slept == [REQUEST_INTERVAL_SECONDS]


# Composed: Wiley does not answer 429 on demand.
@pytest.mark.vcr(match_on=wiley_tdm.MATCH_ON)
def test_a_429_is_waited_out_for_as_long_as_wiley_asks(wiley, vcr):
    (result,) = _download(wiley, SERVED_DOI)

    assert result.success
    assert wiley.slept == [30.0]
    assert vcr.all_played


# Composed: Wiley does not answer 429 on demand.
@pytest.mark.vcr(match_on=wiley_tdm.MATCH_ON)
def test_wiley_asking_for_longer_than_a_run_waits_leaves_it_for_the_run(wiley, vcr):
    first, second = _download(wiley, SERVED_DOI, MISSING_DOI)

    assert first.url_attempts[0]["outcome"] == "rate_limited"
    assert "asked to wait 3600s" in first.error_message
    assert second.url_attempts[0]["outcome"] == "deferred"
    assert wiley.slept == []
    assert vcr.play_count == 1


# Composed: Wiley does not serve a page in place of a PDF on demand.
@pytest.mark.vcr(match_on=wiley_tdm.MATCH_ON)
def test_an_answer_that_is_not_a_pdf_fails_and_is_never_kept(wiley, tmp_path, vcr):
    (result,) = _download(wiley, SERVED_DOI)

    assert not result.success
    assert result.url_attempts[0]["outcome"] == "not_pdf"
    assert result.error_message.endswith("is not a PDF.")
    assert list((tmp_path / "wiley").rglob("*.pdf")) == []
    assert vcr.all_played


@pytest.mark.vcr(record_mode="none")
def test_without_a_token_nothing_is_asked(tmp_path):
    extractor = WileyExtractor(_settings(tmp_path, wiley_api_key=None))

    with pytest.raises(ValueError, match="WILEY_API_KEY"):
        _download(extractor, SERVED_DOI)


def _catalog(settings, *dois):
    ctx = Context(settings, Catalog.open(settings.catalog_root))
    refs = ctx.catalog.register_many([Identifier(doi=doi) for doi in dois])
    return ctx, refs


def test_only_articles_with_a_wiley_doi_are_planned_for_wiley(tmp_path):
    settings = _settings(tmp_path)
    ctx, refs = _catalog(settings, SERVED_DOI, OTHER_DOI, "10.1113/jphysiol.2006.1")

    plan = DownloadStage(settings).plan(ctx, refs, {}, {})

    assert [work.ref.identifier.doi for work in plan.pending] == [
        SERVED_DOI, "10.1113/jphysiol.2006.1"]
    assert plan.skipped == 1


@pytest.mark.vcr(match_on=wiley_tdm.MATCH_ON)
def test_an_article_wiley_does_not_have_is_not_asked_for_again(tmp_path):
    settings = _settings(tmp_path)
    ctx, refs = _catalog(settings, MISSING_DOI)

    run_stages(ctx, [DownloadStage(settings)], refs)

    artifact = ctx.catalog.artifacts([refs[0].id], "download")[refs[0].id]["wiley"]
    assert artifact.status is Status.PERMANENT
    assert "404" in artifact.error
    assert artifact.summary["url_outcomes"] == {"http_error": 1}


def test_a_wiley_pdf_is_extracted_exactly_as_a_pdf_source_download_is(tmp_path):
    """Download through the stage from a recorded answer, extract through the stage, and
    compare with the same file extracted as a `pdf` source download."""
    settings = _settings(tmp_path, pdf_extract_workers=1)
    ctx, refs = _catalog(settings, SERVED_DOI)
    recorder = vcrpy.VCR(match_on=wiley_tdm.MATCH_ON, record_mode="none")
    recorder.register_matcher("wiley_token", wiley_tdm.same_token)
    cassette = CASSETTES / (
        "test_wiley_dois_are_fetched_with_the_token_and_others_are_never_asked.yaml")
    # Only the download is replayed: Docling's model files are not Wiley's to answer.
    with recorder.use_cassette(str(cassette)):
        run_stages(ctx, [DownloadStage(settings)], refs)
    run_stages(ctx, [ExtractStage(settings)], refs)

    artifact = ctx.catalog.artifacts([refs[0].id], "extract")[refs[0].id]["wiley"]
    assert artifact.status is Status.OK, artifact.error
    wiley = ExtractedContent.from_dict(ctx.catalog.payload(artifact))
    assert wiley.source is DownloadSource.WILEY
    assert wiley.full_text_path.parent.parent == tmp_path / "data" / "extractions" / "wiley"

    download = DownloadResult.from_dict(
        ctx.catalog.payload(ctx.catalog.artifacts([refs[0].id], "download")[refs[0].id]["wiley"]))
    identifier = download.identifier
    as_pdf = DownloadResult(identifier, DownloadSource.PDF, True, files=[store_pdf(
        tmp_path / "pdfs", identifier, download.files[0].file_path.read_bytes(),
        DownloadSource.PDF)])
    (pdf,) = PdfExtractor(settings).extract([as_pdf])

    # the article has two tables, one of them a table of peaks
    assert [len(t.coordinates) > 0 for t in wiley.tables] == [False, True]
    assert pdf.full_text_path.read_text() == wiley.full_text_path.read_text()
    assert [t.to_dict() | {"raw_content_path": None} for t in wiley.tables] == [
        t.to_dict() | {"raw_content_path": None} for t in pdf.tables]
    assert wiley.figure_captions == pdf.figure_captions
    assert artifact.summary["text_sha256"]
    assert artifact.summary["tables"] == len(pdf.tables)
