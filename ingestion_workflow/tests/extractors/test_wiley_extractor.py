"""The Wiley TDM source: what it asks Wiley for, what it keeps, and how its PDFs are read."""

from __future__ import annotations

import json
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path

import pytest
import vcr as vcrpy
from ingestion_workflow.catalog import Catalog, Outcome, Status
from ingestion_workflow.config import Settings
from ingestion_workflow.extractors import wiley_extractor
from ingestion_workflow.extractors.base import SourceMisconfigured
from ingestion_workflow.extractors.copies import copy_of
from ingestion_workflow.extractors.pdf_extractor import PDF_MAGIC, _HostGate, store_pdf
from ingestion_workflow.extractors.utils import safe_hash_stem
from ingestion_workflow.extractors.wiley_extractor import TDM_URL, WileyExtractor
from ingestion_workflow.models import (
    CopyAccess,
    CopyRoute,
    CopyVersion,
    DownloadCopy,
    DownloadResult,
    DownloadSource,
    ExtractedContent,
    FileType,
    Identifier,
    Identifiers,
)
from ingestion_workflow.pipeline import Context, run_stages
from ingestion_workflow.pipeline.stages import DownloadStage, ExtractStage
from ingestion_workflow.pipeline.stages.extract import (
    EXTRACTOR_VERSIONS,
    current_extractions,
    extraction_fingerprint,
    made_from,
)
from ingestion_workflow.pipeline.stages.triage import judged_extraction
from ingestion_workflow.tests import wiley_tdm

#: A 129 KB article Wiley serves.
SERVED_DOI = "10.1111/j.1440-1614.2006.01765.x"
#: A Wiley DOI naming no article.
MISSING_DOI = "10.1002/ingestion-workflow-no-such-article"
OTHER_DOI = "10.1016/j.neuroimage.2023.120"

CASSETTES = Path(__file__).parents[1] / "cassettes"
SERVED = CASSETTES / "test_wiley_dois_are_fetched_with_the_token.yaml"
MISSING = CASSETTES / "test_an_article_wiley_does_not_have_is_not_asked_for_again.yaml"


def _settings(tmp_path, **overrides) -> Settings:
    return Settings(**{
        "cache_root": tmp_path / "cache",
        "data_root": tmp_path / "data",
        "catalog_root": tmp_path / "catalog",
        "wiley_api_key": wiley_tdm.token(),
        "download_sources": ["wiley"],
        **overrides,
    })


@pytest.fixture()
def slept(monkeypatch):
    """A fresh run's Wiley gate, on a clock only its sleeps advance; the sleeps it took."""
    now, slept = [1000.0], []

    def sleep(delay):
        slept.append(delay)
        now[0] += delay

    gate = _HostGate(lambda: now[0], sleep, min_interval=wiley_extractor.GATE._min_interval)
    monkeypatch.setattr(wiley_extractor, "GATE", gate)
    return slept


@pytest.fixture()
def wiley(tmp_path, slept):
    return WileyExtractor(_settings(tmp_path))


@contextmanager
def _replay(cassette, **options):
    recorder = vcrpy.VCR(match_on=wiley_tdm.MATCH_ON, record_mode="none")
    recorder.register_matcher("wiley_token", wiley_tdm.same_token)
    with recorder.use_cassette(str(cassette), **options) as played:
        yield played


def _download(extractor, *dois):
    return extractor.download(Identifiers([Identifier(doi=doi) for doi in dois]))


def _catalog(settings, *identifiers):
    ctx = Context(settings, Catalog.open(settings.catalog_root))
    refs = ctx.catalog.register_many(
        [i if isinstance(i, Identifier) else Identifier(doi=i) for i in identifiers])
    return ctx, refs


def _assert_token_kept_out(ctx, refs):
    """Not in any error, attempt, summary or payload the catalog keeps."""
    token = wiley_tdm.token()
    ids = [ref.id for ref in refs]
    for stage in ("download", "extract"):
        for by_source in ctx.catalog.artifacts(ids, stage).values():
            for artifact in by_source.values():
                kept = [artifact.error, artifact.summary, ctx.catalog.payload(artifact)]
                assert token not in json.dumps(kept, default=str)


@pytest.mark.vcr(match_on=wiley_tdm.MATCH_ON)
def test_wiley_dois_are_fetched_with_the_token(wiley, slept, tmp_path, vcr):
    served, missing = _download(wiley, SERVED_DOI, MISSING_DOI)

    assert served.success and served.source is DownloadSource.WILEY
    (pdf,) = served.files
    # its own root, so it never overwrites the article's pdf-source download
    article_dir = tmp_path / "cache" / "wiley" / safe_hash_stem(served.identifier.slug)
    assert pdf.file_path == article_dir / "article.pdf"
    assert not (tmp_path / "cache" / "pdf").exists()
    assert (pdf.file_type, pdf.source) == (FileType.PDF, DownloadSource.WILEY)
    assert pdf.file_path.read_bytes().startswith(PDF_MAGIC)
    assert copy_of(served) == DownloadCopy(
        route=CopyRoute.PUBLISHER, version=CopyVersion.VOR, access=CopyAccess.TDM_GRANT,
        url=TDM_URL + "10.1111%2Fj.1440-1614.2006.01765.x")
    assert served.url_attempts == [{
        "url": TDM_URL + "10.1111%2Fj.1440-1614.2006.01765.x", "provider": "wiley",
        "status": 200, "outcome": "pdf", "reason": "PDF downloaded",
    }]

    assert not missing.success
    assert "404" in missing.error_message
    assert missing.url_attempts[0]["outcome"] == "http_error"
    assert missing.url_attempts[0]["dead"] is True

    # one request per article, the second ten seconds after the first, as Wiley's terms ask
    assert vcr.all_played
    assert slept == [10.0]
    for result in (served, missing):
        assert wiley_tdm.token() not in json.dumps([result.error_message, result.url_attempts])


def test_wiley_is_asked_once_every_ten_seconds_across_calls_and_extractors(tmp_path, slept):
    first, second = WileyExtractor(_settings(tmp_path)), WileyExtractor(_settings(tmp_path))

    with _replay(MISSING, allow_playback_repeats=True) as played:
        _download(first, MISSING_DOI)
        _download(first, MISSING_DOI)
        _download(second, MISSING_DOI)

    assert played.play_count == 3
    assert slept == [10.0, 10.0]


def test_articles_wiley_does_not_have_never_leave_wiley_for_the_run(wiley, slept):
    """A 404 is the article's answer, not Wiley refusing this client: the 21st is asked too."""
    with _replay(MISSING, allow_playback_repeats=True) as played:
        results = _download(wiley, *[MISSING_DOI] * 21)

    assert played.play_count == 21
    assert {r.url_attempts[0]["outcome"] for r in results} == {"http_error"}


# Composed: Wiley does not answer 429 on demand.
@pytest.mark.vcr(match_on=wiley_tdm.MATCH_ON)
def test_a_429_is_waited_out_for_as_long_as_wiley_asks(wiley, slept, vcr):
    (result,) = _download(wiley, SERVED_DOI)

    assert result.success
    assert slept == [30.0]
    assert vcr.all_played


# Composed: Wiley does not answer 429 on demand.
@pytest.mark.vcr(match_on=wiley_tdm.MATCH_ON)
def test_wiley_asking_for_longer_than_a_run_waits_leaves_it_for_the_run(wiley, slept, vcr):
    first, second = _download(wiley, SERVED_DOI, MISSING_DOI)

    assert first.url_attempts[0]["outcome"] == "rate_limited"
    assert "asked to wait 3600s" in first.error_message
    assert second.url_attempts[0]["outcome"] == "deferred"
    assert slept == []
    assert vcr.play_count == 1


# Composed: Wiley does not serve a page in place of a PDF on demand.
@pytest.mark.vcr(match_on=wiley_tdm.MATCH_ON)
def test_an_answer_that_is_not_a_pdf_fails_and_is_never_kept(wiley, tmp_path, vcr):
    (result,) = _download(wiley, SERVED_DOI)

    assert not result.success
    assert result.url_attempts[0]["outcome"] == "not_pdf"
    assert result.error_message.endswith("is not a PDF.")
    assert list((tmp_path / "cache").rglob("*.pdf")) == []
    assert vcr.all_played


@pytest.mark.vcr(record_mode="none")
def test_without_a_token_the_stage_stops_before_asking_or_charging_anyone(tmp_path, caplog):
    settings = _settings(tmp_path, wiley_api_key=None)
    ctx, refs = _catalog(settings, SERVED_DOI)

    with pytest.raises(SourceMisconfigured, match="WILEY_API_KEY"):
        WileyExtractor(settings).download(Identifiers([refs[0].identifier]))
    run_stages(ctx, [DownloadStage(settings)], refs)

    assert "download stopped: wiley: no Wiley TDM token: set WILEY_API_KEY" in caplog.text
    assert not any(ctx.catalog.artifacts([refs[0].id], "download").values())
    assert ctx.catalog.attempt_counts([refs[0].id], "download", "wiley") == {}


# The cassette is composed (see its header).
@pytest.mark.vcr(match_on=wiley_tdm.MATCH_ON)
def test_a_refused_token_stops_the_download_stage(tmp_path, slept, vcr):
    """Wiley refusing the token is not the articles' failure: none is charged a try,
    and the stage asks nothing more this run."""
    settings = _settings(tmp_path)
    ctx, refs = _catalog(settings, SERVED_DOI, MISSING_DOI)
    stage = DownloadStage(settings)

    run_stages(ctx, [stage], refs)
    run_stages(ctx, [stage], refs)

    assert vcr.play_count == 1
    ids = [ref.id for ref in refs]
    assert not any(ctx.catalog.artifacts(ids, "download").values())
    assert ctx.catalog.attempt_counts(ids, "download", "wiley") == {}
    assert len(DownloadStage(settings).plan(ctx, refs, {}, {}).pending) == 2


@pytest.mark.vcr(record_mode="none")
def test_only_articles_with_a_wiley_doi_are_planned_for_wiley(tmp_path):
    settings = _settings(tmp_path)
    ctx, refs = _catalog(
        settings, SERVED_DOI, OTHER_DOI, "10.1113/jphysiol.2006.1", Identifier(pmid="16504543"))

    plan = DownloadStage(settings).plan(ctx, refs, {}, {})

    assert [work.ref.identifier.doi for work in plan.pending] == [
        SERVED_DOI, "10.1113/jphysiol.2006.1"]
    assert plan.skipped == 2

    # neither the other publisher's article nor the one with no DOI is asked for
    run_stages(ctx, [DownloadStage(settings)], [refs[1], refs[3]])
    assert not any(ctx.catalog.artifacts([refs[1].id, refs[3].id], "download").values())


@pytest.mark.vcr(match_on=wiley_tdm.MATCH_ON)
def test_an_article_wiley_does_not_have_is_not_asked_for_again(tmp_path, slept):
    settings = _settings(tmp_path)
    ctx, refs = _catalog(settings, MISSING_DOI)

    run_stages(ctx, [DownloadStage(settings)], refs)

    artifact = ctx.catalog.artifacts([refs[0].id], "download")[refs[0].id]["wiley"]
    assert artifact.status is Status.PERMANENT
    assert "404" in artifact.error
    assert artifact.summary["url_outcomes"] == {"http_error": 1}
    _assert_token_kept_out(ctx, refs)


def _downloaded(ctx, ref, source: DownloadSource, payload: bytes, status=Status.OK):
    """Record a download of `source` holding `payload` as its PDF."""
    settings = ctx.settings
    files = [store_pdf(settings.get_cache_dir(source.value), ref.identifier, payload, source)]
    result = DownloadResult(ref.identifier, source, True, files=files)
    ctx.catalog.record([Outcome(
        ref.id, "download", source.value, status,
        fingerprint=DownloadStage(settings).fingerprint_for(source), payload=result.to_dict())])


def test_a_wiley_pdf_is_read_into_the_articles_pdf_text(tmp_path, slept):
    """Download through the stage from a recorded answer, extract through the stage: the
    text is the `pdf` one, which is all downstream (corpus, parse, triage) knows of."""
    settings = _settings(tmp_path, pdf_extract_workers=1)
    ctx, refs = _catalog(settings, SERVED_DOI)
    # Only the download is replayed: Docling's model files are not Wiley's to answer.
    with _replay(SERVED):
        run_stages(ctx, [DownloadStage(settings)], refs)
    run_stages(ctx, [ExtractStage(settings)], refs)

    extractions = ctx.catalog.artifacts([refs[0].id], "extract")[refs[0].id]
    assert list(extractions) == ["pdf"]
    artifact = extractions["pdf"]
    assert artifact.status is Status.OK, artifact.error
    assert artifact.summary["download_source"] == "wiley"
    content = ExtractedContent.from_dict(ctx.catalog.payload(artifact))
    assert content.source is DownloadSource.PDF
    assert content.full_text_path.parent.parent == tmp_path / "data" / "extractions" / "pdf"
    # the article has two tables, one of them a table of peaks
    assert [len(t.coordinates) > 0 for t in content.tables] == [False, True]
    assert artifact.summary["text_sha256"]
    assert artifact.summary["tables"] == 2
    _assert_token_kept_out(ctx, refs)


def test_a_wiley_pdf_that_cannot_be_read_fails_as_the_pdf_text_and_names_its_download(tmp_path):
    settings = _settings(tmp_path, pdf_extract_workers=1)
    ctx, refs = _catalog(settings, SERVED_DOI)
    _downloaded(ctx, refs[0], DownloadSource.WILEY, PDF_MAGIC + b"1.4 truncated")

    run_stages(ctx, [ExtractStage(settings)], refs)

    (artifact,) = ctx.catalog.artifacts([refs[0].id], "extract")[refs[0].id].values()
    assert (artifact.source, artifact.status) == ("pdf", Status.FAILED)
    assert artifact.summary["download_source"] == "wiley"
    assert artifact.error


def test_an_article_with_a_wiley_and_an_open_access_pdf_gets_its_text_from_wileys(tmp_path):
    settings = _settings(tmp_path, download_sources=["pdf", "wiley"])
    ctx, refs = _catalog(settings, SERVED_DOI)
    ref = refs[0]
    _downloaded(ctx, ref, DownloadSource.PDF, PDF_MAGIC + b" open-access copy")
    downloads = ctx.catalog.artifacts([ref.id], "download")
    stage = ExtractStage(settings)
    (work,) = stage.plan(ctx, refs, {}, downloads).pending
    assert (work.source, work.upstream.source) == ("pdf", "pdf")
    # read from the open-access copy, as the stage records it
    ctx.catalog.record([Outcome(ref.id, "extract", "pdf", Status.OK,
                                fingerprint=work.fingerprint,
                                summary={})])
    assert stage.plan(ctx, refs, ctx.catalog.artifacts([ref.id], "extract"),
                      downloads).fresh == 1

    _downloaded(ctx, ref, DownloadSource.WILEY, PDF_MAGIC + b" version of record")
    downloads = ctx.catalog.artifacts([ref.id], "download")
    (work,) = stage.plan(ctx, refs, ctx.catalog.artifacts([ref.id], "extract"),
                         downloads).pending
    assert (work.source, work.upstream.source) == ("pdf", "wiley")

    # a Wiley download that is not OK leaves the text to the open-access copy
    _downloaded(ctx, ref, DownloadSource.WILEY, PDF_MAGIC, status=Status.FAILED)
    downloads = ctx.catalog.artifacts([ref.id], "download")
    extractions = ctx.catalog.artifacts([ref.id], "extract")
    assert stage.plan(ctx, refs, extractions, downloads).fresh == 1
    assert list(current_extractions(ctx, extractions[ref.id], downloads[ref.id])) == ["pdf"]


def test_a_wiley_pdfs_text_moves_with_the_pdf_extractor_version(tmp_path, monkeypatch):
    assert EXTRACTOR_VERSIONS["wiley"] == EXTRACTOR_VERSIONS["pdf"]
    settings = _settings(tmp_path)
    ctx, refs = _catalog(settings, SERVED_DOI)
    ref = refs[0]
    _downloaded(ctx, ref, DownloadSource.WILEY, PDF_MAGIC + b" version of record")
    downloads = ctx.catalog.artifacts([ref.id], "download")
    stage = ExtractStage(settings)
    (work,) = stage.plan(ctx, refs, {}, downloads).pending
    ctx.catalog.record([Outcome(ref.id, "extract", "pdf", Status.OK,
                                fingerprint=work.fingerprint,
                                summary={"download_source": "wiley"})])
    extractions = ctx.catalog.artifacts([ref.id], "extract")

    monkeypatch.setitem(EXTRACTOR_VERSIONS, "elsevier", EXTRACTOR_VERSIONS["elsevier"] + 1)
    assert stage.plan(ctx, refs, extractions, downloads).fresh == 1
    monkeypatch.setitem(EXTRACTOR_VERSIONS, "wiley", EXTRACTOR_VERSIONS["wiley"] + 1)
    assert [w.source for w in stage.plan(ctx, refs, extractions, downloads).pending] == ["pdf"]


def test_a_pdf_text_read_from_wileys_download_goes_with_that_download(tmp_path):
    """Whose download a page that is not the article is decides whether the text
    read from it is still the article's."""
    settings = _settings(tmp_path, download_sources=["pdf", "wiley"])
    ctx, refs = _catalog(settings, SERVED_DOI)
    ref = refs[0]
    _downloaded(ctx, ref, DownloadSource.PDF, PDF_MAGIC + b" open-access copy",
                status=Status.FAILED)
    _downloaded(ctx, ref, DownloadSource.WILEY, PDF_MAGIC + b" version of record")
    downloads = ctx.catalog.artifacts([ref.id], "download")[ref.id]
    ctx.catalog.record([Outcome(ref.id, "extract", "pdf", Status.OK,
                                fingerprint=extraction_fingerprint("wiley", downloads["wiley"]),
                                summary={"download_source": "wiley", "tables": 1})])
    extractions = ctx.catalog.artifacts([ref.id], "extract")[ref.id]

    assert made_from(extractions, "wiley") is extractions["pdf"]
    assert made_from(extractions, "pdf") is None
    assert judged_extraction(ctx, extractions, downloads) == extractions["pdf"]

    bad = replace(downloads["wiley"], status=Status.FAILED, summary={"bad_page": "paywall"})
    assert judged_extraction(ctx, extractions, {**downloads, "wiley": bad}) is None
