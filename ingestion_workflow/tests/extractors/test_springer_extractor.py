"""The Springer Nature source: what it asks the Open Access API for, what it keeps, and
how its JATS is read."""

from __future__ import annotations

import json
import os
import re
from datetime import timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest
import vcr as vcrpy
import yaml
from ingestion_workflow.catalog import Catalog, Status
from ingestion_workflow.config import Settings
from ingestion_workflow.extractors import springer_extractor
from ingestion_workflow.extractors.base import SourceExhausted, SourceMisconfigured
from ingestion_workflow.extractors.figure_captions import captions_digest
from ingestion_workflow.extractors.pdf_extractor import _HostGate
from ingestion_workflow.extractors.springer_extractor import (
    NO_BODY,
    NOT_OPEN_ACCESS,
    SpringerExtractor,
    doi_query,
)
from ingestion_workflow.extractors.utils import safe_hash_stem
from ingestion_workflow.models import (
    DownloadSource,
    ExtractedContent,
    FileType,
    Identifier,
    Identifiers,
)
from ingestion_workflow.pipeline import Context, run_stages
from ingestion_workflow.pipeline.stages import DownloadStage, ExtractStage

#: BMC Geriatrics, CC BY: a case report with two tables and three figures.
CASE_REPORT = "10.1186/s12877-026-07992-9"
#: Molecular Pain, CC BY.
MOLECULAR_PAIN = "10.1186/1744-8069-9-43"
#: Machine Learning, 2001, not open access: its ':' fails an unquoted query.
KLUWER = "10.1023/A:1010933404324"
#: Subscription articles, none open access.
CLOSED = [
    KLUWER,
    "10.1007/s11682-013-9285-5",
    "10.1007/s00429-014-0782-0",
    "10.1038/npp.2008.93",
    "10.1007/s00221-007-1124-8",
    "10.1038/npp.2014.169",
    "10.1007/s00429-012-0484-4",
    "10.1038/npp.2012.249",
    "10.1007/s00221-013-3575-4",
    "10.1007/s11682-016-9635-1",
    "10.1007/s00213-013-3338-8",
    "10.1007/s00115-009-2827-9",
    "10.1038/nn1263",
    "10.1007/s00213-007-0797-9",
    "10.1007/s00482-018-0301-y",
    "10.1007/s11682-014-9293-0",
    "10.1007/s00221-009-1942-y",
    "10.1007/s10548-014-0404-4",
]
#: The 21st DOI, so a second query names it alone; not open access either.
LAST = "10.1007/s002210000402"
TWENTY_ONE = [CASE_REPORT, MOLECULAR_PAIN, *CLOSED, LAST]
OTHER_DOI = "10.1016/j.neuroimage.2023.120"

CASSETTES = Path(__file__).parents[1] / "cassettes"
TWO_QUERIES = CASSETTES / "test_springer_dois_are_asked_twenty_to_a_quoted_query.yaml"
FIVE_BMC = CASSETTES / "springer_five_bmc_articles_one_without_its_body.yaml"
#: Five BMC articles of 2026 asked in one query; the API served the first without a body.
BODILESS = "10.1186/s12888-026-08282-9"
FIVE = [BODILESS, "10.1186/s12888-026-08418-x", "10.1186/s13195-026-02139-9", CASE_REPORT,
        "10.1186/s13195-026-02115-3"]
MATCH_ON = ("method", "scheme", "host", "port", "path", "query")


def _settings(tmp_path, **overrides) -> Settings:
    return Settings(**{
        "cache_root": tmp_path / "cache",
        "data_root": tmp_path / "data",
        "catalog_root": tmp_path / "catalog",
        # the cassettes keep no key: `api_key` is filtered from every query
        "springer_api_key": os.environ.get("SPRINGER_API_KEY") or "replay-key",
        "download_sources": ["springer"],
        **overrides,
    })


@pytest.fixture()
def slept(monkeypatch):
    """A fresh run's Springer gate, on a clock only its sleeps advance; the sleeps it took."""
    now, slept = [1000.0], []

    def sleep(delay):
        slept.append(delay)
        now[0] += delay

    gate = _HostGate(lambda: now[0], sleep, min_interval=springer_extractor.GATE._min_interval)
    monkeypatch.setattr(springer_extractor, "GATE", gate)
    return slept


def _catalog(settings, *identifiers):
    ctx = Context(settings, Catalog.open(settings.catalog_root))
    refs = ctx.catalog.register_many(
        [i if isinstance(i, Identifier) else Identifier(doi=i) for i in identifiers])
    return ctx, refs


def _replay(cassette):
    """Replay outside pytest-recording, with its key filter."""
    recorder = vcrpy.VCR(match_on=MATCH_ON, record_mode="none",
                         filter_query_parameters=["api_key"])
    return recorder.use_cassette(str(cassette))


def _queries(cassette) -> list:
    return [parse_qs(urlsplit(request.uri).query) for request in cassette.requests]


def _composed(tmp_path, *responses) -> Path:
    """The two-query cassette with each later answer replaced by one of `responses`
    (status, headers), as the API gives neither on demand."""
    recorded = yaml.safe_load(TWO_QUERIES.read_text(encoding="utf-8"))
    first, second = recorded["interactions"]
    interactions = [first]
    for status, headers in responses:
        interactions.append({
            "request": second["request"],
            "response": {"body": {"string": ""}, "headers": headers,
                         "status": {"code": status, "message": ""}},
        })
    interactions.append(second)
    path = tmp_path / "composed.yaml"
    path.write_text(yaml.safe_dump({**recorded, "interactions": interactions}), encoding="utf-8")
    return path


def _assert_key_kept_out(ctx, refs, key):
    """Not in any error, summary or payload the catalog keeps."""
    ids = [ref.id for ref in refs]
    for stage in ("download", "extract"):
        for by_source in ctx.catalog.artifacts(ids, stage).values():
            for artifact in by_source.values():
                kept = [artifact.error, artifact.summary, ctx.catalog.payload(artifact)]
                assert key not in json.dumps(kept, default=str)


def test_the_query_quotes_every_doi():
    assert doi_query([CASE_REPORT, KLUWER]) == (
        'doi:"10.1186/s12877-026-07992-9" OR doi:"10.1023/A:1010933404324"')


@pytest.mark.vcr(match_on=MATCH_ON)
def test_springer_dois_are_asked_twenty_to_a_quoted_query(tmp_path, slept, vcr):
    results = SpringerExtractor(_settings(tmp_path)).download(
        Identifiers([Identifier(doi=doi) for doi in TWENTY_ONE]))

    first, second = _queries(vcr)
    assert first["q"] == [doi_query(TWENTY_ONE[:20])]
    assert first["p"] == ["20"]
    assert second["q"] == [doi_query([LAST])]
    assert "api_key" not in first
    assert vcr.all_played
    # 100 requests a minute
    assert slept == [pytest.approx(0.6)]

    by_doi = {result.identifier.doi.lower(): result for result in results}
    assert len(by_doi) == 21
    for doi in (CASE_REPORT, MOLECULAR_PAIN):
        served = by_doi[doi.lower()]
        assert served.success and served.source is DownloadSource.SPRINGER
        assert served.article_version == "publishedVersion"
        article_dir = tmp_path / "cache" / "springer" / safe_hash_stem(served.identifier.slug)
        names = {f.file_path.relative_to(article_dir).as_posix(): f.file_type
                 for f in served.files}
        assert names["article.xml"] is FileType.XML
        assert names["tables/tables.xml"] is FileType.XML
    case_files = {f.file_path.name for f in by_doi[CASE_REPORT].files}
    assert {"table_000.csv", "table_001.csv"} <= case_files
    # the article named alone in its query was answered 404, the others not returned
    for doi in [*CLOSED, LAST]:
        result = by_doi[doi.lower()]
        assert (result.success, result.error_message) == (False, NOT_OPEN_ACCESS)


def test_articles_springer_does_not_serve_are_not_asked_for_again(tmp_path, slept):
    settings = _settings(tmp_path)
    ctx, refs = _catalog(settings, *TWENTY_ONE)
    with _replay(TWO_QUERIES):
        run_stages(ctx, [DownloadStage(settings)], refs)

    found = ctx.catalog.artifacts([ref.id for ref in refs], "download")
    status = {ref.identifier.doi.lower(): found[ref.id]["springer"].status for ref in refs}
    assert status.pop(CASE_REPORT) is Status.OK
    assert status.pop(MOLECULAR_PAIN) is Status.OK
    assert set(status.values()) == {Status.PERMANENT}
    assert DownloadStage(settings).plan(ctx, refs, found, {}).pending == []
    _assert_key_kept_out(ctx, refs, settings.springer_api_key)


def test_a_springer_article_is_read_into_its_springer_text(tmp_path, slept):
    """Download through the stage from the recorded answer, extract through the stage:
    pubget's code reads the JATS, sections, tables and figure captions."""
    settings = _settings(tmp_path)
    ctx, refs = _catalog(settings, *TWENTY_ONE)
    with _replay(TWO_QUERIES):
        run_stages(ctx, [DownloadStage(settings)], refs)
    case_report = next(ref for ref in refs if ref.identifier.doi.lower() == CASE_REPORT)

    run_stages(ctx, [ExtractStage(settings)], [case_report])

    extractions = ctx.catalog.artifacts([case_report.id], "extract")[case_report.id]
    assert list(extractions) == ["springer"]
    artifact = extractions["springer"]
    assert artifact.status is Status.OK, artifact.error
    content = ExtractedContent.from_dict(ctx.catalog.payload(artifact))
    assert content.source is DownloadSource.SPRINGER
    text = content.full_text_path.read_text(encoding="utf-8")
    assert text.startswith("A case report of potentially inappropriate medication-induced lead")
    # the abstract's sections, then the body's, then the legends
    assert re.findall(r"(?m)^## (.+?) ?$", text) == [
        "Background", "Case presentation", "Conclusions", "Background", "Case report",
        "Discussion", "Conclusion",
        "Figure legends"]

    assert [t.table_number for t in content.tables] == [1, 2]
    assert content.tables[0].caption.startswith("Baseline and post-chelation laboratory")
    assert content.tables[1].caption.startswith("Blood and urine lead levels")
    assert artifact.summary["tables"] == 2

    captions = content.figure_captions
    assert [c["ids"] for c in captions] == [["Fig1"], ["Fig2"], ["Fig3"]]
    starts = ("T2/FLAIR images", "PET-CT revealed", "Clinical algorithm")
    for caption, start in zip(captions, starts):
        lo, hi = caption["span"]
        assert start in text[lo:hi]
    assert artifact.summary["captions_sha256"] == captions_digest(captions)


@pytest.mark.vcr(match_on=MATCH_ON)
def test_a_refused_key_stops_the_download_stage(tmp_path, slept, vcr):
    """The API refusing the key is not the articles' failure: none is charged a try,
    and the stage asks nothing more this run."""
    settings = _settings(tmp_path, springer_api_key="not-a-springer-key")
    ctx, refs = _catalog(settings, CASE_REPORT, LAST)
    stage = DownloadStage(settings)

    run_stages(ctx, [stage], refs)
    run_stages(ctx, [stage], refs)

    assert vcr.play_count == 1
    ids = [ref.id for ref in refs]
    assert not any(ctx.catalog.artifacts(ids, "download").values())
    assert ctx.catalog.attempt_counts(ids, "download", "springer") == {}
    assert len(DownloadStage(settings).plan(ctx, refs, {}, {}).pending) == 2


@pytest.mark.vcr(record_mode="none")
def test_without_a_key_the_stage_stops_before_asking_or_charging_anyone(tmp_path, caplog):
    settings = _settings(tmp_path, springer_api_key=None)
    ctx, refs = _catalog(settings, CASE_REPORT)

    with pytest.raises(SourceMisconfigured, match="SPRINGER_API_KEY"):
        SpringerExtractor(settings).download(Identifiers([refs[0].identifier]))
    run_stages(ctx, [DownloadStage(settings)], refs)

    assert "download stopped: springer: no Springer Nature API key" in caplog.text
    assert ctx.catalog.attempt_counts([refs[0].id], "download", "springer") == {}


def test_a_429_is_waited_out_for_as_long_as_springer_asks(tmp_path, slept):
    cassette = _composed(tmp_path, (429, {"retry-after": ["30"]}))
    extractor = SpringerExtractor(_settings(tmp_path))

    with _replay(cassette) as played:
        results = extractor.download(Identifiers([Identifier(doi=doi) for doi in TWENTY_ONE]))

    assert played.all_played
    assert slept == [pytest.approx(0.6), pytest.approx(30.0)]
    assert sum(result.success for result in results) == 2


def test_a_spent_quota_leaves_the_rest_pending_and_springer_for_the_run(tmp_path, slept, caplog):
    """A 429 asking for longer than a run waits is the day's quota: the articles already
    answered are recorded, and the rest are charged nothing and not asked again this run."""
    cassette = _composed(tmp_path, (429, {"retry-after": ["86400"]}))
    settings = _settings(tmp_path)
    ctx, refs = _catalog(settings, *TWENTY_ONE)
    stage = DownloadStage(settings)
    last = next(ref for ref in refs if ref.identifier.doi.lower() == LAST.lower())

    with _replay(cassette) as played:
        run_stages(ctx, [stage], refs)
        assert played.play_count == 2
        run_stages(ctx, [stage], [last])
        assert played.play_count == 2

    found = ctx.catalog.artifacts([ref.id for ref in refs], "download")
    assert sum("springer" in found.get(ref.id, {}) for ref in refs) == 20
    assert "springer" not in found.get(last.id, {})
    assert ctx.catalog.attempt_counts([last.id], "download", "springer") == {}
    assert "download[springer] stopped for this run" in caplog.text
    (pending,) = DownloadStage(settings).plan(ctx, [last], found, {}).pending
    assert pending.source == "springer"


def test_a_spent_quota_raises_with_the_articles_already_answered(tmp_path, slept):
    cassette = _composed(tmp_path, (429, {"retry-after": ["86400"]}))
    extractor = SpringerExtractor(_settings(tmp_path))

    with _replay(cassette):
        with pytest.raises(SourceExhausted, match="asked to wait 86400s") as raised:
            extractor.download(Identifiers([Identifier(doi=doi) for doi in TWENTY_ONE]))

    assert len(raised.value.results) == 20


@pytest.mark.vcr(record_mode="none")
def test_only_articles_with_a_springer_nature_doi_are_planned_for_springer(tmp_path):
    settings = _settings(tmp_path)
    ctx, refs = _catalog(settings, CASE_REPORT, OTHER_DOI, KLUWER, Identifier(pmid="16504543"))

    plan = DownloadStage(settings).plan(ctx, refs, {}, {})

    assert [work.ref.identifier.doi.lower() for work in plan.pending] == [
        CASE_REPORT, KLUWER.lower()]
    assert plan.skipped == 2


def test_an_article_served_without_its_body_is_not_a_download_and_is_asked_again(tmp_path, slept):
    """The API's record of it is front matter only: no text, so not this source's
    full text, and not known to be final either."""
    settings = _settings(tmp_path)
    ctx, refs = _catalog(settings, *FIVE)
    with _replay(FIVE_BMC) as played:
        run_stages(ctx, [DownloadStage(settings)], refs)
        assert played.play_count == 1

    found = ctx.catalog.artifacts([ref.id for ref in refs], "download")
    by_doi = {ref.identifier.doi.lower(): found[ref.id]["springer"] for ref in refs}
    bodiless = by_doi.pop(BODILESS)
    assert (bodiless.status, bodiless.error) == (Status.FAILED, NO_BODY)
    assert {a.status for a in by_doi.values()} == {Status.OK}
    later = Context(settings, ctx.catalog, retry_after=timedelta(0))
    assert [w.ref.identifier.doi.lower() for w in
            DownloadStage(settings).plan(later, refs, found, {}).pending] == [BODILESS]
