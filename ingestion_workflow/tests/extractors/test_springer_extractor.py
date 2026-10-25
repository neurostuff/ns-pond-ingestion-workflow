"""The Springer Nature source: what it asks the Open Access API for, what it keeps, and
how its JATS is read."""

from __future__ import annotations

import gzip
import json
import os
import re
import socket
from datetime import timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlsplit

import pytest
import vcr as vcrpy
import yaml
from ingestion_workflow.catalog import Catalog, Outcome, Status
from ingestion_workflow.config import Settings
from ingestion_workflow.extractors import springer_extractor
from ingestion_workflow.extractors.base import SourceExhausted, SourceMisconfigured
from ingestion_workflow.extractors.copies import copy_of
from ingestion_workflow.extractors.figure_captions import captions_digest
from ingestion_workflow.extractors.pdf_extractor import _HostGate
from ingestion_workflow.extractors.springer_extractor import (
    NO_BODY,
    NOT_OPEN_ACCESS,
    QUERY_REFUSED,
    SpringerExtractor,
    doi_query,
    query_doi,
)
from ingestion_workflow.extractors.utils import safe_hash_stem
from ingestion_workflow.models import (
    DownloadCopy,
    DownloadSource,
    ExtractedContent,
    FileType,
    Identifier,
    Identifiers,
)
from ingestion_workflow.pipeline import Context, run_stages
from ingestion_workflow.pipeline.stages import DownloadStage, ExtractStage
from ingestion_workflow.pipeline.stages.extract import EXTRACTOR_VERSIONS
from ingestion_workflow.pipeline.stages.triage import TIE_ORDER
from ingestion_workflow.services.citations import JATS_SOURCES
from ingestion_workflow.services.metadata import MetadataService
from lxml import etree
from study_schema.models.paper_parse import CopyAccess, CopyRoute, CopyVersion

#: BMC Geriatrics, CC BY: a case report with two tables and three figures.
CASE_REPORT = "10.1186/s12877-026-07992-9"
#: Molecular Pain, CC BY.
MOLECULAR_PAIN = "10.1186/1744-8069-9-43"
#: Machine Learning, 2001, not open access: its ':' fails an unquoted query.
KLUWER = "10.1023/a:1010933404324"
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
    # the backoff after a 5xx or a dropped connection
    monkeypatch.setattr(springer_extractor.time, "sleep", sleep)
    return slept


def _catalog(settings, *identifiers, evidence):
    ctx = Context(settings, Catalog.open(settings.catalog_root, evidence=evidence))
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
        'doi:"10.1186/s12877-026-07992-9" OR doi:"10.1023/a:1010933404324"')


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
        assert copy_of(served) == DownloadCopy(
            route=CopyRoute.publisher, version=CopyVersion.vor, access=CopyAccess.public)
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


def test_articles_springer_does_not_serve_are_not_asked_for_again(tmp_path, slept, evidence):
    settings = _settings(tmp_path)
    ctx, refs = _catalog(settings, *TWENTY_ONE, evidence=evidence)
    with _replay(TWO_QUERIES):
        run_stages(ctx, [DownloadStage(settings)], refs)

    found = ctx.catalog.artifacts([ref.id for ref in refs], "download")
    status = {ref.identifier.doi.lower(): found[ref.id]["springer"].status for ref in refs}
    assert status.pop(CASE_REPORT) is Status.OK
    assert status.pop(MOLECULAR_PAIN) is Status.OK
    assert set(status.values()) == {Status.PERMANENT}
    assert DownloadStage(settings).plan(ctx, refs, found, {}).pending == []
    _assert_key_kept_out(ctx, refs, settings.springer_api_key)


def test_a_springer_article_is_read_into_its_springer_text(tmp_path, slept, evidence):
    """Download through the stage from the recorded answer, extract through the stage:
    pubget's code reads the JATS, sections, tables and figure captions."""
    settings = _settings(tmp_path)
    ctx, refs = _catalog(settings, *TWENTY_ONE, evidence=evidence)
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
def test_a_refused_key_stops_the_download_stage(tmp_path, slept, vcr, caplog, evidence):
    """The API refusing the key is not the articles' failure: none is charged a try,
    and the stage asks nothing more this run."""
    settings = _settings(tmp_path, springer_api_key="not-a-springer-key")
    ctx, refs = _catalog(settings, CASE_REPORT, LAST, evidence=evidence)
    stage = DownloadStage(settings)

    run_stages(ctx, [stage], refs)
    run_stages(ctx, [stage], refs)

    assert vcr.play_count == 1
    ids = [ref.id for ref in refs]
    assert not any(ctx.catalog.artifacts(ids, "download").values())
    assert ctx.catalog.attempt_counts(ids, "download", "springer") == {}
    assert len(DownloadStage(settings).plan(ctx, refs, {}, {}).pending) == 2
    assert "refused the API key in SPRINGER_API_KEY (401)" in caplog.text
    assert "not-a-springer-key" not in caplog.text


@pytest.mark.vcr(record_mode="none")
def test_without_a_key_the_stage_stops_before_asking_or_charging_anyone(
        tmp_path, caplog, evidence):
    settings = _settings(tmp_path, springer_api_key=None)
    ctx, refs = _catalog(settings, CASE_REPORT, evidence=evidence)

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


def test_a_spent_quota_leaves_the_rest_pending_and_springer_for_the_run(
        tmp_path, slept, caplog, evidence):
    """A 429 asking for longer than a run waits is the day's quota: the articles already
    answered are recorded, and the rest are charged nothing and not asked again this run."""
    cassette = _composed(tmp_path, (429, {"retry-after": ["86400"]}))
    settings = _settings(tmp_path)
    ctx, refs = _catalog(settings, *TWENTY_ONE, evidence=evidence)
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
def test_only_articles_with_a_springer_nature_doi_are_planned_for_springer(tmp_path, evidence):
    settings = _settings(tmp_path)
    ctx, refs = _catalog(settings, CASE_REPORT, OTHER_DOI, KLUWER, Identifier(pmid="16504543"),
            evidence=evidence)

    plan = DownloadStage(settings).plan(ctx, refs, {}, {})

    assert [work.ref.identifier.doi.lower() for work in plan.pending] == [
        CASE_REPORT, KLUWER.lower()]
    assert plan.skipped == 2


def test_an_article_served_without_its_body_is_not_a_download_and_is_asked_again(
        tmp_path, slept, evidence):
    """The API's record of it is front matter only: no text, so not this source's
    full text, and not known to be final either."""
    settings = _settings(tmp_path)
    ctx, refs = _catalog(settings, *FIVE, evidence=evidence)
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


# -- composed answers: the API gives none of these on demand -------------------------


def _uri(dois, start=1) -> str:
    params = {"p": len(dois), "q": doi_query(dois)}
    if start > 1:
        params["s"] = start
    return "https://api.springernature.com/openaccess/jats?" + urlencode(params)


def _answer(uri, status=200, body=b"", headers=None) -> dict:
    return {
        "request": {"body": None, "headers": {}, "method": "GET", "uri": uri},
        "response": {"body": {"string": body}, "headers": headers or {},
                     "status": {"code": status, "message": ""}},
    }


def _cassette(tmp_path, *interactions) -> Path:
    path = tmp_path / "composed.yaml"
    path.write_text(yaml.safe_dump({"version": 1, "interactions": list(interactions)}),
                    encoding="utf-8")
    return path


def _five_articles() -> dict:
    """The real five-article answer's articles, by DOI, as their XML."""
    recorded = yaml.safe_load(FIVE_BMC.read_text(encoding="utf-8"))
    body = gzip.decompress(recorded["interactions"][0]["response"]["body"]["string"])
    root = etree.fromstring(body)
    return {a.findtext("front/article-meta/article-id[@pub-id-type='doi']"): a
            for a in root.iterfind("records/article")}


def _response(articles, total=None) -> bytes:
    """A jats answer holding `articles`, as the API wraps them."""
    total = len(articles) if total is None else total
    records = b"".join(etree.tostring(a) for a in articles)
    return (b"<response><result><total>%d</total><start>1</start>"
            b"<recordsDisplayed>%d</recordsDisplayed></result><records>%s</records></response>"
            % (total, len(articles), records))


NO_DATA_404 = json.dumps({"status": "Fail", "message": "No data was found for the given query."})


def _download(extractor, *identifiers, **kwargs):
    return extractor.download(Identifiers(
        [i if isinstance(i, Identifier) else Identifier(doi=i) for i in identifiers]), **kwargs)


def test_a_server_error_is_retried_with_backoff(tmp_path, slept):
    case = _five_articles()[CASE_REPORT]
    uri = _uri([CASE_REPORT])
    cassette = _cassette(tmp_path, _answer(uri, 503), _answer(uri, 503),
                         _answer(uri, body=_response([case])))

    with _replay(cassette) as played:
        (result,) = _download(SpringerExtractor(_settings(tmp_path)), CASE_REPORT)

    assert played.all_played
    assert result.success
    assert slept == [2.0, 4.0]


def test_a_batch_the_server_keeps_failing_fails_alone_and_is_retried(tmp_path, slept, evidence):
    """Three 503s fail that batch's 20 articles for a later run, not for good, and the
    next batch is still asked; no failure quotes the URL or its key."""
    recorded = yaml.safe_load(TWO_QUERIES.read_text(encoding="utf-8"))
    first_uri = recorded["interactions"][0]["request"]["uri"]
    cassette = _cassette(tmp_path, *[_answer(first_uri, 503)] * 3, recorded["interactions"][1])
    settings = _settings(tmp_path)
    ctx, refs = _catalog(settings, *TWENTY_ONE, evidence=evidence)

    with _replay(cassette) as played:
        run_stages(ctx, [DownloadStage(settings)], refs)
        assert played.all_played

    found = ctx.catalog.artifacts([ref.id for ref in refs], "download")
    status = {ref.identifier.doi.lower(): found[ref.id]["springer"] for ref in refs}
    last = status.pop(LAST.lower())
    assert last.status is Status.PERMANENT
    assert {a.status for a in status.values()} == {Status.FAILED}
    assert {a.error for a in status.values()} == {
        "Springer Nature request failed: HTTP status 503"}
    _assert_key_kept_out(ctx, refs, settings.springer_api_key)
    _assert_key_kept_out(ctx, refs, "api.springernature.com")


def test_a_connection_that_keeps_failing_fails_the_batch_by_kind_alone(
        tmp_path, slept, monkeypatch):
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    # nothing listens there: the connection is refused, for real
    monkeypatch.setattr(springer_extractor, "API_URL", f"http://127.0.0.1:{port}/openaccess/jats")
    settings = _settings(tmp_path)

    (result,) = _download(SpringerExtractor(settings), CASE_REPORT)

    assert (result.success, result.error_message) == (
        False, "Springer Nature request failed: ConnectError")
    assert slept == [2.0, 4.0]


def test_a_malformed_later_answer_fails_only_its_own_batch(tmp_path, slept, evidence):
    recorded = yaml.safe_load(TWO_QUERIES.read_text(encoding="utf-8"))
    cassette = _cassette(tmp_path, recorded["interactions"][0],
                         _answer(_uri([LAST]), body=b"<response><records><article"))
    settings = _settings(tmp_path)
    ctx, refs = _catalog(settings, *TWENTY_ONE, evidence=evidence)

    with _replay(cassette):
        run_stages(ctx, [DownloadStage(settings)], refs)

    found = ctx.catalog.artifacts([ref.id for ref in refs], "download")
    status = {ref.identifier.doi.lower(): found[ref.id]["springer"] for ref in refs}
    assert status[CASE_REPORT].status is Status.OK
    assert status[MOLECULAR_PAIN].status is Status.OK
    assert (status[LAST.lower()].status, status[LAST.lower()].error) == (
        Status.FAILED, "Springer Nature sent an answer that is not XML")


def test_a_404_that_does_not_say_no_data_is_not_taken_as_not_open_access(
        tmp_path, slept, evidence):
    """A gateway's or a moved path's 404 says nothing about the articles: retried."""
    cassette = _cassette(tmp_path, _answer(_uri([LAST]), 404, b"<html>Not Found</html>"))
    settings = _settings(tmp_path)
    ctx, refs = _catalog(settings, LAST, evidence=evidence)

    with _replay(cassette) as played:
        run_stages(ctx, [DownloadStage(settings)], refs)
        assert played.all_played

    artifact = ctx.catalog.artifacts([refs[0].id], "download")[refs[0].id]["springer"]
    assert artifact.status is Status.FAILED
    assert artifact.error != NOT_OPEN_ACCESS


def test_a_query_refused_for_every_doi_alone_leaves_springer_with_nothing_charged(
        tmp_path, slept, caplog, evidence):
    """Every DOI refused even alone is the query's fault, not theirs: none is final."""
    asked = [CASE_REPORT, MOLECULAR_PAIN, LAST]
    cassette = _cassette(tmp_path, *[
        _answer(_uri(dois), 400) for dois in
        (asked, asked[:1], asked[1:], asked[1:2], asked[2:])])
    settings = _settings(tmp_path)
    ctx, refs = _catalog(settings, *asked, evidence=evidence)
    stage = DownloadStage(settings)

    with _replay(cassette) as played:
        report = run_stages(ctx, [stage], refs)
        assert played.all_played
        run_stages(ctx, [stage], refs)
        assert played.play_count == 5

    ids = [ref.id for ref in refs]
    assert not any(ctx.catalog.artifacts(ids, "download").values())
    assert ctx.catalog.attempt_counts(ids, "download", "springer") == {}
    assert "refused every DOI of a query, each asked alone (400)" in report.render()


def test_429s_do_not_spend_the_server_error_tries(tmp_path, slept):
    case = _five_articles()[CASE_REPORT]
    uri = _uri([CASE_REPORT])
    cassette = _cassette(tmp_path, _answer(uri, 429), _answer(uri, 503), _answer(uri, 503),
                         _answer(uri, body=_response([case])))

    with _replay(cassette) as played:
        (result,) = _download(SpringerExtractor(_settings(tmp_path)), CASE_REPORT)
        assert played.all_played

    assert result.success


def test_a_refused_query_is_halved_until_the_doi_it_cannot_name_is_alone(
        tmp_path, slept, evidence):
    """A 400 is one DOI's: the others in its query are still asked, and only it is final.
    A Markdown backslash and look-alike dashes are dropped from what is asked."""
    articles = _five_articles()
    escaped = "10.1007/978-981-33-6044-0\\_18"
    dashed = "10.1186/s13195\u2010026\u201002139\u20109"
    assert query_doi(escaped) == "10.1007/978-981-33-6044-0_18"
    asked = [CASE_REPORT, query_doi(escaped), query_doi(dashed)]
    cassette = _cassette(
        tmp_path,
        _answer(_uri(asked), 400),
        _answer(_uri(asked[:1]), body=_response([articles[CASE_REPORT]])),
        _answer(_uri(asked[1:]), 400),
        _answer(_uri(asked[1:2]), 400),
        _answer(_uri(asked[2:]), body=_response([articles["10.1186/s13195-026-02139-9"]])),
    )
    settings = _settings(tmp_path)
    ctx, refs = _catalog(settings, CASE_REPORT, escaped, dashed, evidence=evidence)

    with _replay(cassette) as played:
        run_stages(ctx, [DownloadStage(settings)], refs)
        assert played.all_played

    found = ctx.catalog.artifacts([ref.id for ref in refs], "download")
    case, refused, served = (found[ref.id]["springer"] for ref in refs)
    assert case.status is Status.OK and served.status is Status.OK
    assert (refused.status, refused.error) == (Status.PERMANENT, QUERY_REFUSED)


def test_dois_are_matched_by_value_not_case_or_order_and_shared_ones_both_served(tmp_path, slept):
    """Two identifiers with one DOI (in two spellings) are asked once and both served;
    the answer's articles, out of the order asked, each go to their own DOI."""
    articles = _five_articles()
    other = "10.1186/s13195-026-02115-3"
    asked = [CASE_REPORT, other]
    cassette = _cassette(tmp_path, _answer(
        _uri(asked), body=_response([articles[other], articles[CASE_REPORT]])))
    identifiers = [Identifier(doi="10.1186/S12877\u2010026\u201007992\u20109"),
                   Identifier(doi=other), Identifier(doi=CASE_REPORT, pmid="40000001")]
    progress = []

    with _replay(cassette) as played:
        results = _download(SpringerExtractor(_settings(tmp_path)), *identifiers,
                            progress_hook=progress.append)
        assert played.play_count == 1

    assert len(results) == len(identifiers) == sum(progress)
    assert {id(r.identifier) for r in results} == {id(i) for i in identifiers}
    for result in results:
        assert result.success
        article = etree.parse(str(result.files[0].file_path))
        served_doi = article.findtext("front/article-meta/article-id[@pub-id-type='doi']")
        wanted = other if result.identifier.doi == other else CASE_REPORT
        assert served_doi == wanted


@pytest.mark.parametrize("body_first", [False, True])
def test_every_page_is_read_and_a_duplicate_with_a_body_is_kept(tmp_path, slept, body_first):
    """More records than a page holds are paged with `s`; of two records for one DOI
    the one with a body is kept, whichever page it is on."""
    case = _five_articles()[CASE_REPORT]
    stripped = etree.fromstring(etree.tostring(case))
    stripped.remove(stripped.find("body"))
    pages = [case, stripped] if body_first else [stripped, case]
    cassette = _cassette(
        tmp_path,
        _answer(_uri([CASE_REPORT]), body=_response(pages[:1], total=2)),
        _answer(_uri([CASE_REPORT], start=2), body=_response(pages[1:], total=2)),
    )

    with _replay(cassette) as played:
        (result,) = _download(SpringerExtractor(_settings(tmp_path)), CASE_REPORT)
        assert played.all_played

    assert result.success
    assert etree.parse(str(result.files[0].file_path)).find("body") is not None


def test_a_fourth_429_in_a_row_is_the_quota_spent(tmp_path, slept):
    cassette = _cassette(tmp_path, *[_answer(_uri([CASE_REPORT]), 429)] * 4)

    with _replay(cassette) as played:
        with pytest.raises(SourceExhausted, match="kept answering 429"):
            _download(SpringerExtractor(_settings(tmp_path)), CASE_REPORT)
        assert played.all_played

    assert slept == [2.0, 4.0, 8.0]


def test_a_403_is_the_quota_spent_leaving_every_article_pending_and_said_in_the_report(
        tmp_path, slept, caplog, evidence):
    """A refusal of a key the API knows: nothing is charged, Springer is asked nothing
    more, the next source is still asked, and the run report says so."""
    message = "Account over its daily limit"
    cassette = _cassette(tmp_path, _answer(
        _uri([CASE_REPORT, LAST]), 403,
        json.dumps({"status": "Fail", "message": message}).encode()))
    settings = _settings(tmp_path, download_sources=["springer", "wiley"], wiley_api_key=None)
    wiley_doi = "10.1111/j.1440-1614.2006.01765.x"
    ctx, refs = _catalog(settings, CASE_REPORT, LAST, wiley_doi, evidence=evidence)

    with _replay(cassette) as played:
        report = run_stages(ctx, [DownloadStage(settings)], refs)
        assert played.play_count == 1

    ids = [ref.id for ref in refs]
    assert not any(ctx.catalog.artifacts(ids, "download").values())
    assert ctx.catalog.attempt_counts(ids, "download", "springer") == {}
    line = report.render()
    assert f"springer EXHAUSTED: Springer Nature refused this key (403): {message}" in line
    assert "2 article(s) left pending for springer (later sources still tried)" in line
    # the next source was still reached: it stopped the stage for want of its own token
    assert "download stopped: wiley: no Wiley TDM token" in caplog.text
    assert len(DownloadStage(settings).plan(ctx, refs[:2], {}, {}).pending) == 2


def test_springer_is_not_asked_about_an_article_another_source_already_served(tmp_path, evidence):
    settings = _settings(tmp_path, download_sources=["elsevier", "springer"])
    ctx, refs = _catalog(settings, CASE_REPORT, evidence=evidence)
    stage = DownloadStage(settings)
    ctx.catalog.record([Outcome(refs[0].id, "download", "elsevier", Status.OK,
                                fingerprint=stage.fingerprint_for(DownloadSource.ELSEVIER),
                                payload={"files": []})])

    found = ctx.catalog.artifacts([refs[0].id], "download")
    plan = stage.plan(ctx, refs, found, {})

    assert (plan.pending, plan.fresh) == ([], 1)


def test_a_springer_extraction_has_the_jats_metadata_fallback(tmp_path, slept, evidence):
    settings = _settings(tmp_path)
    ctx, refs = _catalog(settings, *FIVE, evidence=evidence)
    with _replay(FIVE_BMC):
        run_stages(ctx, [DownloadStage(settings)], refs)
    case = next(ref for ref in refs if ref.identifier.doi.lower() == CASE_REPORT)
    run_stages(ctx, [ExtractStage(settings)], [case])
    artifact = ctx.catalog.artifacts([case.id], "extract")[case.id]["springer"]
    content = ExtractedContent.from_dict(ctx.catalog.payload(artifact))

    metadata = MetadataService(settings)._get_fallback_metadata(content)

    assert metadata.title.startswith("A case report of potentially inappropriate")


def test_springer_ranks_with_the_publisher_xml_sources():
    order = Settings().download_sources
    assert order.index("europepmc") < order.index("elsevier") < order.index("springer")
    assert order.index("springer") < order.index("ace") < order.index("pdf") < order.index("wiley")
    assert TIE_ORDER.index("elsevier") < TIE_ORDER.index("springer") < TIE_ORDER.index("ace")
    assert "springer" in JATS_SOURCES
    # extracted by pubget's code, as pmc and europepmc are, at the version it started at
    assert EXTRACTOR_VERSIONS["springer"] == 1
