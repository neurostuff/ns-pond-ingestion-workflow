"""Every candidate PDF URL is tried in order, against local OpenAlex, Semantic Scholar
and file hosts."""

from __future__ import annotations

import re
import time
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime
from urllib.parse import urlsplit

import pytest
import requests
from ingestion_workflow.catalog import ArticleRef, Catalog, Outcome, Status
from ingestion_workflow.clients.openalex import OpenAlexClient
from ingestion_workflow.extractors import pdf_extractor
from ingestion_workflow.extractors.pdf_extractor import (
    SKIPPED_PMC,
    PdfCandidate,
    _HostGate,
    _is_pmc_copy,
    _retry_after,
    _try_order,
    _type_of,
    _version_of,
    remembered_dead_urls,
)
from ingestion_workflow.models import (
    DownloadResult,
    DownloadSource,
    ExtractedContent,
    Identifier,
    Identifiers,
)
from ingestion_workflow.pipeline import Context
from ingestion_workflow.pipeline.stages.download import DownloadStage
from ingestion_workflow.tests.oa_hosts import EMAIL, HTML, PDF, OaHosts, location, pdf_source
from tenacity import wait_none


def _download(extractor, *dois) -> list[DownloadResult]:
    return extractor.download(Identifiers([Identifier(doi=d) for d in dois]))


def _outcomes(result):
    return [a.get("outcome") for a in result.url_attempts]


def test_a_repository_copy_is_tried_before_the_publisher_copy(stub, tmp_path):
    publisher = stub.serve("/journal/a.pdf", (200, PDF))
    repository = stub.serve("/escholarship/a.pdf", (200, PDF))
    best = location(publisher, "journal")
    copy = location(repository, "repository", name="eScholarship")
    stub.work("10.1234/a", best, copy, best=best)

    result = _download(pdf_source(tmp_path), "10.1234/a")[0]

    assert result.success
    assert stub.fetched == ["/escholarship/a.pdf"]
    assert result.files[0].file_path.read_bytes() == PDF
    assert result.url_attempts == [
        {
            "url": repository,
            "provider": "openalex",
            "location_type": "repository",
            "source": "eScholarship",
            "version": "publishedVersion",
            "status": 200,
            "outcome": "pdf",
            "reason": "PDF downloaded",
        }
    ]
    stage = DownloadStage(pdf_source(tmp_path).settings)
    ref = ArticleRef(id="art1", identifier=result.identifier)
    ok = stage._outcome(ref, DownloadSource.PDF, result, "fp")
    assert ok.status is Status.OK
    assert ok.summary["url_attempts"] == ok.payload["url_attempts"] == result.url_attempts


def test_a_403_then_a_page_that_is_not_a_pdf_fall_through_to_the_next_url(stub, tmp_path):
    pmc = stub.serve("/europepmc/a.pdf", (403, b"Forbidden"))
    journal = stub.serve("/journal/a.pdf", (200, HTML))
    s2 = stub.serve("/s2/a.pdf", (200, PDF))
    stub.work("10.1234/a", location(journal, "journal"), location(pmc, "repository"))
    stub.s2["DOI:10.1234/a"] = s2

    result = _download(pdf_source(tmp_path), "10.1234/a")[0]

    assert result.success
    assert stub.fetched == ["/europepmc/a.pdf", "/journal/a.pdf", "/s2/a.pdf"]
    assert [(a["provider"], a["outcome"], a["status"]) for a in result.url_attempts] == [
        ("openalex", "http_error", 403),
        ("openalex", "not_pdf", 200),
        ("semantic_scholar", "pdf", 200),
    ]
    assert "403" in result.url_attempts[0]["reason"]
    assert result.url_attempts[1]["reason"] == f"Response from {journal} is not a PDF."


def test_every_attempt_is_recorded_and_the_last_reason_is_the_outcome(stub, tmp_path):
    first = stub.serve("/repo/a.pdf", (403, b"Forbidden"))
    second = stub.serve("/journal/a.pdf", (200, HTML))
    landing = stub.url + "/landing/a"
    stub.work(
        "10.1234/a",
        location(first, "repository"),
        location(second, "journal"),
        location(None, "repository", landing=landing),
    )

    result = _download(pdf_source(tmp_path), "10.1234/a")[0]

    assert not result.success
    assert _outcomes(result) == ["http_error", "not_pdf", "landing_page_only"]
    assert result.error_message == f"Response from {second} is not a PDF."
    assert "/landing/a" not in stub.fetched

    ref = ArticleRef(id="art1", identifier=result.identifier)
    stage = DownloadStage(pdf_source(tmp_path).settings)
    outcome = stage._outcome(ref, DownloadSource.PDF, result, "fp")
    # every copy is dead (a 403, an HTML page) or a landing page; a new copy may be listed
    assert outcome.status is Status.FAILED
    assert outcome.error == result.error_message
    assert outcome.summary["url_attempts"] == result.url_attempts
    assert DownloadResult.from_dict(result.to_dict()).url_attempts == result.url_attempts


def test_an_open_access_landing_page_alone_is_recorded_not_fetched(stub, tmp_path):
    landing = stub.url + "/landing/a"
    stub.serve("/landing/a", (200, HTML))
    stub.work("10.1234/a", location(None, "repository", landing=landing))

    result = _download(pdf_source(tmp_path), "10.1234/a")[0]

    assert not result.success
    assert result.error_message == f"OA landing page only: {landing}"
    assert stub.fetched == []


def test_a_429_is_waited_out_as_the_host_asks(stub, tmp_path):
    url = stub.serve(
        "/repo/a.pdf",
        (429, b"slow down", {"Retry-After": "7"}),
        (429, b"slow down", {"Retry-After": "7"}),
        (200, PDF),
    )
    stub.work("10.1234/a", location(url, "repository"))
    extractor = pdf_source(tmp_path)

    result = _download(extractor, "10.1234/a")[0]

    assert result.success
    assert extractor.slept == [7.0, 7.0]
    assert stub.fetched == ["/repo/a.pdf"] * 3


def test_a_host_asking_to_wait_past_the_cap_is_left_for_the_run(stub, tmp_path):
    other_host = OaHosts()
    try:
        busy = stub.serve("/repo/a.pdf", (429, b"slow down", {"Retry-After": "3600"}))
        other = other_host.serve("/journal/a.pdf", (200, PDF))
        stub.work("10.1234/a", location(busy, "repository"), location(other, "journal"))
        extractor = pdf_source(tmp_path)

        result = _download(extractor, "10.1234/a")[0]
    finally:
        other_host.close()

    assert result.success
    # no early retry: the URL is given up at once and the host is not waited for
    assert extractor.slept == []
    assert stub.fetched == ["/repo/a.pdf"]
    assert other_host.fetched == ["/journal/a.pdf"]
    assert _outcomes(result) == ["rate_limited", "pdf"]
    assert result.url_attempts[0]["status"] == 429
    assert "asked to wait 3600s" in result.url_attempts[0]["reason"]


def test_a_429_without_retry_after_backs_off_by_doubling(stub, tmp_path):
    url = stub.serve("/repo/a.pdf", (429, b""), (429, b""), (200, PDF))
    stub.work("10.1234/a", location(url, "repository"))
    extractor = pdf_source(tmp_path)

    assert _download(extractor, "10.1234/a")[0].success
    assert extractor.slept == [2.0, 4.0]


def test_a_url_both_providers_list_is_fetched_once(stub, tmp_path):
    url = stub.serve("/europepmc/a.pdf", (403, b"Forbidden"))
    stub.work("10.1234/a", location(url, "repository"))
    stub.s2["DOI:10.1234/a"] = url.replace("http://", "HTTP://") + "/"

    result = _download(pdf_source(tmp_path), "10.1234/a")[0]

    assert stub.fetched == ["/europepmc/a.pdf"]
    assert _outcomes(result) == ["http_error"]


def test_a_preprint_is_tried_after_the_published_copy(stub, tmp_path):
    preprint = stub.serve("/biorxiv/a.pdf", (200, PDF))
    published = stub.serve("/journal/a.pdf", (200, PDF))
    stub.work(
        "10.1234/a",
        location(preprint, "repository", version="submittedVersion"),
        location(published, "journal"),
    )

    result = _download(pdf_source(tmp_path), "10.1234/a")[0]

    assert stub.fetched == ["/journal/a.pdf"]
    assert result.url_attempts[0]["location_type"] == "publisher"


def test_openalex_is_asked_about_articles_semantic_scholar_answered(stub, tmp_path):
    s2 = stub.serve("/s2/a.pdf", (403, b"Forbidden"))
    repo = stub.serve("/repo/a.pdf", (200, PDF))
    stub.s2["DOI:10.1234/a"] = s2
    stub.work("10.1234/a", location(repo, "repository"))

    result = _download(pdf_source(tmp_path), "10.1234/a", "10.1234/b")[0]

    assert result.success
    assert stub.fetched == ["/repo/a.pdf"]
    (query,) = stub.openalex_queries
    assert query["filter"] == ["doi:10.1234/a|10.1234/b"]
    assert query["mailto"] == [EMAIL]
    assert "locations" in query["select"][0].split(",")


def test_a_failed_openalex_lookup_is_recorded_and_the_other_provider_still_tried(
    stub, tmp_path, monkeypatch
):
    monkeypatch.setattr(OpenAlexClient._request_openalex.retry, "wait", wait_none())
    stub.openalex_status = 500
    s2 = stub.serve("/s2/a.pdf", (200, HTML))
    stub.s2["DOI:10.1234/a"] = s2

    a, b = _download(pdf_source(tmp_path), "10.1234/a", "10.1234/b")

    assert [x["outcome"] for x in a.url_attempts] == ["lookup_failed", "not_pdf"]
    assert a.url_attempts[0]["provider"] == "openalex"
    assert a.error_message == f"Response from {s2} is not a PDF."
    assert _outcomes(b) == ["lookup_failed"]
    assert b.error_message.startswith("openalex lookup failed:")


def test_an_article_no_provider_knows_still_says_no_pdf_url(stub, tmp_path):
    result = _download(pdf_source(tmp_path), "10.1234/unknown")[0]

    assert not result.success
    assert result.error_message == "No open-access PDF URL found."
    assert result.url_attempts == []


def test_a_location_openalex_does_not_call_open_access_is_not_fetched(stub, tmp_path):
    closed = stub.serve("/journal/closed.pdf", (200, PDF))
    stub.work("10.1234/a", {**location(closed, "journal"), "is_oa": False})

    result = _download(pdf_source(tmp_path), "10.1234/a")[0]

    assert stub.fetched == []
    assert result.error_message == "No open-access PDF URL found."


class _LocalOnlySession(requests.Session):
    """Refuses any host but the stub's, so a test never reaches the internet."""

    def __init__(self) -> None:
        super().__init__()
        self.refused: list[str] = []
        self.asked: list[dict] = []

    def get(self, url, **kwargs):
        self.asked.append(kwargs)
        if urlsplit(url).hostname != "127.0.0.1":
            self.refused.append(url)
            raise requests.ConnectionError(f"test refuses {url}")
        return super().get(url, **kwargs)


@pytest.fixture
def other_host():
    server = OaHosts()
    yield server
    server.close()


def _stage_outcome(tmp_path, result):
    stage = DownloadStage(pdf_source(tmp_path).settings)
    ref = ArticleRef(id="art1", identifier=result.identifier)
    return stage._outcome(ref, DownloadSource.PDF, result, "fp")


# -- permanence -------------------------------------------------------------------


def test_a_rate_limited_copy_keeps_an_article_retryable_after_a_404(stub, other_host, tmp_path):
    busy = stub.serve("/repo/a.pdf", (429, b"", {"Retry-After": "3600"}))
    gone = other_host.serve("/journal/a.pdf", (404, b"gone"))
    stub.work("10.1234/a", location(busy, "repository"), location(gone, "journal"))

    result = _download(pdf_source(tmp_path), "10.1234/a")[0]

    assert _outcomes(result) == ["rate_limited", "http_error"]
    assert "404" in result.error_message
    assert _stage_outcome(tmp_path, result).status is Status.FAILED


def test_an_article_whose_every_copy_is_dead_stays_retryable(stub, tmp_path):
    forbidden = stub.serve("/journal/a.pdf", (403, b"no"))
    html = stub.serve("/repo/a.pdf", (200, HTML))
    landing = stub.url + "/landing/a"
    stub.work(
        "10.1234/a",
        location(forbidden, "journal"),
        location(html, "repository"),
        location(None, "repository", landing=landing),
    )

    result = _download(pdf_source(tmp_path), "10.1234/a")[0]

    assert _outcomes(result) == ["not_pdf", "http_error", "landing_page_only"]
    assert [a.get("dead") for a in result.url_attempts] == [True, True, None]
    outcome = _stage_outcome(tmp_path, result)
    assert outcome.status is Status.FAILED
    assert outcome.summary["url_outcomes"] == {
        "not_pdf": 1, "http_error": 1, "landing_page_only": 1
    }


def test_a_failed_lookup_keeps_an_article_retryable(stub, tmp_path, monkeypatch):
    monkeypatch.setattr(OpenAlexClient._request_openalex.retry, "wait", wait_none())
    stub.openalex_status = 500
    stub.s2["DOI:10.1234/a"] = stub.serve("/s2/a.pdf", (404, b"gone"))

    result = _download(pdf_source(tmp_path), "10.1234/a")[0]

    assert _outcomes(result) == ["lookup_failed", "http_error"]
    assert _stage_outcome(tmp_path, result).status is Status.FAILED


def test_a_server_error_or_timeout_is_not_final(stub, tmp_path):
    stub.work("10.1234/a", location(stub.serve("/repo/a.pdf", (503, b"down")), "repository"))
    stub.work("10.1234/b", location(stub.url.replace("127.0.0.1", "localhost") + "/x.pdf",
                                     "repository"))

    a, b = _download(pdf_source(tmp_path, session=_LocalOnlySession()), "10.1234/a", "10.1234/b")

    assert _outcomes(a) == ["http_error"] and _stage_outcome(tmp_path, a).status is Status.FAILED
    assert _outcomes(b) == ["fetch_error"] and _stage_outcome(tmp_path, b).status is Status.FAILED


def test_an_article_with_no_candidate_is_not_permanent(stub, tmp_path):
    result = _download(pdf_source(tmp_path), "10.1234/unknown")[0]

    assert _stage_outcome(tmp_path, result).status is Status.FAILED


# -- PMC and Europe PMC copies --------------------------------------------------------


def test_pmc_and_europe_pmc_copies_are_left_to_their_own_sources(stub, tmp_path):
    pmc = [
        "https://pmc.ncbi.nlm.nih.gov/articles/PMC1/pdf/a.pdf",
        "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC1/pdf/a.pdf",
        "https://europepmc.org/articles/pmc1?pdf=render",
    ]
    pubmed = "https://www.ncbi.nlm.nih.gov/pubmed/1.pdf"
    local = stub.serve("/journal/a.pdf", (200, PDF))
    stub.work("10.1234/a", *[location(url, "repository") for url in pmc[:2]],
              location(pubmed, "journal"), location(local, None))
    stub.s2["DOI:10.1234/a"] = pmc[2]  # untyped, as Semantic Scholar gives it
    session = _LocalOnlySession()

    result = _download(pdf_source(tmp_path, session=session), "10.1234/a")[0]

    assert result.success
    assert session.refused == [pubmed]  # a PubMed URL is not a PMC copy
    skipped = [a for a in result.url_attempts if a["outcome"] == "skipped_pmc"]
    assert sorted(a["url"] for a in skipped) == sorted(pmc)
    assert {a["reason"] for a in skipped} == {SKIPPED_PMC}
    assert remembered_dead_urls(result.url_attempts) == {}
    assert "status" not in skipped[0]


@pytest.mark.parametrize(
    "url, is_pmc",
    [
        ("https://pmc.ncbi.nlm.nih.gov/articles/PMC1/pdf/a.pdf", True),
        ("https://www.ncbi.nlm.nih.gov/pmc/articles/PMC1/pdf/a.pdf", True),
        ("https://ncbi.nlm.nih.gov/pmc/articles/PMC1/pdf/a.pdf", True),
        ("https://europepmc.org/articles/pmc1?pdf=render", True),
        ("https://www.europepmc.org/articles/pmc1?pdf=render", True),
        ("https://europepmc.org/backend/ptpmcrender.fcgi?accid=PMC1&blobtype=pdf", True),
        ("https://europepmc.org/api/fulltextRepo?pprId=PPR1&type=FILE&fileName=a.pdf", False),
        ("https://europepmc.org/article/PPR/PPR1", False),
        ("https://PMC.NCBI.NLM.NIH.GOV/articles/PMC1/pdf/a.pdf", True),
        ("https://www.ncbi.nlm.nih.gov/pubmed/1.pdf", False),
        ("https://www.ncbi.nlm.nih.gov/pmcfoo/a.pdf", False),
        ("https://noteuropepmc.org/a.pdf", False),
        ("https://example.org/pmc/a.pdf", False),
    ],
)
def test_which_hosts_are_pmc_or_europe_pmc_copies(url, is_pmc):
    assert _is_pmc_copy(url) is is_pmc


@pytest.mark.parametrize(
    "source_type, location_type",
    [
        ("repository", "repository"),
        ("journal", "publisher"),
        ("conference", "publisher"),
        ("book series", "publisher"),
        ("ebook platform", "publisher"),
        ("metadata", "unknown"),
        (None, "unknown"),
    ],
)
def test_an_openalex_source_type_ranks_the_copy(source_type, location_type):
    assert _type_of(source_type) == location_type


@pytest.mark.parametrize(
    "url",
    [
        "https://arxiv.org/pdf/1.pdf",
        "https://www.biorxiv.org/content/1v1.full.pdf",
        "https://www.medrxiv.org/content/1v1.full.pdf",
        "https://osf.io/a/download",
        "https://www.preprints.org/manuscript/1/v1/download",
        "https://psyarxiv.com/a/download",
        "https://www.researchsquare.com/article/rs-1/v1.pdf",
        "https://papers.ssrn.com/sol3/Delivery.cfm/1.pdf",
    ],
)
def test_every_preprint_server_marks_an_unlabelled_copy_as_a_preprint(url):
    assert _version_of(url, None) == "submittedVersion"


def test_a_host_that_only_ends_like_a_preprint_server_is_not_one():
    assert _version_of("https://notarxiv.org/a.pdf", None) is None


# -- how listings are merged and ordered ------------------------------------------


def test_urls_that_differ_only_in_their_query_are_two_copies():
    first = PdfCandidate("https://example.org/get.pdf?id=1", "openalex", "publisher")
    second = PdfCandidate("https://example.org/get.pdf?id=2", "openalex", "publisher")

    assert [c.url for c in _try_order([first, second])] == [first.url, second.url]


def test_a_url_listed_twice_keeps_every_label_either_listing_carries():
    typed = PdfCandidate("https://example.org/a.pdf", "openalex", "repository",
                         source="Some Repository", version="acceptedVersion", open_access=False,
                         landing_page=True)
    bare = PdfCandidate("https://EXAMPLE.org/a.pdf/", "semantic_scholar", "unknown")

    for listings in ([typed, bare], [bare, typed]):
        (merged,) = _try_order(listings)
        assert (merged.provider, merged.location_type) == ("openalex", "repository")
        assert merged.source == "Some Repository"
        assert merged.version == "acceptedVersion"
        assert merged.also_listed_by == ("semantic_scholar",)
        assert merged.open_access is True and merged.landing_page is False


def test_the_first_of_two_typed_listings_describes_the_url():
    url = "https://a.org/1.pdf"

    (merged,) = _try_order(
        [
            PdfCandidate(url, "openalex", "repository"),
            PdfCandidate(url, "semantic_scholar", "publisher"),
        ]
    )

    assert (merged.provider, merged.location_type) == ("openalex", "repository")
    assert merged.also_listed_by == ("semantic_scholar",)


def test_a_url_one_provider_lists_twice_is_not_listed_by_another():
    url = "https://a.org/1.pdf"

    (merged,) = _try_order(
        [PdfCandidate(url, "openalex", "repository"), PdfCandidate(url, "openalex", "publisher")]
    )

    assert merged.also_listed_by == ()


def test_a_second_listing_supplies_a_label_the_first_lacks():
    first = PdfCandidate("https://example.org/a.pdf", "openalex", "repository")
    second = PdfCandidate("https://example.org/a.pdf", "semantic_scholar", "unknown",
                          source="Repo", version="acceptedVersion")

    (merged,) = _try_order([first, second])

    assert (merged.source, merged.version) == ("Repo", "acceptedVersion")


def test_a_closed_copy_is_tried_after_an_open_one():
    closed = PdfCandidate("https://example.org/closed.pdf", "openalex", "repository",
                          open_access=False)
    opened = PdfCandidate("https://example.org/open.pdf", "openalex", "publisher")

    assert [c.url for c in _try_order([closed, opened])] == [opened.url, closed.url]


def test_equally_ranked_copies_keep_the_providers_order():
    urls = [f"https://example.org/{n}.pdf" for n in range(4)]
    listed = [PdfCandidate(url, "openalex", "publisher") for url in urls]

    assert [c.url for c in _try_order(listed)] == urls


# -- what a later run remembers -----------------------------------------------------


def test_only_a_dead_url_is_remembered():
    attempts = [
        {"url": "https://a.org/1.pdf", "outcome": "http_error", "dead": True, "reason": "403"},
        {"url": "https://a.org/2.pdf", "outcome": "http_error", "reason": "503"},
        {"url": "https://a.org/4.pdf", "outcome": "landing_page_only"},
    ]

    assert remembered_dead_urls(attempts) == {"a.org/1.pdf?": "403"}


def _answer(headers):
    response = requests.Response()
    response.status_code = 429
    response.headers.update(headers)
    return response


@pytest.mark.parametrize(
    "header, seconds", [("7", 7.0), ("0", 0.0), ("-5", 0.0), ("soon", None), (None, None)]
)
def test_retry_after_is_read_as_seconds_and_never_negative(header, seconds):
    headers = {} if header is None else {"Retry-After": header}

    assert _retry_after(_answer(headers)) == seconds


def test_retry_after_is_read_as_an_http_date():
    def asked(offset):
        when = format_datetime(datetime.now(timezone.utc) + offset, usegmt=True)
        return _retry_after(_answer({"Retry-After": when}))

    assert 3590 < asked(timedelta(hours=1)) <= 3600
    assert asked(timedelta(hours=-1)) == 0.0


@pytest.mark.parametrize(
    "form",
    [
        lambda when: time.asctime(when.timetuple()),
        lambda when: format_datetime(when.replace(tzinfo=None)),  # "-0000": no zone given
    ],
    ids=["asctime", "no-zone"],
)
def test_retry_after_is_read_as_a_date_with_no_zone_in_utc(form):
    when = form(datetime.now(timezone.utc) + timedelta(hours=1))

    assert 3590 < _retry_after(_answer({"Retry-After": when})) <= 3600


def test_a_host_giving_a_date_an_hour_away_is_left_for_the_run(stub, tmp_path):
    when = format_datetime(datetime.now(timezone.utc) + timedelta(hours=1), usegmt=True)
    url = stub.serve("/repo/a.pdf", (429, b"", {"Retry-After": when}))
    stub.work("10.1234/a", location(url, "repository"))
    extractor = pdf_source(tmp_path)

    result = _download(extractor, "10.1234/a")[0]

    assert stub.fetched == ["/repo/a.pdf"] and extractor.slept == []
    assert _outcomes(result) == ["rate_limited"]
    assert re.search(r"asked to wait 3(599|600)s", result.error_message)


@pytest.mark.parametrize(
    "status, dead",
    [(400, True), (403, True), (404, True), (408, False), (425, False), (500, False),
     (503, False)],
)
def test_which_http_statuses_mean_the_url_is_dead(stub, tmp_path, status, dead):
    stub.work("10.1234/a", location(stub.serve("/repo/a.pdf", (status, b"")), "repository"))

    result = _download(pdf_source(tmp_path), "10.1234/a")[0]

    assert result.url_attempts[0].get("dead", False) is dead
    assert _stage_outcome(tmp_path, result).status is Status.FAILED


@pytest.mark.parametrize("body", [b"%PDF\n<html>", b"<html>%PDF-1.4</html>"])
def test_a_payload_that_does_not_start_as_a_pdf_is_not_one(stub, tmp_path, body):
    payload = stub.serve("/repo/a.pdf", (200, body))
    stub.work("10.1234/a", location(payload, "repository"))

    result = _download(pdf_source(tmp_path), "10.1234/a")[0]

    assert _outcomes(result) == ["not_pdf"]


def test_the_error_names_the_last_url_passed_over_when_none_was_fetched(tmp_path):
    extractor = pdf_source(tmp_path)
    identifier = Identifier(doi="10.1234/a")
    first, second = (
        PdfCandidate(f"https://example.org/{n}", "openalex", "repository", landing_page=True)
        for n in (1, 2)
    )

    result = extractor._download_single(identifier, [first, second], tmp_path)

    assert result.error_message == f"OA landing page only: {second.url}"


def test_the_error_names_the_last_failed_lookup_when_there_is_no_url(tmp_path):
    extractor = pdf_source(tmp_path)
    failures = [
        {"provider": p, "outcome": "lookup_failed", "reason": f"{p} lookup failed: down"}
        for p in ("semantic_scholar", "openalex")
    ]

    result = extractor._download_single(Identifier(doi="10.1234/a"), [], tmp_path, failures)

    assert result.error_message == "openalex lookup failed: down"


# -- host gate edges ----------------------------------------------------------------


def test_a_host_asking_for_exactly_the_longest_wait_is_waited_for():
    gate = _HostGate(lambda: 0.0, lambda delay: None)

    assert gate.rate_limited("h", 60.0) is None
    assert "asked to wait 61s" in gate.rate_limited("other", 61.0)


def test_a_longer_wait_already_given_is_not_shortened_by_a_later_429():
    now = [0.0]
    slept: list[float] = []
    gate = _HostGate(lambda: now[0], slept.append)

    gate.rate_limited("h", 30.0)
    gate.rate_limited("h", 5.0)
    gate.wait("h")

    assert slept == [30.0]


def test_an_answer_from_the_host_ends_a_run_of_429s():
    gate = _HostGate(lambda: 0.0, lambda delay: None)

    for _ in range(3):
        assert gate.rate_limited("h", None) is None
    gate.answered("h", served=True)

    assert gate.rate_limited("h", None) is None


def test_pages_that_are_not_pdfs_count_as_a_host_refusing(stub, tmp_path):
    dois = [f"10.1234/h{n}" for n in range(22)]
    for doi in dois:
        stub.work(doi, location(stub.serve(f"/{doi}.pdf", (200, HTML)), "journal"))

    results = _download(pdf_source(tmp_path), *dois)

    assert len(stub.fetched) == 20
    assert results[21].url_attempts[0]["outcome"] == "deferred"


# -- per-host backoff --------------------------------------------------------------


def test_a_host_left_for_the_run_is_not_asked_again_for_other_articles(
    stub, other_host, tmp_path
):
    stub.serve("/repo/a.pdf", (429, b"", {"Retry-After": "900"}))
    stub.serve("/repo/b.pdf", (200, PDF))
    other = other_host.serve("/journal/b.pdf", (403, b"no"))
    stub.work("10.1234/a", location(stub.url + "/repo/a.pdf", "repository"))
    stub.work("10.1234/b", location(stub.url + "/repo/b.pdf", "repository"),
              location(other, "journal"))
    extractor = pdf_source(tmp_path)

    a, b = _download(extractor, "10.1234/a", "10.1234/b")

    assert stub.fetched == ["/repo/a.pdf"]
    assert _outcomes(b) == ["deferred", "http_error"]
    assert b.url_attempts[0]["reason"] == f"deferred: {_host(stub.url)} asked to wait 900s"
    assert [_stage_outcome(tmp_path, r).status for r in (a, b)] == [Status.FAILED] * 2
    assert extractor.slept == []


def test_a_host_that_keeps_answering_429_is_left_after_three_waits(stub, tmp_path):
    stub.work("10.1234/a", location(stub.serve("/repo/a.pdf", (429, b"")), "repository"))
    extractor = pdf_source(tmp_path)

    result = _download(extractor, "10.1234/a")[0]

    assert extractor.slept == [2.0, 4.0, 8.0]
    assert stub.fetched == ["/repo/a.pdf"] * 4
    assert result.url_attempts[0]["reason"].endswith("kept answering 429")
    assert _stage_outcome(tmp_path, result).status is Status.FAILED


def test_no_worker_asks_a_host_before_the_time_it_gave():
    now = [0.0]
    slept: list[float] = []
    gate = _HostGate(lambda: now[0], slept.append)

    assert gate.rate_limited("h", 30.0) is None
    now[0] = 10.0
    gate.wait("h")  # another worker, 10s later, waits out the remaining 20s
    gate.wait("elsewhere")

    assert slept == [20.0]


def test_a_host_refusing_every_url_is_left_for_a_later_run(stub, tmp_path):
    dois = [f"10.1234/r{n}" for n in range(22)]
    for doi in dois:
        stub.work(doi, location(stub.serve(f"/{doi}.pdf", (403, b"no")), "journal"))

    results = _download(pdf_source(tmp_path), *dois)

    assert len(stub.fetched) == 20
    assert [r.url_attempts[0]["outcome"] for r in results[20:]] == ["deferred", "deferred"]
    assert "refused 20 URLs this run and served no PDF" in results[21].error_message
    assert results[0].url_attempts[0]["dead"] is True
    assert "dead" not in results[21].url_attempts[0]


def test_a_host_that_has_served_a_pdf_is_not_cut_off(stub, tmp_path):
    dois = [f"10.1234/r{n}" for n in range(22)]
    stub.work(dois[0], location(stub.serve("/first.pdf", (200, PDF)), "journal"))
    for doi in dois[1:]:
        stub.work(doi, location(stub.serve(f"/{doi}.pdf", (403, b"no")), "journal"))

    _download(pdf_source(tmp_path), *dois)

    assert len(stub.fetched) == 22


# -- versions -----------------------------------------------------------------------


def test_a_url_both_providers_list_keeps_the_openalex_version(stub, other_host, tmp_path):
    preprint = stub.serve("/repo/a.pdf", (200, PDF))
    published = other_host.serve("/journal/a.pdf", (403, b"no"))
    stub.work(
        "10.1234/a",
        location(preprint, "repository", version="submittedVersion", name="bioRxiv"),
        location(published, "journal"),
    )
    stub.s2["DOI:10.1234/a"] = preprint

    result = _download(pdf_source(tmp_path), "10.1234/a")[0]

    assert other_host.fetched == ["/journal/a.pdf"]  # the published copy is tried first
    copy = result.url_attempts[1]
    assert copy["provider"] == "openalex"
    assert copy["also_listed_by"] == ["semantic_scholar"]
    assert copy["version"] == "submittedVersion"
    assert copy["location_type"] == "repository"
    assert result.article_version == "submittedVersion"

    outcome = _stage_outcome(tmp_path, result)
    assert outcome.summary["article_version"] == outcome.payload["article_version"]
    assert outcome.payload["article_version"] == "submittedVersion"
    assert DownloadResult.from_dict(result.to_dict()).article_version == "submittedVersion"


def test_an_unlabelled_copy_on_a_preprint_server_counts_as_a_preprint():
    biorxiv = PdfCandidate("https://www.biorxiv.org/content/1v1.full.pdf", "semantic_scholar",
                           "unknown")
    accepted = PdfCandidate("https://example.org/a.pdf", "openalex", "unknown",
                            version="acceptedVersion")

    ordered = _try_order([biorxiv, accepted])

    assert [c.url for c in ordered] == [accepted.url, biorxiv.url]
    assert ordered[1].version == "submittedVersion"


def test_a_preprint_label_from_openalex_is_not_overwritten_by_the_host_rule():
    labelled = PdfCandidate("https://osf.io/a/download", "openalex", "repository",
                            version="acceptedVersion")

    assert _try_order([labelled])[0].version == "acceptedVersion"


def test_the_extraction_says_which_version_its_text_came_from(tmp_path, monkeypatch):
    from ingestion_workflow.extractors import docling_convert
    from ingestion_workflow.models import DownloadedFile, FileType

    class _Document:
        tables: list = []
        pictures: list = []

        def export_to_text(self):
            return "Results were reported in MNI space."

    monkeypatch.setattr(docling_convert, "convert_pdf", lambda path: _Document())
    pdf = tmp_path / "a.pdf"
    pdf.write_bytes(PDF)
    download = DownloadResult(
        identifier=Identifier(doi="10.1234/a"),
        source=DownloadSource.PDF,
        success=True,
        files=[DownloadedFile(pdf, FileType.PDF, "application/pdf", DownloadSource.PDF)],
        article_version="submittedVersion",
    )

    content = pdf_extractor._extract_pdf_article(download, tmp_path / "out")

    assert content.article_version == "submittedVersion"
    assert ExtractedContent.from_dict(content.to_dict()).article_version == "submittedVersion"


# -- OpenAlex batch failures ----------------------------------------------------------


def test_a_failed_openalex_batch_is_blamed_only_on_its_articles(stub, tmp_path, monkeypatch):
    monkeypatch.setattr(OpenAlexClient._request_openalex.retry, "wait", wait_none())
    stub.openalex_failing_types = {"pmid"}

    unknown, pmid_only = pdf_source(tmp_path).download(
        Identifiers([Identifier(doi="10.1234/unknown"), Identifier(pmid="12345")])
    )

    assert unknown.url_attempts == []
    assert unknown.error_message == "No open-access PDF URL found."
    assert _outcomes(pmid_only) == ["lookup_failed"]
    assert "pmid batch of 1" in pmid_only.error_message


def test_a_failed_openalex_batch_keeps_what_the_other_batches_found(stub, tmp_path, monkeypatch):
    monkeypatch.setattr(OpenAlexClient._request_openalex.retry, "wait", wait_none())
    stub.openalex_failing_types = {"pmid"}
    stub.work("10.1234/a", location(stub.serve("/repo/a.pdf", (200, PDF)), "repository"))

    found, pmid_only = pdf_source(tmp_path, semantic_scholar_api_key=None).download(
        Identifiers([Identifier(doi="10.1234/a"), Identifier(pmid="12345")])
    )

    assert found.success and _outcomes(found) == ["pdf"]
    assert stub.fetched == ["/repo/a.pdf"]
    assert _outcomes(pmid_only) == ["lookup_failed"]
    assert "pmid batch of 1" in pmid_only.error_message


def test_an_article_its_pmid_batch_answered_did_not_fail(stub, tmp_path, monkeypatch):
    monkeypatch.setattr(OpenAlexClient._request_openalex.retry, "wait", wait_none())
    stub.openalex_failing_types = {"doi"}
    stub.work(None, location(stub.serve("/repo/a.pdf", (200, PDF)), "repository"), pmid="12345")

    (result,) = pdf_source(tmp_path, semantic_scholar_api_key=None).download(
        Identifiers([Identifier(doi="10.1234/a", pmid="12345")])
    )

    assert result.success and _outcomes(result) == ["pdf"]


def test_the_pmid_pass_skips_an_article_the_doi_pass_answered(stub):
    stub.work("10.1234/a", pmid="12345")

    OpenAlexClient(email=EMAIL).get_locations(
        Identifiers([Identifier(doi="10.1234/a", pmid="12345")])
    )

    assert [q["filter"][0].split(":")[0] for q in stub.openalex_queries] == ["doi"]


def test_openalex_s_best_copy_is_tried_first_among_equal_copies(stub, tmp_path):
    first = location(stub.serve("/repo/a.pdf", (200, PDF)), "repository")
    best = location(stub.serve("/repo/b.pdf", (200, PDF)), "repository")
    stub.work("10.1234/a", first, best, best=best)

    _download(pdf_source(tmp_path), "10.1234/a")

    assert stub.fetched == ["/repo/b.pdf"]


def test_openalex_is_not_asked_without_an_email_or_a_key(stub, tmp_path):
    extractor = pdf_source(tmp_path, openalex_email=None, semantic_scholar_api_key=None)

    result = _download(extractor, "10.1234/a")[0]

    assert stub.openalex_queries == []
    assert result.url_attempts == []
    assert result.error_message == "No open-access PDF URL found."


def test_a_work_with_no_location_still_answers_for_its_article(stub):
    stub.work("10.1234/a")
    identifiers = Identifiers([Identifier(doi="10.1234/a")])

    assert OpenAlexClient(email=EMAIL).get_locations(identifiers) == {identifiers[0].slug: []}


# -- the shape OpenAlex gives a location ----------------------------------------------


def test_a_location_with_a_pdf_link_and_a_landing_page_fetches_the_pdf(stub, tmp_path):
    pdf = stub.serve("/repo/a.pdf", (200, PDF))
    landing = stub.serve("/repo/a", (200, HTML))
    stub.work("10.1234/a", location(pdf, "repository", landing=landing))

    result = _download(pdf_source(tmp_path), "10.1234/a")[0]

    assert result.success
    assert stub.fetched == ["/repo/a.pdf"]
    assert result.url_attempts[0]["url"] == pdf


PLOS_PDF = (
    "https://journals.plos.org/plosone/article/file?id=10.1371/journal.pone.0068910"
    "&type=printable"
)


@pytest.mark.vcr
def test_a_real_openalex_listing_fetches_the_pdf_link_not_the_landing_page(tmp_path):
    session = _LocalOnlySession()  # the cassette answers OpenAlex; no file host is reached
    extractor = pdf_source(tmp_path, session=session, semantic_scholar_api_key=None)

    result = _download(extractor, "10.1371/journal.pone.0068910")[0]

    assert session.refused == [PLOS_PDF]
    assert result.url_attempts[0]["url"] == PLOS_PDF
    assert result.url_attempts[0]["location_type"] == "publisher"
    assert "landing_page_only" in _outcomes(result)


# -- what counts, and how often -------------------------------------------------------


def test_each_outcome_is_counted_as_often_as_it_happens(stub, other_host, tmp_path):
    first = stub.serve("/repo/a.pdf", (403, b"no"))
    second = other_host.serve("/journal/a.pdf", (403, b"no"))
    stub.work("10.1234/a", location(first, "repository"), location(second, "journal"))

    result = _download(pdf_source(tmp_path), "10.1234/a")[0]

    assert _stage_outcome(tmp_path, result).summary["url_outcomes"] == {"http_error": 2}


def test_a_host_answering_server_errors_is_not_refusing(stub, tmp_path):
    dois = [f"10.1234/s{n}" for n in range(22)]
    for doi in dois:
        stub.work(doi, location(stub.serve(f"/{doi}.pdf", (503, b"down")), "journal"))

    results = _download(pdf_source(tmp_path), *dois)

    assert len(stub.fetched) == 22
    assert {r.url_attempts[0]["outcome"] for r in results} == {"http_error"}


def test_semantic_scholar_is_not_asked_without_a_key(stub, tmp_path):
    stub.s2["DOI:10.1234/a"] = stub.serve("/s2/a.pdf", (200, PDF))

    result = _download(pdf_source(tmp_path, semantic_scholar_api_key=None), "10.1234/a")[0]

    assert stub.s2_queries == [] and stub.fetched == []
    assert result.error_message == "No open-access PDF URL found."


def test_every_fetch_is_streamed_with_a_timeout(stub, tmp_path):
    stub.work("10.1234/a", location(stub.serve("/repo/a.pdf", (200, PDF)), "repository"))
    session = _LocalOnlySession()

    assert _download(pdf_source(tmp_path, session=session), "10.1234/a")[0].success
    assert session.asked == [{"timeout": pdf_extractor.DOWNLOAD_TIMEOUT, "stream": True}]


def test_a_europe_pmc_preprint_pdf_is_fetched(stub, tmp_path):
    preprint = "https://europepmc.org/api/fulltextRepo?pprId=PPR1&type=FILE&fileName=a.pdf"
    stub.work("10.1234/a", location(preprint, "repository", version="submittedVersion"))
    session = _LocalOnlySession()

    result = _download(pdf_source(tmp_path, session=session), "10.1234/a")[0]

    assert session.refused == [preprint]
    assert _outcomes(result) == ["fetch_error"]


# -- a host left for the run ----------------------------------------------------------


def test_a_host_left_for_the_run_meanwhile_is_not_asked(stub, tmp_path):
    url = stub.serve("/repo/a.pdf", (200, PDF))
    extractor = pdf_source(tmp_path)
    extractor._hosts.rate_limited(_host(url), 3600.0)  # another worker's 429
    attempt: dict = {}

    assert extractor._fetch(url, attempt) is None
    assert stub.fetched == []
    assert attempt == {
        "outcome": "deferred", "reason": f"deferred: {_host(url)} asked to wait 3600s"
    }


def test_a_url_on_a_host_left_for_the_run_does_not_become_the_error(stub, other_host, tmp_path):
    other_host.serve("/journal/a.pdf", (429, b"", {"Retry-After": "900"}))
    stub.work("10.1234/a", location(other_host.url + "/journal/a.pdf", "journal"))
    forbidden = stub.serve("/repo/b.pdf", (403, b"no"))
    stub.work("10.1234/b", location(forbidden, "repository"),
              location(other_host.url + "/journal/b.pdf", "journal"))

    _, b = _download(pdf_source(tmp_path), "10.1234/a", "10.1234/b")

    assert other_host.fetched == ["/journal/a.pdf"]
    assert _outcomes(b) == ["http_error", "deferred"]
    assert "403" in b.error_message


# -- URLs found dead on an earlier run ------------------------------------------------


@pytest.fixture
def catalog(tmp_path):
    with Catalog.open(tmp_path / "catalog") as opened:
        yield opened


def _run(tmp_path, catalog, ref, refresh=(), **settings):
    """One run of the download stage's PDF source, recorded in the catalog."""
    stage = DownloadStage(pdf_source(tmp_path).settings)
    # a new run: a new host gate
    stage._extractors[DownloadSource.PDF] = pdf_source(tmp_path, **settings)
    ctx = Context(stage.settings, catalog, refresh=refresh)
    outcomes = list(stage._download(ctx, DownloadSource.PDF, [ref]))
    catalog.record(outcomes)
    return outcomes


def test_a_later_run_does_not_fetch_a_dead_url(stub, other_host, tmp_path, catalog):
    dead = stub.serve("/repo/a.pdf", (403, b"no"))
    busy = other_host.serve("/journal/a.pdf", (429, b"", {"Retry-After": "3600"}))
    stub.work("10.1234/a", location(dead, "repository"), location(busy, "journal"))
    ref = catalog.register(Identifier(doi="10.1234/a"))
    (first,) = _run(tmp_path, catalog, ref)
    assert first.status is Status.FAILED

    other_host.serve("/journal/a.pdf", (200, PDF))
    (outcome,) = _run(tmp_path, catalog, ref)

    assert outcome.status is Status.OK
    assert stub.fetched == ["/repo/a.pdf"]  # only on the first run
    kinds = [a["outcome"] for a in outcome.summary["url_attempts"]]
    assert kinds == ["known_dead", "pdf"]
    assert "403" in outcome.summary["url_attempts"][0]["reason"]


def test_a_dead_url_is_fetched_once_and_a_new_copy_is_still_tried(stub, tmp_path, catalog):
    stub.work("10.1234/a", location(stub.serve("/repo/a.pdf", (403, b"no")), "repository"))
    ref = catalog.register(Identifier(doi="10.1234/a"))

    runs = [_run(tmp_path, catalog, ref)[0] for _ in range(3)]

    assert stub.fetched == ["/repo/a.pdf"]
    assert [o.status for o in runs] == [Status.FAILED] * 3
    assert [o.summary["url_outcomes"] for o in runs] == [
        {"http_error": 1}, {"known_dead": 1}, {"known_dead": 1}
    ]
    assert "403" in runs[2].error

    stub.works[0]["locations"].append(location(stub.serve("/repo/b.pdf", (200, PDF)), "journal"))
    (found,) = _run(tmp_path, catalog, ref)

    assert found.status is Status.OK
    assert stub.fetched == ["/repo/a.pdf", "/repo/b.pdf"]


def test_a_refresh_still_skips_a_dead_url(stub, tmp_path, catalog):
    stub.work("10.1234/a", location(stub.serve("/repo/a.pdf", (403, b"no")), "repository"))
    ref = catalog.register(Identifier(doi="10.1234/a"))
    _run(tmp_path, catalog, ref)
    stub.works[0]["locations"] = [location(stub.serve("/repo/b.pdf", (200, PDF)), "journal")]
    (found,) = _run(tmp_path, catalog, ref)  # the dead copy is not listed this time
    stub.works[0]["locations"].insert(0, location(stub.url + "/repo/a.pdf", "repository"))

    (refreshed,) = _run(tmp_path, catalog, ref, refresh=["download:pdf"])

    assert stub.fetched == ["/repo/a.pdf", "/repo/b.pdf", "/repo/b.pdf"]
    assert [a["outcome"] for a in found.summary["url_attempts"]] == ["pdf", "known_dead"]
    assert [a["outcome"] for a in refreshed.summary["url_attempts"]] == ["known_dead", "pdf"]


@pytest.mark.parametrize(
    "lost, middle_outcomes",
    [("lookup", {"lookup_failed": 1, "known_dead": 1}), ("outage", {"known_dead": 1})],
)
def test_a_dead_url_is_remembered_through_a_run_that_did_not_list_it(
    stub, tmp_path, catalog, monkeypatch, lost, middle_outcomes
):
    monkeypatch.setattr(OpenAlexClient._request_openalex.retry, "wait", wait_none())
    stub.work("10.1234/a", location(stub.serve("/repo/a.pdf", (403, b"no")), "repository"))
    ref = catalog.register(Identifier(doi="10.1234/a"))
    _run(tmp_path, catalog, ref)

    if lost == "lookup":
        stub.openalex_status = 500
        (middle,) = _run(tmp_path, catalog, ref)
        stub.openalex_status = 200
    else:  # the PDF cache cannot be made, so the whole batch fails
        (tmp_path / "not-a-dir").write_text("")
        (middle,) = _run(tmp_path, catalog, ref, pdf_cache_root=tmp_path / "not-a-dir")
    (last,) = _run(tmp_path, catalog, ref)

    assert middle.status is Status.FAILED
    assert middle.summary["url_outcomes"] == middle_outcomes
    assert stub.fetched == ["/repo/a.pdf"]
    assert last.summary["url_outcomes"] == {"known_dead": 1}
    assert "403" in last.summary["url_attempts"][0]["reason"]


def test_a_dead_url_listed_later_as_a_closed_copy_is_recorded_once_as_dead(
    stub, tmp_path, catalog
):
    url = stub.serve("/repo/a.pdf", (403, b"no"))
    stub.work("10.1234/a", location(url, "repository"))
    ref = catalog.register(Identifier(doi="10.1234/a"))
    _run(tmp_path, catalog, ref)
    stub.works[0]["locations"] = [{**location(url, "repository"), "is_oa": False}]

    (outcome,) = _run(tmp_path, catalog, ref)

    assert [a["outcome"] for a in outcome.summary["url_attempts"]] == ["known_dead"]


def test_the_dead_urls_come_from_the_pdf_download_not_another_source(stub, tmp_path, catalog):
    stub.work("10.1234/a", location(stub.serve("/repo/a.pdf", (403, b"no")), "repository"))
    ref = catalog.register(Identifier(doi="10.1234/a"))
    catalog.record([Outcome.failure(ref.id, "download", "ace", "no page", summary={"files": 0})])

    _run(tmp_path, catalog, ref)
    _run(tmp_path, catalog, ref)

    assert stub.fetched == ["/repo/a.pdf"]


def _host(url):
    return urlsplit(url).netloc
