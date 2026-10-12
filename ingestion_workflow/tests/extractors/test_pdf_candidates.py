"""Every candidate PDF URL is tried in order, against stub OpenAlex, Semantic Scholar
and file hosts."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

import pytest
import requests
from ingestion_workflow.catalog import ArticleRef, Artifact, Status
from ingestion_workflow.clients.openalex import OpenAlexClient
from ingestion_workflow.clients.semantic_scholar import SemanticScholarClient
from ingestion_workflow.config import Settings
from ingestion_workflow.extractors import pdf_extractor
from ingestion_workflow.extractors.pdf_extractor import (
    SKIPPED_PMC,
    PdfCandidate,
    PdfExtractor,
    _HostGate,
    _try_order,
    remembered_dead_urls,
)
from ingestion_workflow.models import (
    DownloadResult,
    DownloadSource,
    ExtractedContent,
    Identifier,
    Identifiers,
)
from ingestion_workflow.pipeline.stages.download import DownloadStage
from tenacity import wait_none

PDF = b"%PDF-1.4\n%%EOF\n"
HTML = b"<html><body>Read this article on our site</body></html>"
EMAIL = "pipeline@example.org"


class _Stub:
    """One local server playing OpenAlex, Semantic Scholar and every file host."""

    def __init__(self) -> None:
        self.works: list[dict] = []
        self.s2: dict[str, str] = {}  # "DOI:..." -> openAccessPdf url
        self.files: dict[str, list[tuple[int, bytes, dict]]] = {}
        self.openalex_status = 200
        self.openalex_failing_types: set[str] = set()
        self.fetched: list[str] = []
        self.openalex_queries: list[dict] = []
        stub = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def _send(self, status, body, headers=None):
                self.send_response(status)
                for key, value in (headers or {}).items():
                    self.send_header(key, value)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                parts = urlsplit(self.path)
                if parts.path == "/works":
                    query = parse_qs(parts.query)
                    stub.openalex_queries.append(query)
                    id_type = query["filter"][0].split(":", 1)[0]
                    if stub.openalex_status != 200 or id_type in stub.openalex_failing_types:
                        return self._send(500 if stub.openalex_status == 200 else
                                          stub.openalex_status, b"{}")
                    wanted = query["filter"][0].split(":", 1)[1].split("|")
                    results = [
                        w for w in stub.works if w["ids"]["doi"].split("doi.org/")[1] in wanted
                    ]
                    return self._send(200, json.dumps({"results": results}).encode())
                stub.fetched.append(parts.path)
                queue = stub.files.get(parts.path)
                if not queue:
                    return self._send(404, b"not found")
                status, body, headers = queue.pop(0) if len(queue) > 1 else queue[0]
                return self._send(status, body, headers)

            def do_POST(self):
                length = int(self.headers.get("Content-Length") or 0)
                ids = json.loads(self.rfile.read(length))["ids"]
                records = [
                    {"title": "t", "openAccessPdf": {"url": stub.s2[i]}} if i in stub.s2 else None
                    for i in ids
                ]
                return self._send(200, json.dumps(records).encode())

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def serve(self, path: str, *responses) -> str:
        self.files[path] = [r if len(r) == 3 else (*r, {}) for r in responses]
        return self.url + path

    def work(self, doi: str, *locations: dict, best: dict | None = None) -> None:
        self.works.append(
            {
                "ids": {"doi": f"https://doi.org/{doi}"},
                "best_oa_location": best,
                "locations": list(locations),
            }
        )


def _location(pdf_url=None, kind="journal", landing=None, version="publishedVersion", name=None):
    return {
        "is_oa": True,
        "pdf_url": pdf_url,
        "landing_page_url": landing,
        "version": version,
        "source": {"type": kind, "display_name": name or kind},
    }


@pytest.fixture
def stub(monkeypatch):
    server = _Stub()
    monkeypatch.setattr(OpenAlexClient, "BASE_URL", server.url)
    monkeypatch.setattr(SemanticScholarClient, "BASE_URL", server.url + "/graph/v1/paper/batch")
    yield server
    server.server.shutdown()
    server.server.server_close()


def _extractor(tmp_path, session=None, **overrides) -> PdfExtractor:
    settings = Settings(
        cache_root=tmp_path / "cache",
        data_root=tmp_path / "data",
        pdf_cache_root=tmp_path / "pdfs",
        max_workers=1,
        openalex_email=EMAIL,
        openalex_api_key=None,
        semantic_scholar_api_key="s2-key",
        **overrides,
    )
    extractor = PdfExtractor(settings=settings, session=session or requests.Session())
    extractor.slept = []
    now = [1000.0]

    def sleep(delay):
        extractor.slept.append(delay)
        now[0] += delay

    extractor._sleep = sleep
    extractor._clock = lambda: now[0]
    return extractor


def _download(extractor, *dois) -> list[DownloadResult]:
    return extractor.download(Identifiers([Identifier(doi=d) for d in dois]))


def _outcomes(result):
    return [a.get("outcome") for a in result.url_attempts]


def test_a_repository_copy_is_tried_before_the_publisher_copy(stub, tmp_path):
    publisher = stub.serve("/journal/a.pdf", (200, PDF))
    repository = stub.serve("/escholarship/a.pdf", (200, PDF))
    best = _location(publisher, "journal")
    copy = _location(repository, "repository", name="eScholarship")
    stub.work("10.1234/a", best, copy, best=best)

    result = _download(_extractor(tmp_path), "10.1234/a")[0]

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
    stage = DownloadStage(_extractor(tmp_path).settings)
    ref = ArticleRef(id="art1", identifier=result.identifier)
    ok = stage._outcome(ref, DownloadSource.PDF, result, "fp")
    assert ok.status is Status.OK
    assert ok.summary["url_attempts"] == ok.payload["url_attempts"] == result.url_attempts


def test_a_403_then_a_page_that_is_not_a_pdf_fall_through_to_the_next_url(stub, tmp_path):
    pmc = stub.serve("/europepmc/a.pdf", (403, b"Forbidden"))
    journal = stub.serve("/journal/a.pdf", (200, HTML))
    s2 = stub.serve("/s2/a.pdf", (200, PDF))
    stub.work("10.1234/a", _location(journal, "journal"), _location(pmc, "repository"))
    stub.s2["DOI:10.1234/a"] = s2

    result = _download(_extractor(tmp_path), "10.1234/a")[0]

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
        _location(first, "repository"),
        _location(second, "journal"),
        _location(None, "repository", landing=landing),
    )

    result = _download(_extractor(tmp_path), "10.1234/a")[0]

    assert not result.success
    assert _outcomes(result) == ["http_error", "not_pdf", "landing_page_only"]
    assert result.error_message == f"Response from {second} is not a PDF."
    assert "/landing/a" not in stub.fetched

    ref = ArticleRef(id="art1", identifier=result.identifier)
    stage = DownloadStage(_extractor(tmp_path).settings)
    outcome = stage._outcome(ref, DownloadSource.PDF, result, "fp")
    # every copy failed for good: a 403, an HTML page and a landing page
    assert outcome.status is Status.PERMANENT
    assert outcome.error == result.error_message
    assert outcome.summary["url_attempts"] == result.url_attempts
    assert DownloadResult.from_dict(result.to_dict()).url_attempts == result.url_attempts


def test_an_open_access_landing_page_alone_is_recorded_not_fetched(stub, tmp_path):
    landing = stub.url + "/landing/a"
    stub.serve("/landing/a", (200, HTML))
    stub.work("10.1234/a", _location(None, "repository", landing=landing))

    result = _download(_extractor(tmp_path), "10.1234/a")[0]

    assert not result.success
    assert result.error_message == f"OA landing page only: {landing}"
    assert stub.fetched == []


def test_the_attempt_cap_stops_fetching_and_records_the_rest(stub, tmp_path):
    urls = [stub.serve(f"/repo/{n}.pdf", (403, b"no")) for n in range(5)]
    stub.work("10.1234/a", *[_location(url, "repository") for url in urls])

    result = _download(_extractor(tmp_path, pdf_max_attempts=3), "10.1234/a")[0]

    assert stub.fetched == ["/repo/0.pdf", "/repo/1.pdf", "/repo/2.pdf"]
    assert _outcomes(result) == ["http_error"] * 3 + ["not_tried"] * 2
    assert result.url_attempts[3]["reason"] == "not tried: attempt cap of 3 reached"
    assert "403" in result.error_message


def test_a_429_is_waited_out_as_the_host_asks(stub, tmp_path):
    url = stub.serve(
        "/repo/a.pdf",
        (429, b"slow down", {"Retry-After": "7"}),
        (429, b"slow down", {"Retry-After": "7"}),
        (200, PDF),
    )
    stub.work("10.1234/a", _location(url, "repository"))
    extractor = _extractor(tmp_path)

    result = _download(extractor, "10.1234/a")[0]

    assert result.success
    assert extractor.slept == [7.0, 7.0]
    assert stub.fetched == ["/repo/a.pdf"] * 3


def test_a_host_asking_to_wait_past_the_cap_is_left_for_the_run(stub, tmp_path):
    other_host = _Stub()
    try:
        busy = stub.serve("/repo/a.pdf", (429, b"slow down", {"Retry-After": "3600"}))
        other = other_host.serve("/journal/a.pdf", (200, PDF))
        stub.work("10.1234/a", _location(busy, "repository"), _location(other, "journal"))
        extractor = _extractor(tmp_path)

        result = _download(extractor, "10.1234/a")[0]
    finally:
        other_host.server.shutdown()
        other_host.server.server_close()

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
    stub.work("10.1234/a", _location(url, "repository"))
    extractor = _extractor(tmp_path)

    assert _download(extractor, "10.1234/a")[0].success
    assert extractor.slept == [2.0, 4.0]


def test_a_url_both_providers_list_is_fetched_once(stub, tmp_path):
    url = stub.serve("/europepmc/a.pdf", (403, b"Forbidden"))
    stub.work("10.1234/a", _location(url, "repository"))
    stub.s2["DOI:10.1234/a"] = url.replace("http://", "HTTP://") + "/"

    result = _download(_extractor(tmp_path), "10.1234/a")[0]

    assert stub.fetched == ["/europepmc/a.pdf"]
    assert _outcomes(result) == ["http_error"]


def test_a_preprint_is_tried_after_the_published_copy(stub, tmp_path):
    preprint = stub.serve("/biorxiv/a.pdf", (200, PDF))
    published = stub.serve("/journal/a.pdf", (200, PDF))
    stub.work(
        "10.1234/a",
        _location(preprint, "repository", version="submittedVersion"),
        _location(published, "journal"),
    )

    result = _download(_extractor(tmp_path), "10.1234/a")[0]

    assert stub.fetched == ["/journal/a.pdf"]
    assert result.url_attempts[0]["location_type"] == "publisher"


def test_openalex_is_asked_about_articles_semantic_scholar_answered(stub, tmp_path):
    s2 = stub.serve("/s2/a.pdf", (403, b"Forbidden"))
    repo = stub.serve("/repo/a.pdf", (200, PDF))
    stub.s2["DOI:10.1234/a"] = s2
    stub.work("10.1234/a", _location(repo, "repository"))

    result = _download(_extractor(tmp_path), "10.1234/a", "10.1234/b")[0]

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

    a, b = _download(_extractor(tmp_path), "10.1234/a", "10.1234/b")

    assert [x["outcome"] for x in a.url_attempts] == ["lookup_failed", "not_pdf"]
    assert a.url_attempts[0]["provider"] == "openalex"
    assert a.error_message == f"Response from {s2} is not a PDF."
    assert _outcomes(b) == ["lookup_failed"]
    assert b.error_message.startswith("openalex lookup failed:")


def test_an_article_no_provider_knows_still_says_no_pdf_url(stub, tmp_path):
    result = _download(_extractor(tmp_path), "10.1234/unknown")[0]

    assert not result.success
    assert result.error_message == "No open-access PDF URL found."
    assert result.url_attempts == []


def test_a_location_openalex_does_not_call_open_access_is_not_fetched(stub, tmp_path):
    closed = stub.serve("/journal/closed.pdf", (200, PDF))
    location = {**_location(closed, "journal"), "is_oa": False}
    stub.work("10.1234/a", location)

    result = _download(_extractor(tmp_path), "10.1234/a")[0]

    assert stub.fetched == []
    assert result.error_message == "No open-access PDF URL found."


class _LocalOnlySession(requests.Session):
    """Refuses any host but the stub's, so a test never reaches the internet."""

    def __init__(self) -> None:
        super().__init__()
        self.refused: list[str] = []

    def get(self, url, **kwargs):
        if urlsplit(url).hostname != "127.0.0.1":
            self.refused.append(url)
            raise requests.ConnectionError(f"test refuses {url}")
        return super().get(url, **kwargs)


@pytest.fixture
def other_host():
    server = _Stub()
    yield server
    server.server.shutdown()
    server.server.server_close()


def _stage_outcome(tmp_path, result):
    stage = DownloadStage(_extractor(tmp_path).settings)
    ref = ArticleRef(id="art1", identifier=result.identifier)
    return stage._outcome(ref, DownloadSource.PDF, result, "fp")


# -- permanence -------------------------------------------------------------------


def test_a_rate_limited_copy_keeps_an_article_retryable_after_a_404(stub, other_host, tmp_path):
    busy = stub.serve("/repo/a.pdf", (429, b"", {"Retry-After": "3600"}))
    gone = other_host.serve("/journal/a.pdf", (404, b"gone"))
    stub.work("10.1234/a", _location(busy, "repository"), _location(gone, "journal"))

    result = _download(_extractor(tmp_path), "10.1234/a")[0]

    assert _outcomes(result) == ["rate_limited", "http_error"]
    assert "404" in result.error_message
    assert result.permanent is False
    assert _stage_outcome(tmp_path, result).status is Status.FAILED


def test_an_article_whose_every_copy_failed_for_good_is_permanent(stub, tmp_path):
    forbidden = stub.serve("/journal/a.pdf", (403, b"no"))
    html = stub.serve("/repo/a.pdf", (200, HTML))
    landing = stub.url + "/landing/a"
    stub.work(
        "10.1234/a",
        _location(forbidden, "journal"),
        _location(html, "repository"),
        _location(None, "repository", landing=landing),
    )

    result = _download(_extractor(tmp_path), "10.1234/a")[0]

    assert _outcomes(result) == ["not_pdf", "http_error", "landing_page_only"]
    assert result.permanent is True
    outcome = _stage_outcome(tmp_path, result)
    assert outcome.status is Status.PERMANENT
    assert outcome.summary["url_outcomes"] == {
        "not_pdf": 1, "http_error": 1, "landing_page_only": 1
    }


def test_a_failed_lookup_keeps_an_article_retryable(stub, tmp_path, monkeypatch):
    monkeypatch.setattr(OpenAlexClient._request_openalex.retry, "wait", wait_none())
    stub.openalex_status = 500
    stub.s2["DOI:10.1234/a"] = stub.serve("/s2/a.pdf", (404, b"gone"))

    result = _download(_extractor(tmp_path), "10.1234/a")[0]

    assert _outcomes(result) == ["lookup_failed", "http_error"]
    assert result.permanent is False
    assert _stage_outcome(tmp_path, result).status is Status.FAILED


def test_a_server_error_or_timeout_is_not_final(stub, tmp_path):
    stub.work("10.1234/a", _location(stub.serve("/repo/a.pdf", (503, b"down")), "repository"))
    stub.work("10.1234/b", _location(stub.url.replace("127.0.0.1", "localhost") + "/x.pdf",
                                     "repository"))

    a, b = _download(_extractor(tmp_path, session=_LocalOnlySession()), "10.1234/a", "10.1234/b")

    assert _outcomes(a) == ["http_error"] and a.permanent is False
    assert _outcomes(b) == ["fetch_error"] and b.permanent is False


def test_an_article_with_no_candidate_is_not_permanent(stub, tmp_path):
    result = _download(_extractor(tmp_path), "10.1234/unknown")[0]

    assert result.permanent is False
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
    stub.work("10.1234/a", *[_location(url, "repository") for url in pmc[:2]],
              _location(pubmed, "journal"), _location(local, None))
    stub.s2["DOI:10.1234/a"] = pmc[2]  # untyped, as Semantic Scholar gives it
    session = _LocalOnlySession()

    result = _download(_extractor(tmp_path, session=session), "10.1234/a")[0]

    assert result.success
    assert session.refused == [pubmed]  # a PubMed URL is not a PMC copy
    skipped = [a for a in result.url_attempts if a["outcome"] == "skipped_pmc"]
    assert sorted(a["url"] for a in skipped) == sorted(pmc)
    assert {a["reason"] for a in skipped} == {SKIPPED_PMC}
    assert all(a["permanent"] for a in skipped)
    assert "status" not in skipped[0]


# -- per-host backoff --------------------------------------------------------------


def test_a_host_left_for_the_run_is_not_asked_again_for_other_articles(
    stub, other_host, tmp_path
):
    stub.serve("/repo/a.pdf", (429, b"", {"Retry-After": "900"}))
    stub.serve("/repo/b.pdf", (200, PDF))
    other = other_host.serve("/journal/b.pdf", (403, b"no"))
    stub.work("10.1234/a", _location(stub.url + "/repo/a.pdf", "repository"))
    stub.work("10.1234/b", _location(stub.url + "/repo/b.pdf", "repository"),
              _location(other, "journal"))
    extractor = _extractor(tmp_path)

    a, b = _download(extractor, "10.1234/a", "10.1234/b")

    assert stub.fetched == ["/repo/a.pdf"]
    assert _outcomes(b) == ["deferred", "http_error"]
    assert b.url_attempts[0]["reason"] == f"deferred: {_host(stub.url)} asked to wait 900s"
    assert b.permanent is False and a.permanent is False
    assert extractor.slept == []


def test_a_host_that_keeps_answering_429_is_left_after_three_waits(stub, tmp_path):
    stub.work("10.1234/a", _location(stub.serve("/repo/a.pdf", (429, b"")), "repository"))
    extractor = _extractor(tmp_path)

    result = _download(extractor, "10.1234/a")[0]

    assert extractor.slept == [2.0, 4.0, 8.0]
    assert stub.fetched == ["/repo/a.pdf"] * 4
    assert result.url_attempts[0]["reason"].endswith("kept answering 429")
    assert result.permanent is False


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
        stub.work(doi, _location(stub.serve(f"/{doi}.pdf", (403, b"no")), "journal"))

    results = _download(_extractor(tmp_path), *dois)

    assert len(stub.fetched) == 20
    assert [r.url_attempts[0]["outcome"] for r in results[20:]] == ["deferred", "deferred"]
    assert "refused 20 URLs this run and served no PDF" in results[21].error_message
    assert results[21].permanent is False and results[0].permanent is True


def test_a_host_that_has_served_a_pdf_is_not_cut_off(stub, tmp_path):
    dois = [f"10.1234/r{n}" for n in range(22)]
    stub.work(dois[0], _location(stub.serve("/first.pdf", (200, PDF)), "journal"))
    for doi in dois[1:]:
        stub.work(doi, _location(stub.serve(f"/{doi}.pdf", (403, b"no")), "journal"))

    _download(_extractor(tmp_path), *dois)

    assert len(stub.fetched) == 22


# -- versions -----------------------------------------------------------------------


def test_a_url_both_providers_list_keeps_the_openalex_version(stub, other_host, tmp_path):
    preprint = stub.serve("/repo/a.pdf", (200, PDF))
    published = other_host.serve("/journal/a.pdf", (403, b"no"))
    stub.work(
        "10.1234/a",
        _location(preprint, "repository", version="submittedVersion", name="bioRxiv"),
        _location(published, "journal"),
    )
    stub.s2["DOI:10.1234/a"] = preprint

    result = _download(_extractor(tmp_path), "10.1234/a")[0]

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

    unknown, pmid_only = _extractor(tmp_path).download(
        Identifiers([Identifier(doi="10.1234/unknown"), Identifier(pmid="12345")])
    )

    assert unknown.url_attempts == []
    assert unknown.error_message == "No open-access PDF URL found."
    assert _outcomes(pmid_only) == ["lookup_failed"]
    assert "pmid batch of 1" in pmid_only.error_message


# -- URLs that failed for good on an earlier run ----------------------------------------


def test_a_later_run_does_not_fetch_a_url_that_failed_for_good(stub, other_host, tmp_path):
    dead = stub.serve("/repo/a.pdf", (403, b"no"))
    busy = other_host.serve("/journal/a.pdf", (429, b"", {"Retry-After": "3600"}))
    stub.work("10.1234/a", _location(dead, "repository"), _location(busy, "journal"))
    first = _download(_extractor(tmp_path), "10.1234/a")[0]
    assert first.permanent is False
    previous = Artifact(
        article_id="art1", stage="download", source="pdf", status=Status.FAILED,
        summary=_stage_outcome(tmp_path, first).summary,
    )

    other_host.serve("/journal/a.pdf", (200, PDF))
    stage = DownloadStage(_extractor(tmp_path).settings)
    stage._extractors[DownloadSource.PDF] = _extractor(tmp_path)  # a new run
    ctx = _Context({"art1": {"pdf": previous}})
    ref = ArticleRef(id="art1", identifier=Identifier(doi="10.1234/a"))

    (outcome,) = list(stage._download(ctx, DownloadSource.PDF, [ref]))

    assert outcome.status is Status.OK
    assert stub.fetched == ["/repo/a.pdf"]  # only on the first run
    kinds = [a["outcome"] for a in outcome.summary["url_attempts"]]
    assert kinds == ["known_dead", "pdf"]
    assert "403" in outcome.summary["url_attempts"][0]["reason"]


def test_a_url_remembered_as_dead_stays_remembered(tmp_path):
    attempts = [
        {"url": "https://a.org/x.pdf", "outcome": "known_dead", "permanent": True, "reason": "r"},
        {"url": "https://a.org/y.pdf", "outcome": "rate_limited", "permanent": False},
        {"url": "https://a.org/z", "outcome": "landing_page_only", "permanent": True},
        {"url": "https://a.org/w.pdf", "outcome": "not_pdf", "permanent": True, "reason": "html"},
    ]

    assert remembered_dead_urls(attempts) == {"a.org/x.pdf?": "r", "a.org/w.pdf?": "html"}


def test_a_refresh_forgets_the_dead_urls():
    previous = Artifact(
        article_id="art1", stage="download", source="pdf", status=Status.FAILED,
        summary={"url_attempts": [
            {"url": "https://a.org/x.pdf", "outcome": "http_error", "permanent": True}
        ]},
    )
    stage = DownloadStage(Settings(max_workers=1))
    ref = ArticleRef(id="art1", identifier=Identifier(doi="10.1234/a"))

    assert stage._dead_urls(_Context({"art1": {"pdf": previous}}), [ref]) == {
        "10-1234-a": {"a.org/x.pdf?": ""}
    }
    assert stage._dead_urls(_Context({"art1": {"pdf": previous}}, refresh=True), [ref]) == {}


class _Context:
    """The part of the stage context the download reads: the catalog and --refresh."""

    def __init__(self, artifacts, refresh=False):
        self.refresh = refresh
        self.catalog = self

        def artifacts_of(ids, stage):
            return {i: artifacts[i] for i in ids if i in artifacts}

        self.artifacts = artifacts_of

    def refreshing(self, stage, source=""):
        return self.refresh


def _host(url):
    return urlsplit(url).netloc
