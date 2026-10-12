"""Every candidate PDF URL is tried, in order, against stub OpenAlex, S2 and file hosts."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

import pytest
from ingestion_workflow.catalog import ArticleRef, Status
from ingestion_workflow.clients.openalex import OpenAlexClient
from ingestion_workflow.clients.semantic_scholar import SemanticScholarClient
from ingestion_workflow.config import Settings
from ingestion_workflow.extractors.pdf_extractor import PdfExtractor
from ingestion_workflow.models import DownloadResult, DownloadSource, Identifier, Identifiers
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
                    if stub.openalex_status != 200:
                        return self._send(stub.openalex_status, b"{}")
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


def _extractor(tmp_path, **overrides) -> PdfExtractor:
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
    extractor = PdfExtractor(settings=settings)
    extractor.slept = []
    extractor._sleep = extractor.slept.append
    return extractor


def _download(extractor, *dois) -> list[DownloadResult]:
    return extractor.download(Identifiers([Identifier(doi=d) for d in dois]))


def _outcomes(result):
    return [a.get("outcome") for a in result.attempts]


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
    assert result.attempts == [
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
    assert ok.summary["attempts"] == ok.payload["attempts"] == result.attempts


def test_a_403_then_a_page_that_is_not_a_pdf_fall_through_to_the_next_url(stub, tmp_path):
    pmc = stub.serve("/europepmc/a.pdf", (403, b"Forbidden"))
    journal = stub.serve("/journal/a.pdf", (200, HTML))
    s2 = stub.serve("/s2/a.pdf", (200, PDF))
    stub.work("10.1234/a", _location(journal, "journal"), _location(pmc, "repository"))
    stub.s2["DOI:10.1234/a"] = s2

    result = _download(_extractor(tmp_path), "10.1234/a")[0]

    assert result.success
    assert stub.fetched == ["/europepmc/a.pdf", "/journal/a.pdf", "/s2/a.pdf"]
    assert [(a["provider"], a["outcome"], a["status"]) for a in result.attempts] == [
        ("openalex", "http_error", 403),
        ("openalex", "not_pdf", 200),
        ("semantic_scholar", "pdf", 200),
    ]
    assert "403" in result.attempts[0]["reason"]
    assert result.attempts[1]["reason"] == f"Response from {journal} is not a PDF."


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
    assert outcome.status is Status.FAILED
    assert outcome.error == result.error_message
    assert outcome.summary["attempts"] == result.attempts
    assert DownloadResult.from_dict(result.to_dict()).attempts == result.attempts


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
    assert result.attempts[3]["reason"] == "not tried: attempt cap of 3 reached"
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


def test_a_host_that_keeps_answering_429_is_left_for_the_next_url(stub, tmp_path):
    busy = stub.serve("/repo/a.pdf", (429, b"slow down", {"Retry-After": "3600"}))
    other = stub.serve("/journal/a.pdf", (200, PDF))
    stub.work("10.1234/a", _location(busy, "repository"), _location(other, "journal"))
    extractor = _extractor(tmp_path)

    result = _download(extractor, "10.1234/a")[0]

    assert result.success
    # three waits, each capped, then the fourth 429 gives the URL up
    assert extractor.slept == [60.0, 60.0, 60.0]
    assert stub.fetched == ["/repo/a.pdf"] * 4 + ["/journal/a.pdf"]
    assert _outcomes(result) == ["rate_limited", "pdf"]
    assert result.attempts[0]["status"] == 429


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
    assert result.attempts[0]["location_type"] == "publisher"


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

    assert [x["outcome"] for x in a.attempts] == ["lookup_failed", "not_pdf"]
    assert a.attempts[0]["provider"] == "openalex"
    assert a.error_message == f"Response from {s2} is not a PDF."
    assert _outcomes(b) == ["lookup_failed"]
    assert b.error_message.startswith("openalex lookup failed:")


def test_an_article_no_provider_knows_still_says_no_pdf_url(stub, tmp_path):
    result = _download(_extractor(tmp_path), "10.1234/unknown")[0]

    assert not result.success
    assert result.error_message == "No open-access PDF URL found."
    assert result.attempts == []


def test_a_location_openalex_does_not_call_open_access_is_not_fetched(stub, tmp_path):
    closed = stub.serve("/journal/closed.pdf", (200, PDF))
    location = {**_location(closed, "journal"), "is_oa": False}
    stub.work("10.1234/a", location)

    result = _download(_extractor(tmp_path), "10.1234/a")[0]

    assert stub.fetched == []
    assert result.error_message == "No open-access PDF URL found."
