"""Which failed requests are worth asking again, and how requests are spaced."""

from __future__ import annotations

import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import httpx
import pytest
import requests
import tenacity
from ingestion_workflow.clients.orcid import ORCID_REQUEST_LIMIT, OrcidClient
from ingestion_workflow.clients.sciencedirect import ScienceDirectClient
from ingestion_workflow.utils.http import Pacer, retry_transient, transient


def _status(code: int) -> requests.HTTPError:
    response = requests.Response()
    response.status_code = code
    return requests.HTTPError(f"{code}", response=response)


@pytest.mark.parametrize("exc, expected", [
    (_status(429), True),
    (_status(500), True),
    (_status(503), True),
    (requests.ReadTimeout("read timed out"), True),
    (requests.ConnectionError("connection refused"), True),
    (requests.exceptions.ChunkedEncodingError("connection broken mid-body"), True),
    (requests.JSONDecodeError("Expecting value", "<html>Bad gateway</html>", 0), True),
    (_status(400), False),
    (_status(403), False),
    (_status(404), False),
])
def test_a_provider_down_or_busy_is_asked_again_and_a_refusal_is_not(exc, expected):
    assert transient(exc) is expected


def test_httpx_failures_are_judged_as_requests_ones_are():
    request = httpx.Request("GET", "https://example.org")
    refused = lambda code: httpx.HTTPStatusError(  # noqa: E731
        "x", request=request, response=httpx.Response(code, request=request))
    answers = [transient(refused(code)) for code in (429, 503, 400, 404)]
    assert answers == [True, True, False, False]
    assert transient(httpx.ConnectError("refused", request=request))
    assert transient(httpx.ReadTimeout("slow", request=request))


def test_requests_are_kept_an_interval_apart_across_threads():
    pacer = Pacer()
    stamps = []

    def ask():
        pacer.wait(0.05)
        stamps.append(time.monotonic())

    threads = [threading.Thread(target=ask) for _ in range(5)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    gaps = [b - a for a, b in zip(sorted(stamps), sorted(stamps)[1:])]
    assert min(gaps) >= 0.045


def test_a_failure_is_asked_again_to_the_attempts_and_then_raised_as_it_was():
    calls = []

    @retry_transient(3, 8, wait=tenacity.wait_none())
    def ask():
        calls.append(1)
        raise _status(503)

    with pytest.raises(requests.HTTPError):
        ask()
    assert len(calls) == 3


def test_a_refusal_is_asked_once():
    calls = []

    @retry_transient(3, 8, wait=tenacity.wait_none())
    def ask():
        calls.append(1)
        raise _status(403)

    with pytest.raises(requests.HTTPError):
        ask()
    assert len(calls) == 1


class _Provider(BaseHTTPRequestHandler):
    statuses: list
    asked: list

    def _answer(self):
        self.asked.append(time.monotonic())
        status = self.statuses[min(len(self.asked), len(self.statuses)) - 1]
        body = b'{"email": [], "results": [], "resultsFound": 0}'
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    do_GET = do_PUT = _answer

    def log_message(self, *args):
        pass


@pytest.fixture
def provider():
    handler = type("Handler", (_Provider,), {"statuses": [], "asked": []})
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    handler.url = f"http://127.0.0.1:{server.server_port}/"
    yield handler
    server.shutdown()
    thread.join()
    server.server_close()


def test_orcid_keeps_its_rate_and_asks_again_after_a_busy_answer(provider, monkeypatch):
    provider.statuses = [503, 200, 200]
    monkeypatch.setattr(OrcidClient, "BASE_URL", provider.url)
    monkeypatch.setattr(OrcidClient.emails.retry, "wait", tenacity.wait_none())
    client = OrcidClient()
    client.emails("0000-0000-0000-0001")
    client.emails("0000-0000-0000-0002")
    gaps = [b - a for a, b in zip(provider.asked, provider.asked[1:])]
    assert len(provider.asked) == 3
    assert min(gaps) >= 0.9 / ORCID_REQUEST_LIMIT


def test_sciencedirect_asks_again_after_a_busy_answer_not_a_refusal(provider, monkeypatch):
    monkeypatch.setattr(ScienceDirectClient, "URL", provider.url)
    monkeypatch.setattr(ScienceDirectClient._request.retry, "wait", tenacity.wait_none())
    monkeypatch.setattr("ingestion_workflow.clients.sciencedirect._MIN_REQUEST_INTERVAL", 0.05)
    client = ScienceDirectClient("key")
    provider.statuses = [503, 200]
    assert client.count({"qs": "x"}) == 0
    assert len(provider.asked) == 2
    provider.asked.clear()
    provider.statuses = [401]
    with pytest.raises(requests.HTTPError):
        client.count({"qs": "x"})
    assert len(provider.asked) == 1
