"""Which failed requests are worth asking again, and how requests are spaced."""

from __future__ import annotations

import threading
import time

import httpx
import pytest
import requests
import tenacity
from ingestion_workflow.clients.crossref import CrossrefClient
from ingestion_workflow.clients.doi_registry import DoiRegistry
from ingestion_workflow.clients.openalex import OpenAlexClient
from ingestion_workflow.clients.orcid import OrcidClient
from ingestion_workflow.clients.pubmed import PubMedClient
from ingestion_workflow.clients.sciencedirect import ScienceDirectClient
from ingestion_workflow.clients.semantic_scholar import SemanticScholarClient
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



def _gaps(provider):
    return [b - a for a, b in zip(provider.asked, provider.asked[1:])]


def test_orcid_asks_again_after_a_busy_answer(provider, monkeypatch):
    provider.statuses = [503, 200]
    monkeypatch.setattr(OrcidClient, "BASE_URL", provider.url)
    monkeypatch.setattr(OrcidClient.emails.retry, "wait", tenacity.wait_none())
    OrcidClient().emails("0000-0000-0000-0001")
    assert len(provider.asked) == 2


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


def _crossref_work(provider, monkeypatch):
    monkeypatch.setattr(CrossrefClient, "BASE_URL", provider.url)
    client = CrossrefClient()
    return lambda: client.work("10.1/x")


def _crossref_list(provider, monkeypatch):
    monkeypatch.setattr(CrossrefClient, "BASE_URL", provider.url)
    client = CrossrefClient()
    return lambda: client._list({})


def _openalex(provider, monkeypatch):
    monkeypatch.setattr(OpenAlexClient, "BASE_URL", provider.url)
    client = OpenAlexClient()
    return lambda: client._request_openalex({})


def _semantic_scholar(provider, monkeypatch):
    monkeypatch.setattr(SemanticScholarClient, "BULK_SEARCH_URL", provider.url)
    client = SemanticScholarClient("key")
    return lambda: client._request_bulk_search({})


def _orcid(provider, monkeypatch):
    monkeypatch.setattr(OrcidClient, "BASE_URL", provider.url)
    client = OrcidClient()
    return lambda: client.emails("0000-0000-0000-0001")


def _sciencedirect(provider, monkeypatch):
    monkeypatch.setattr(ScienceDirectClient, "URL", provider.url)
    client = ScienceDirectClient("key")
    return lambda: client._request({})


def _pubmed(provider, monkeypatch):
    client = PubMedClient("t@example.com")
    return lambda: client._request_json(provider.url, [])


def _pubmed_keyed(provider, monkeypatch):
    client = PubMedClient("t@example.com", api_key="k")
    return lambda: client._request_json(provider.url, [])


#: Each provider's rate as its limit says it, in seconds between requests, and how long to
#: look: a request in each of these is never closer to the one before than the interval.
RATES = [
    pytest.param(_crossref_work, 1 / 10, 3, id="crossref"),
    pytest.param(_crossref_list, 1 / 3, 3, id="crossref-list"),
    pytest.param(_openalex, 1 / 10, 3, id="openalex"),
    pytest.param(_semantic_scholar, 1.0, 2, id="semantic-scholar"),
    pytest.param(_orcid, 1 / 12, 3, id="orcid"),
    pytest.param(_sciencedirect, 1.0, 2, id="sciencedirect"),
    pytest.param(_pubmed, 1 / 3, 3, id="pubmed"),
    pytest.param(_pubmed_keyed, 1 / 10, 3, id="pubmed-with-key"),
]


@pytest.mark.parametrize("client, interval, requests_made", RATES)
def test_each_provider_is_asked_no_faster_than_its_rate_and_no_slower(
        client, interval, requests_made, provider, monkeypatch):
    ask = client(provider, monkeypatch)
    for _ in range(requests_made):
        ask()
    assert len(provider.asked) == requests_made
    # Requests are spaced to the rate, not left a second apart whatever it is.
    assert all(interval * 0.9 <= gap <= interval * 1.5 + 0.1 for gap in _gaps(provider))


#: How many times each provider is asked for one request that keeps failing, and the longest
#: wait between asks.
ATTEMPTS = [
    (CrossrefClient.work, 4, None),
    (CrossrefClient._page, 4, None),
    (OpenAlexClient._request_openalex, 3, 8),
    (SemanticScholarClient._request_semantic_scholar, 3, 8),
    (SemanticScholarClient._request_bulk_search, 3, 8),
    (SemanticScholarClient._request_metadata_batch, 3, 8),
    (OrcidClient.emails, 4, 16),
    (ScienceDirectClient._request, 3, 8),
    (DoiRegistry._work, 6, 30),
    (PubMedClient._request_json, 6, 30),
    (PubMedClient._request_efetch, 3, 8),
]


@pytest.mark.parametrize("method, attempts, longest_wait", ATTEMPTS)
def test_each_provider_is_asked_as_many_times_as_it_always_was(method, attempts, longest_wait):
    assert method.retry.stop.max_attempt_number == attempts
    if longest_wait:  # Crossref waits as its Retry-After says, else as _wait does
        assert method.retry.wait.max == longest_wait
