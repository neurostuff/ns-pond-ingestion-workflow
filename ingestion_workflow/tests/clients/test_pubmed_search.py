import json
import pytest
from ingestion_workflow.clients.pubmed import (
    ESEARCH_MAX_RESULTS,
    PUBMED_FIRST_YEAR,
    PubMedClient,
)
from ingestion_workflow.config import Settings
from ingestion_workflow.models import Identifier, Identifiers


@pytest.fixture
def settings_with_email() -> Settings:
    return Settings(pubmed_email="tests@example.com")


def test_pubmed_client_search_paginates(monkeypatch):
    client = PubMedClient(email="tests@example.com")

    def fake_esearch(
        query: str,
        *,
        retstart: int,
        retmax: int,
        mindate=None,
        maxdate=None,
    ) -> dict:
        count = 2500
        if retstart == 0:
            ids = [str(i) for i in range(0, 1000)]
        elif retstart == 1000:
            ids = [str(i) for i in range(1000, 2000)]
        elif retstart == 2000:
            ids = [str(i) for i in range(2000, 2500)]
        else:
            ids = []
        return {"esearchresult": {"count": str(count), "idlist": ids}}

    monkeypatch.setattr(client, "_esearch", fake_esearch)

    results = client.search("test query")
    assert len(results.identifiers) == 2500
    assert results.lookup("0", key="pmid") is not None
    assert results.lookup("2499", key="pmid") is not None


def test_pubmed_client_search_splits_large_result_set(monkeypatch):
    client = PubMedClient(email="tests@example.com")

    def fake_collect(
        query: str,
        *,
        mindate=None,
        maxdate=None,
        retmax=None,
    ):
        if mindate is None and maxdate is None:
            ids = [str(i) for i in range(ESEARCH_MAX_RESULTS)]
            return ids, ESEARCH_MAX_RESULTS
        year = mindate or 1990
        ids = [f"{year}001", f"{year}002"]
        return ids, len(ids)

    def fake_count(
        query: str,
        *,
        mindate=None,
        maxdate=None,
    ) -> int:
        if mindate is not None and mindate == maxdate:
            return 2
        return ESEARCH_MAX_RESULTS  # the whole query, and the span it is split over

    monkeypatch.setattr(client, "_esearch_count", fake_count)
    monkeypatch.setattr(client, "_collect_esearch_ids", fake_collect)
    monkeypatch.setattr(client, "_current_year", lambda: 1992)

    results = client.search("test query", start_year=1990)
    # up to next year, for in-press articles dated ahead
    expected_count = (1993 - 1990 + 1) * 2
    assert len(results.identifiers) == expected_count
    assert results.lookup("1990001", key="pmid") is not None


def test_pubmed_search_service_delegates(settings_with_email):
    expected = Identifiers([Identifier(pmid="12345")])

    class DummyClient:
        def __init__(self) -> None:
            self.calls = []

        def search(self, query: str, **years) -> Identifiers:
            self.calls.append((query, years))
            return expected

    from ingestion_workflow.services.query_dialect import Years, parse
    from ingestion_workflow.services.search import PubMedSearchService

    dummy_client = DummyClient()
    for years in (Years.parse("2000"), Years.parse("2024:2026"), None):
        service = PubMedSearchService(
            parse("sample", years), settings_with_email, client=dummy_client
        )
        assert service.search() is expected
    term = '"sample"[tiab] AND "journal article"[pt]'
    assert dummy_client.calls == [
        (f"{term} AND 2000[dp]", {"start_year": 2000, "end_year": 2000}),
        (f"{term} AND 2024:2026[dp]", {"start_year": 2024, "end_year": 2026}),
        # no --year: the client splits over all of PubMed's years
        (term, {}),
    ]


def test_a_large_search_without_years_is_split_over_all_of_pubmed(monkeypatch):
    client = PubMedClient(email="tests@example.com")
    windows = []

    def fake_count(query, *, mindate=None, maxdate=None):
        if mindate is None or mindate != maxdate:
            return ESEARCH_MAX_RESULTS
        return 1

    def fake_collect(query, *, mindate=None, maxdate=None, retmax=None):
        windows.append(mindate)
        return [str(mindate)], 1

    monkeypatch.setattr(client, "_esearch_count", fake_count)
    monkeypatch.setattr(client, "_collect_esearch_ids", fake_collect)
    monkeypatch.setattr(client, "_current_year", lambda: 2026)

    client.search("test query")
    assert windows[0] == PUBMED_FIRST_YEAR and windows[-1] == 2027
    assert windows == list(range(PUBMED_FIRST_YEAR, 2028))


def test_a_large_search_with_hits_dated_outside_its_years_is_refused(monkeypatch):
    client = PubMedClient(email="tests@example.com")

    def fake_count(query, *, mindate=None, maxdate=None):
        return ESEARCH_MAX_RESULTS if mindate is None else ESEARCH_MAX_RESULTS - 3

    monkeypatch.setattr(client, "_esearch_count", fake_count)
    monkeypatch.setattr(client, "_current_year", lambda: 2026)

    with pytest.raises(ValueError, match="only 9,997 are dated 1781-2027"):
        client.search("test query")


def test_pubmed_search_service_requires_email(monkeypatch):
    from ingestion_workflow.services.query_dialect import parse
    from ingestion_workflow.services.search import PubMedSearchService

    monkeypatch.delenv("PUBMED_EMAIL", raising=False)
    monkeypatch.delenv("EMAIL", raising=False)

    settings = Settings.model_construct(pubmed_email=None)

    with pytest.raises(ValueError):
        PubMedSearchService(parse("query"), settings)


def test_pubmed_request_json_handles_invalid_control_chars(monkeypatch):
    client = PubMedClient(email="tests@example.com")
    invalid_json = '{"esearchresult": {"ERROR": "Search Backend failed\nTry again"}}'

    class DummyResponse:
        def __init__(self, text: str) -> None:
            self.text = text

        def raise_for_status(self) -> None:
            return None

        def json(self):
            raise json.JSONDecodeError("bad json", self.text, 0)

    monkeypatch.setattr(client, "_rate_limit_sleep", lambda: None)
    monkeypatch.setattr(client._client, "get", lambda *_args, **_kwargs: DummyResponse(invalid_json))

    payload = client._request_json("https://example.com", params=[])
    assert payload["esearchresult"]["ERROR"].startswith("Search Backend failed")


@pytest.mark.vcr()
def test_pubmed_client_search_real_api():
    client = PubMedClient(email="tests@example.com", tool="ingestion-workflow-tests")
    results = client.search("codex-nonexistent-term-zzqvwp[Title]")
    assert isinstance(results, Identifiers)
