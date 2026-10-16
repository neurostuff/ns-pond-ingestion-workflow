"""ScienceDirect search, planned from counts and paged to the end."""

import json
import os

import pytest
from ingestion_workflow.clients.sciencedirect import ScienceDirectClient, identifier_of
from ingestion_workflow.config import Settings
from ingestion_workflow.services.query_dialect import QueryError, Years, parse
from ingestion_workflow.services.search import ScienceDirectSearchService

# every request is the same PUT; the body (query, offset, page size) tells them apart
BY_BODY = ["method", "scheme", "host", "port", "path", "query", "body"]


@pytest.fixture()
def settings():
    # the key header is dropped from the cassettes; any value replays
    return Settings(elsevier_api_key=os.environ.get("ELSEVIER_API_KEY", "replay"))


def _search(settings, text, years=None):
    service = ScienceDirectSearchService(
        parse(text, Years.parse(years) if years else None), settings
    )
    service.plan()
    return service, list(service.search().identifiers)


@pytest.mark.vcr(match_on=BY_BODY)
def test_one_page_holds_everything_found(settings):
    service, found = _search(settings, "fMRI AND misophonia", "2026")

    [(body, total)] = service.parts
    assert json.loads(body) == {"qs": "fMRI AND misophonia", "date": "2026"}
    assert len(found) == total == 17
    assert all(i.doi == i.doi.lower() for i in found if i.doi)
    assert all(i.other_ids["pii"] for i in found)


@pytest.mark.vcr(match_on=BY_BODY)
def test_a_total_one_past_a_page_is_paged_to_its_last_record(settings, monkeypatch):
    monkeypatch.setattr(ScienceDirectClient, "PAGE_SIZE", 25)
    service, found = _search(settings, "fMRI AND hyperacusis", "2022")

    assert service.parts == [(json.dumps({"qs": "fMRI AND hyperacusis", "date": "2022"}), 26)]
    assert len({i.doi for i in found}) == 26  # offsets 0 and 25


@pytest.mark.vcr(match_on=BY_BODY)
def test_a_total_of_whole_pages_asks_for_no_page_past_it(settings, monkeypatch):
    # the cassette holds the one page of 10; asking for offset 10 would not match
    monkeypatch.setattr(ScienceDirectClient, "PAGE_SIZE", 10)
    service, found = _search(settings, "fMRI AND hyperacusis", "2020")

    assert len(found) == 10


@pytest.mark.vcr(match_on=BY_BODY)
def test_a_search_past_the_last_offset_is_split_into_clauses_then_years(settings, monkeypatch):
    # 54 found in all; the API is taken to serve 30, so it is split
    monkeypatch.setattr(ScienceDirectClient, "PAGE_SIZE", 10)
    monkeypatch.setattr(ScienceDirectClient, "MAX_RESULTS", 30)
    service, found = _search(
        settings, "(fMRI AND misophonia) OR (fMRI AND hyperacusis)", "2025:2026"
    )

    assert [(json.loads(body), total) for body, total in service.parts] == [
        ({"qs": "fMRI AND misophonia", "date": "2025-2026"}, 24),
        ({"qs": "fMRI AND hyperacusis", "date": "2025"}, 19),
        ({"qs": "fMRI AND hyperacusis", "date": "2026"}, 23),
    ]
    # every part paged to its end (24 + 19 + 23 records); an article two parts
    # found is one record, so the union is the unsplit search's 54
    assert len(found) == len({i.doi for i in found}) == 54


@pytest.mark.vcr(match_on=BY_BODY)
def test_a_total_at_the_limit_is_not_split(settings, monkeypatch):
    monkeypatch.setattr(ScienceDirectClient, "MAX_RESULTS", 17)
    service, found = _search(settings, "fMRI AND misophonia", "2026")

    assert len(service.parts) == 1 and len(found) == 17


@pytest.mark.vcr(match_on=BY_BODY)
def test_one_year_over_the_limit_is_refused_not_split_again(settings, monkeypatch):
    monkeypatch.setattr(ScienceDirectClient, "MAX_RESULTS", 16)
    service = ScienceDirectSearchService(
        parse("fMRI AND misophonia", Years.parse("2026")), settings
    )
    with pytest.raises(QueryError, match="ScienceDirect finds 17"):
        service.plan()


@pytest.mark.vcr(match_on=BY_BODY)
def test_a_search_that_cannot_be_split_is_refused(settings, monkeypatch):
    monkeypatch.setattr(ScienceDirectClient, "MAX_RESULTS", 30)
    service = ScienceDirectSearchService(parse("fMRI AND misophonia"), settings)
    with pytest.raises(QueryError, match="ScienceDirect finds 73"):
        service.plan()


def test_a_result_is_named_by_its_doi_or_else_its_pii():
    assert identifier_of({"doi": "10.1016/J.X", "pii": "S1"}).doi == "10.1016/j.x"
    no_doi = identifier_of({"pii": "S0001"})
    assert no_doi.doi is None and no_doi.other_ids == {"pii": "S0001"}
    neither = identifier_of({"title": "Preface"})
    assert neither.doi is None and neither.other_ids is None


def test_sciencedirect_needs_its_key():
    with pytest.raises(ValueError, match="elsevier_api_key"):
        ScienceDirectSearchService(parse("fMRI"), Settings.model_construct(elsevier_api_key=None))
