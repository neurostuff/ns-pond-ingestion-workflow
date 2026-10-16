"""ScienceDirect search, paged to the end and split where the API stops paging."""

import json
import os

import pytest
from ingestion_workflow.config import Settings
from ingestion_workflow.services.query_dialect import QueryError, Years, parse
from ingestion_workflow.services.search import ScienceDirectSearchService


@pytest.fixture()
def settings():
    # the key header is dropped from the cassettes; any value replays
    return Settings(elsevier_api_key=os.environ.get("ELSEVIER_API_KEY", "replay"))


@pytest.mark.vcr()
def test_one_page_holds_everything_found(settings):
    service = ScienceDirectSearchService(
        parse("fMRI AND misophonia", Years.parse("2026")), settings
    )
    found = list(service.search().identifiers)

    [(body, total)] = service.parts
    assert json.loads(body) == {"qs": "fMRI AND misophonia", "date": "2026"}
    assert len(found) == total > 0
    assert all(i.doi == i.doi.lower() for i in found if i.doi)


@pytest.mark.vcr()
def test_a_search_past_the_last_offset_is_split_into_clauses_then_years(settings):
    # 54 found in all; the API is taken to serve 30, so it is split
    query = parse("(fMRI AND misophonia) OR (fMRI AND hyperacusis)", Years.parse("2025:2026"))
    service = ScienceDirectSearchService(query, settings, show=10, max_results=30)
    found = list(service.search().identifiers)

    assert [(json.loads(body), total) for body, total in service.parts] == [
        ({"qs": "fMRI AND misophonia", "date": "2025-2026"}, 24),
        ({"qs": "fMRI AND hyperacusis", "date": "2025"}, 19),
        ({"qs": "fMRI AND hyperacusis", "date": "2026"}, 23),
    ]
    assert len(found) == 24 + 19 + 23  # every part paged to its end
    assert len({i.doi for i in found}) == 54  # the parts' union is the unsplit search


@pytest.mark.vcr()
def test_a_search_that_cannot_be_split_is_refused(settings):
    service = ScienceDirectSearchService(parse("fMRI AND misophonia"), settings, max_results=30)
    with pytest.raises(QueryError, match="ScienceDirect finds 73"):
        service.search()


def test_sciencedirect_needs_its_key():
    with pytest.raises(ValueError, match="elsevier_api_key"):
        ScienceDirectSearchService(parse("fMRI"), Settings.model_construct(elsevier_api_key=None))
