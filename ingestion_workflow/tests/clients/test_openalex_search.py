"""Searching OpenAlex for journal articles, paged by cursor to the end."""

import os

import pytest
from ingestion_workflow.clients.openalex import OpenAlexClient

# the cassette matches on the filter, so it also pins `type:article` and the year
QUERY = "fMRI AND synesthesia"


def _client() -> OpenAlexClient:
    # the key is dropped from the cassette; any value replays
    return OpenAlexClient("test@example.com", os.environ.get("OPENALEX_API_KEY", "replay"))


@pytest.mark.vcr()
def test_a_search_follows_the_cursor_to_the_last_page():
    found = _client().search(QUERY, year=2026, per_page=2)

    assert len(found.identifiers) == 3  # two pages: 2 + 1
    assert all(i.doi == i.doi.lower() and not i.doi.startswith("http") for i in found if i.doi)


@pytest.mark.vcr()
def test_a_work_without_any_id_is_kept_with_its_openalex_id():
    found = _client().search(QUERY, year=2026, per_page=2)

    bare = [i for i in found if not (i.doi or i.pmid or i.pmcid)]
    assert len(bare) == 1
    assert bare[0].other_ids["openalex"].startswith("https://openalex.org/W")
