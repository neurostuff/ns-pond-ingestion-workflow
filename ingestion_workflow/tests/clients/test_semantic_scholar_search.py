"""Semantic Scholar's bulk search, paged by token to the end."""

import os

import pytest
from ingestion_workflow.clients.semantic_scholar import SemanticScholarClient
from ingestion_workflow.services.query_dialect import parse, to_semantic_scholar

# 1,038 papers when recorded: a full page of 1,000 and a second of 38
QUERY = "fMRI AND (dyslexia OR tinnitus OR insomnia OR stuttering)"


@pytest.mark.vcr()
def test_a_bulk_search_follows_the_token_to_the_last_page():
    # the key header is dropped from the cassette; any value replays
    client = SemanticScholarClient(os.environ.get("SEMANTIC_SCHOLAR_API_KEY", "replay"))
    found = list(client.search_bulk(to_semantic_scholar(parse(QUERY))).identifiers)

    assert len(found) == 1038
    dois = [i.doi for i in found if i.doi]
    assert dois and all(doi == doi.lower() for doi in dois)
    assert any(i.pmid for i in found) and any(i.pmcid for i in found)
    assert all(i.other_ids["semantic_scholar"] for i in found)
