"""The search service and the overlap it reports."""

import os

import pytest
from ingestion_workflow.config import Settings
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.services.search import OpenAlexSearchService, collapse


@pytest.mark.vcr()
def test_the_service_returns_what_the_client_finds():
    settings = Settings(
        openalex_email="test@example.com",
        openalex_api_key=os.environ.get("OPENALEX_API_KEY", "replay"),
    )
    service = OpenAlexSearchService("fMRI AND synesthesia", settings, year=2026)
    assert len(service.search().identifiers) == 3


def test_records_sharing_any_id_are_one_article_across_providers():
    found = {
        "pubmed": [Identifier(pmid="1"), Identifier(pmid="2"), Identifier(pmid="2")],
        "openalex": [
            Identifier(doi="10.1/a", pmid="1"),
            Identifier(doi="10.1/b"),
            Identifier(doi="10.1/b", pmcid="PMC9"),
            Identifier(other_ids={"openalex": "https://openalex.org/W1"}),
        ],
    }
    articles, members = collapse(found)

    assert len(articles["pubmed"]) == 2
    assert len(articles["openalex"]) == 3
    assert len(articles["pubmed"] & articles["openalex"]) == 1
    assert len(set().union(*articles.values())) == 4
    assert sum(len(m) for m in members.values()) == 7


def test_the_same_title_without_the_same_id_stays_distinct():
    articles, _ = collapse({"a": [Identifier(doi="10.1/a"), Identifier(doi="10.1/b")]})
    assert len(articles["a"]) == 2
