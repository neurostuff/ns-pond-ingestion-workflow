"""The search services, and the overlap counted in catalog articles."""

import os

import pytest
from ingestion_workflow.catalog import Catalog
from ingestion_workflow.config import Settings
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.services.query_dialect import QueryError, Years, parse
from ingestion_workflow.services.search import (
    OpenAlexSearchService,
    SearchReport,
    resolve_hits,
    search_services,
    surviving,
)


@pytest.mark.vcr()
def test_the_service_returns_what_the_client_finds():
    settings = Settings(
        openalex_email="test@example.com",
        openalex_api_key=os.environ.get("OPENALEX_API_KEY", "replay"),
    )
    service = OpenAlexSearchService(parse("fMRI AND synesthesia", Years.parse("2026")), settings)
    assert len(service.search().identifiers) == 3


def test_a_query_one_provider_cannot_say_fails_before_any_search():
    settings = Settings(pubmed_email="test@example.com", semantic_scholar_api_key="unused")
    with pytest.raises(QueryError, match="Semantic Scholar"):
        search_services(parse("title: fMRI"), ["pubmed", "semantic_scholar"], settings)


def test_semantic_scholar_needs_its_key():
    settings = Settings.model_construct(semantic_scholar_api_key=None)
    with pytest.raises(ValueError, match="semantic_scholar_api_key"):
        search_services(parse("fMRI"), ["semantic_scholar"], settings)


SEARCHED = {
    "pubmed": [Identifier(pmid="1"), Identifier(pmid="2"), Identifier(pmid="3")],
    "openalex": [
        Identifier(doi="10.1/a", pmid="1"),
        Identifier(doi="10.1/b"),
        Identifier(other_ids={"openalex": "https://openalex.org/W1"}),
    ],
    "semantic_scholar": [Identifier(doi="10.1/b"), Identifier(doi="10.1/c")],
}


@pytest.fixture()
def report(tmp_path):
    with Catalog.open(tmp_path / "catalog") as catalog:
        catalog.register_many([Identifier(pmid="3")])
        return _run(catalog, SEARCHED, enriched=[Identifier(pmid="2", doi="10.1/b")])


def _run(catalog, searched, enriched):
    """What `ingest add` does: snapshot, register, enrich (here: register `enriched`), report."""
    held = set(catalog.all_article_ids())
    catalog.register_many([r for records in searched.values() for r in records])
    catalog.register_many(enriched)
    return SearchReport.build(surviving(catalog, held), resolve_hits(catalog, searched))


def test_articles_are_counted_as_the_catalog_holds_them_after_enrichment(report):
    assert {p: len(a) for p, a in report.articles.items()} == {
        "pubmed": 3,
        "openalex": 2,
        "semantic_scholar": 2,
    }
    assert report.shared() == {
        ("pubmed", "openalex"): 2,
        ("pubmed", "semantic_scholar"): 1,
        ("openalex", "semantic_scholar"): 1,
    }
    assert {p: report.unique(p) for p in report.articles} == {
        "pubmed": 1,
        "openalex": 0,
        "semantic_scholar": 1,
    }
    assert len(report.distinct) == 4


def test_what_the_catalog_held_before_is_known_and_the_rest_new(report):
    assert len(report.known) == 1


def test_a_record_without_an_id_is_counted_not_dropped(report):
    assert report.without_id == {"pubmed": 0, "openalex": 1, "semantic_scholar": 0}


def test_the_same_title_without_the_same_id_stays_distinct(tmp_path):
    searched = {"a": [Identifier(doi="10.1/a"), Identifier(doi="10.1/b")]}
    with Catalog.open(tmp_path / "catalog") as catalog:
        report = _run(catalog, searched, enriched=[])
    assert len(report.articles["a"]) == 2


def test_a_hit_enrichment_merges_into_a_held_article_is_known(tmp_path):
    # the catalog holds the paper by DOI only; PubMed's hit is its PMID
    with Catalog.open(tmp_path / "catalog") as catalog:
        catalog.register_many([Identifier(doi="10.1/x")])
        report = _run(
            catalog,
            {"pubmed": [Identifier(pmid="111")]},
            enriched=[Identifier(pmid="111", doi="10.1/x")],
        )
        assert catalog.count_articles() == 1
    assert len(report.distinct) == 1
    assert len(report.known) == 1


def test_held_articles_chained_by_enrichment_are_one_known_article(tmp_path):
    # held as three articles: DOI x, PMID 1, PMCID 9; enrichment links x to 1 and 1 to 9
    with Catalog.open(tmp_path / "catalog") as catalog:
        catalog.register_many(
            [Identifier(doi="10.1/x"), Identifier(pmid="1"), Identifier(pmcid="PMC9")]
        )
        report = _run(
            catalog,
            {"openalex": [Identifier(doi="10.1/x")], "pubmed": [Identifier(pmid="1")]},
            enriched=[Identifier(doi="10.1/x", pmid="1"), Identifier(pmid="1", pmcid="PMC9")],
        )
        assert catalog.count_articles() == 1
    assert len(report.distinct) == 1
    assert len(report.known) == 1  # never more known than distinct, so never fewer than 0 new


def test_an_article_new_this_run_is_not_known(tmp_path):
    with Catalog.open(tmp_path / "catalog") as catalog:
        catalog.register_many([Identifier(doi="10.1/held")])
        report = _run(catalog, {"pubmed": [Identifier(pmid="5")]}, enriched=[])
    assert len(report.distinct) == 1 and not report.known
