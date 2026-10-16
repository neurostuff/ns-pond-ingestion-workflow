"""`ingest add --search`: search, register, enrich, report."""

import json
import os

import ingestion_workflow.cli.main as cli
import pytest
from ingestion_workflow.catalog import Catalog
from typer.testing import CliRunner


@pytest.fixture()
def config(tmp_path):
    # keys are dropped from the cassettes; any value replays
    path = tmp_path / "settings.yaml"
    path.write_text(
        json.dumps(
            {
                "data_root": str(tmp_path / "data"),
                "cache_root": str(tmp_path / "cache"),
                "catalog_root": str(tmp_path / "catalog"),
                "ns_pond_root": str(tmp_path / "pond"),
                "log_to_file": False,
                "show_progress": False,
                "pubmed_email": "test@example.com",
                "pubmed_api_key": os.environ.get("PUBMED_API_KEY", "replay"),
                "semantic_scholar_api_key": os.environ.get("SEMANTIC_SCHOLAR_API_KEY", "replay"),
                "openalex_email": "test@example.com",
                "openalex_api_key": os.environ.get("OPENALEX_API_KEY", "replay"),
                "elsevier_api_key": os.environ.get("ELSEVIER_API_KEY", "replay"),
            }
        ),
        encoding="utf-8",
    )
    return path


def _add(config, *args):
    result = CliRunner().invoke(cli.app, ["add", *args, "--config", str(config)])
    assert result.exit_code == 0, result.output
    return result.output


def _openalex(config):
    return _add(
        config,
        "--search",
        "fMRI AND synesthesia",
        "--year",
        "2026",
        "--provider",
        "openalex",
        "--no-enrich",
    )


@pytest.mark.vcr()
def test_hits_are_reported_registered_and_the_bare_work_is_counted(config, tmp_path):
    out = _openalex(config)

    assert "openalex: title_and_abstract.search:(fMRI AND synesthesia),type:article," in out
    assert "openalex: 2 articles" in out
    assert "2 distinct: 0 already in the catalog, 2 new" in out
    assert "1 openalex records have no DOI, PMID or PMCID and were not registered" in out
    with Catalog.open(tmp_path / "catalog") as catalog:
        assert catalog.count_articles() == 2


@pytest.mark.vcr()
def test_a_second_run_finds_them_in_the_catalog(config, tmp_path):
    _openalex(config)
    out = _openalex(config)

    assert "2 distinct: 2 already in the catalog, 0 new" in out


@pytest.mark.vcr()
def test_every_provider_is_searched_and_counted_after_enrichment(config, tmp_path):
    out = _add(config, "--search", "fMRI AND misophonia")

    assert 'pubmed: ("fMRI"[tiab] AND "misophonia"[tiab]) AND "journal article"[pt]' in out
    assert "semantic_scholar: query=fMRI + misophonia" in out
    assert '{"qs": "fMRI AND misophonia"}: 73 found' in out
    for line in [
        "pubmed: 11 articles, 1 found by no other",
        "semantic_scholar: 13 articles, 3 found by no other",
        "openalex: 12 articles, 2 found by no other",
        "sciencedirect: 73 articles, 70 found by no other",
        # PubMed's hits carry PMIDs only; they meet the others' DOIs once enriched
        "pubmed and semantic_scholar share 9",
        "pubmed and openalex share 10",
        "pubmed and sciencedirect share 2",
        "semantic_scholar and openalex share 9",
        "87 distinct: 0 already in the catalog, 87 new",
        # each article is enriched and counted once, however many providers found it
        "87 identifiers resolved to 87 new articles (0 already known)",
        "3 semantic_scholar records have no DOI, PMID or PMCID and were not registered",
    ]:
        assert line in out
    with Catalog.open(tmp_path / "catalog") as catalog:
        assert catalog.count_articles() == 87


@pytest.mark.vcr()
def test_pmids_and_dois_meet_only_once_enriched(config):
    # PubMed returns PMIDs and ScienceDirect DOIs: without enrichment they share nothing
    out = _add(
        config,
        "--search",
        "fMRI AND misophonia",
        "--year",
        "2026",
        "--provider",
        "pubmed",
        "--provider",
        "sciencedirect",
    )
    for line in [
        "pubmed: 3 articles, 2 found by no other",
        "sciencedirect: 17 articles, 16 found by no other",
        "pubmed and sciencedirect share 1",
        "19 distinct: 0 already in the catalog, 19 new",
        "19 identifiers resolved to 19 new articles (0 already known)",
    ]:
        assert line in out


def _fails(config, *args):
    result = CliRunner().invoke(cli.app, ["add", *args, "--config", str(config)])
    assert result.exit_code == 2, result.output
    return " ".join(result.output.replace("│", " ").split())


def test_a_query_that_does_not_parse_is_a_usage_error(config):
    assert "the query ends where a term was expected" in _fails(config, "--search", "fMRI AND")


def test_year_and_provider_need_a_search(config):
    assert "--year and --provider apply to --search" in _fails(config, "--year", "2026")
    assert "--year and --provider apply to --search" in _fails(config, "--provider", "pubmed")


def test_a_bad_year_is_a_usage_error(config):
    assert "a year is 2026 or a range 2025:2026" in _fails(
        config, "--search", "fMRI", "--year", "2026-2027"
    )


def test_a_provider_without_its_key_is_a_usage_error(tmp_path, monkeypatch):
    monkeypatch.delenv("SEMANTIC_SCHOLAR_API_KEY", raising=False)
    path = tmp_path / "keyless.yaml"
    path.write_text(json.dumps({"catalog_root": str(tmp_path / "catalog"), "log_to_file": False}))
    out = _fails(path, "--search", "fMRI", "--provider", "semantic_scholar")
    assert "semantic_scholar_api_key must be configured" in out


def test_a_query_one_provider_cannot_say_is_a_usage_error(config):
    out = _fails(config, "--search", "abstract: fMRI", "--provider", "sciencedirect")
    assert "ScienceDirect's search has no abstract field" in out


@pytest.mark.vcr(match_on=["method", "scheme", "host", "port", "path", "query", "body"])
def test_a_search_sciencedirect_cannot_split_stops_before_any_provider_searches(
    config, tmp_path, monkeypatch
):
    # the cassette holds ScienceDirect's one count and no OpenAlex request
    from ingestion_workflow.clients.sciencedirect import ScienceDirectClient

    # eight ANDs leave no room for a word, and it has no year range or OR clauses
    monkeypatch.setattr(ScienceDirectClient, "MAX_RESULTS", 1)
    out = _fails(
        config,
        "--search",
        "fMRI AND brain AND cortex AND memory AND patients AND human AND data AND analysis "
        "AND model",
        "--year",
        "2026",
        "--provider",
        "openalex",
        "--provider",
        "sciencedirect",
    )
    assert "none of its splits applies" in out
    with Catalog.open(tmp_path / "catalog") as catalog:
        assert catalog.count_articles() == 0


@pytest.mark.vcr()
def test_a_search_adds_to_the_ids_given(config, tmp_path):
    out = _add(
        config,
        "10.1016/j.neuroimage.2023.120",
        "--search",
        "fMRI AND synesthesia",
        "--year",
        "2026",
        "--provider",
        "openalex",
        "--no-enrich",
    )
    assert "openalex: 2 articles" in out
    with Catalog.open(tmp_path / "catalog") as catalog:
        assert catalog.count_articles() == 3


@pytest.mark.vcr(match_on=["method", "scheme", "host", "port", "path", "query", "body"])
def test_a_split_sciencedirect_search_prints_each_part(config, monkeypatch):
    from ingestion_workflow.clients.sciencedirect import ScienceDirectClient

    monkeypatch.setattr(ScienceDirectClient, "PAGE_SIZE", 10)
    monkeypatch.setattr(ScienceDirectClient, "MAX_RESULTS", 30)
    out = _add(
        config,
        "--search",
        "(fMRI AND misophonia) OR (fMRI AND hyperacusis)",
        "--year",
        "2025:2026",
        "--provider",
        "sciencedirect",
        "--no-enrich",
    )
    q = "(fMRI AND misophonia) OR (fMRI AND hyperacusis)"
    for line in [
        f'{{"qs": "{q}", "date": "2025-2026"}}: 54 found; split by year: 21 + 33 = 54',
        f'{{"qs": "{q}", "date": "2026"}}: 33 found; split on \'disease\': 17 + 16 = 33',
        f'{{"qs": "({q}) NOT disease", "date": "2026"}}: 16 found',
    ]:
        assert line in out
    assert "  54 records" in out  # an article two parts found is one record
