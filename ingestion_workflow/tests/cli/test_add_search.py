"""`ingest add --search`: search, register, enrich, report."""

import json
import os

import ingestion_workflow.cli.main as cli
import pytest
from ingestion_workflow.catalog import Catalog
from ingestion_workflow.models.ids import Identifier
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

    assert "years: 1990 onward (the default; --year to change)" in out
    assert (
        'pubmed: ("fMRI"[tiab] AND "misophonia"[tiab]) AND "journal article"[pt] '
        "AND 1990:3000[dp]"
    ) in out
    assert "semantic_scholar: query=fMRI + misophonia year=1990-" in out
    assert "type:article,publication_year:>1989" in out
    assert '{"qs": "fMRI AND misophonia", "date": "1990-9999"}: 73 found' in out
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
        "2 semantic_scholar records have no DOI, PMID or PMCID and were not registered",
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
    path.write_text(json.dumps({"catalog_root": str(tmp_path / "catalog"), "log_to_file": False,
                            "pubmed_email": "replay@example.com"}))
    out = _fails(path, "--search", "fMRI", "--provider", "semantic_scholar")
    assert "semantic_scholar_api_key must be configured" in out


def test_a_query_one_provider_cannot_say_is_a_usage_error(config):
    out = _fails(config, "--search", "abstract: fMRI", "--provider", "sciencedirect")
    assert "ScienceDirect's search has no abstract field" in out


@pytest.mark.vcr(match_on=["method", "scheme", "host", "port", "path", "query", "body"])
def test_a_search_sciencedirect_cannot_split_stops_before_any_provider_searches(
    config, tmp_path, monkeypatch):
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
        f'    {{"qs": "{q}", "date": "2025-2026"}}: 54 found; split by year: 21 + 33',
        f'      {{"qs": "{q}", "date": "2026"}}: 33 found; split into halves of its OR clauses',
        '        {"qs": "fMRI AND hyperacusis", "date": "2026"}: 23 found',
    ]:
        assert line in out
    # the plan is printed before the search pages anything
    assert out.index("split by year") < out.index("sciencedirect: 54 records")


# -- the search is kept ------------------------------------------------------

ALL_PROVIDERS = "test_every_provider_is_searched_and_counted_after_enrichment.yaml"


def _manifest_lines(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


def _searches(config, *args):
    result = CliRunner().invoke(cli.app, ["searches", *args, "--config", str(config)])
    assert result.exit_code == 0, result.output
    return result.output


@pytest.mark.vcr(ALL_PROVIDERS)
def test_a_search_is_kept_with_what_each_provider_found(config, tmp_path):
    out = _add(config, "--search", "fMRI AND misophonia")

    assert "kept as search 1" in out
    with Catalog.open(tmp_path / "catalog") as catalog:
        (run,) = catalog.searches()
        assert run["query"] == "fMRI AND misophonia"
        assert run["years"] == "1990 onward"
        providers = run["providers"]
        assert list(providers) == ["pubmed", "semantic_scholar", "openalex", "sciencedirect"]
        assert providers["sciencedirect"]["query"] == (
            '{"qs": "fMRI AND misophonia", "date": "1990-9999"}'
        )
        assert providers["semantic_scholar"]["query"] == (
            "query=fMRI + misophonia year=1990-"
        )
        assert {n: p["articles"] for n, p in providers.items()} == {
            "pubmed": 11, "semantic_scholar": 13, "openalex": 12, "sciencedirect": 73,
        }
        assert providers["semantic_scholar"]["without_id"] == 2
        assert providers["semantic_scholar"]["records"] == 15
        assert len(catalog.search_articles(1)) == 87
        assert len(catalog.search_articles(1, only="sciencedirect")) == 70
        assert len(catalog.search_articles(1, found_by=["pubmed", "openalex"])) == 11 + 12 - 10


@pytest.mark.vcr(ALL_PROVIDERS)
def test_the_articles_only_one_provider_found_are_a_manifest(config, tmp_path):
    _add(config, "--search", "fMRI AND misophonia")
    only = tmp_path / "only.jsonl"
    rest = tmp_path / "rest.jsonl"

    out = _searches(
        config, "manifest", "1", "--only-found-by", "sciencedirect", "--out", str(only)
    )
    _searches(
        config, "manifest", "1", "--out", str(rest),
        "--found-by", "pubmed", "--found-by", "semantic_scholar", "--found-by", "openalex",
    )

    assert f"70 articles -> {only}" in out
    only_ids, rest_ids = _manifest_lines(only), _manifest_lines(rest)
    assert len(only_ids) == 70 and len(rest_ids) == 17
    with Catalog.open(tmp_path / "catalog") as catalog:
        held_back = {catalog.resolve(Identifier(**row)).id for row in only_ids}
        kept = {catalog.resolve(Identifier(**row)).id for row in rest_ids}
        assert not held_back & kept
        assert len(held_back | kept) == catalog.count_articles() == 87


@pytest.mark.vcr(ALL_PROVIDERS)
def test_searches_list_names_the_query_each_provider_was_sent(config):
    _add(config, "--search", "fMRI AND misophonia")
    out = _searches(config, "list")

    assert "search 1" in out and "years 1990 onward" in out
    assert "query fMRI AND misophonia" in out
    assert "sciencedirect: 73 articles from 73 records, 0 without an id; sent {" in out
    assert "semantic_scholar: 13 articles from 15 records, 2 without an id" in out


@pytest.mark.vcr("test_a_second_run_finds_them_in_the_catalog.yaml")
def test_a_second_search_is_a_second_run(config, tmp_path):
    _openalex(config)
    out = _openalex(config)

    assert "kept as search 2" in out
    with Catalog.open(tmp_path / "catalog") as catalog:
        assert [run["id"] for run in catalog.searches()] == [1, 2]
        assert catalog.search_articles(1) == catalog.search_articles(2)


@pytest.mark.vcr("test_a_second_run_finds_them_in_the_catalog.yaml")
def test_a_manifest_names_only_what_the_search_searched(config, tmp_path):
    _openalex(config)
    for args in (["manifest", "7"], ["manifest", "1", "--only-found-by", "pubmed"]):
        result = CliRunner().invoke(cli.app, ["searches", *args, "--config", str(config)])
        assert result.exit_code == 2, result.output
    result = CliRunner().invoke(
        cli.app,
        ["searches", "manifest", "1", "--found-by", "openalex", "--only-found-by", "openalex",
         "--config", str(config)],
    )
    assert result.exit_code == 2


@pytest.mark.vcr(ALL_PROVIDERS)
def test_a_manifest_defaults_to_the_data_root_and_lists_articles_oldest_first(
        config, tmp_path):
    _add(config, "--search", "fMRI AND misophonia")

    out = _searches(config, "manifest", "1", "--only-found-by", "sciencedirect")

    path = tmp_path / "data" / "manifests" / "search-1.jsonl"
    assert f"70 articles -> {path}" in out
    with Catalog.open(tmp_path / "catalog") as catalog:
        written = [catalog.resolve(Identifier(**row)).id for row in _manifest_lines(path)]
        assert written == catalog.search_articles(1, only="sciencedirect")
        ages = [
            catalog._conn.execute("SELECT created_at FROM articles WHERE id=?", (i,)).fetchone()[0]
            for i in written
        ]
        assert ages == sorted(ages)


def test_searches_list_with_no_search_kept(config):
    assert "no searches kept." in _searches(config, "list")
