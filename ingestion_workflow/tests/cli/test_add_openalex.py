"""`ingest add --openalex-query`: search, report, register."""

import json
import os

import ingestion_workflow.cli.main as cli
import pytest
from ingestion_workflow.catalog import Catalog
from typer.testing import CliRunner


@pytest.fixture()
def config(tmp_path):
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
                "openalex_email": "test@example.com",
                "openalex_api_key": os.environ.get("OPENALEX_API_KEY", "replay"),
            }
        ),
        encoding="utf-8",
    )
    return path


def _add(config):
    result = CliRunner().invoke(
        cli.app,
        ["add", "--openalex-query", "fMRI AND synesthesia", "--openalex-year", "2026",
         "--no-enrich", "--config", str(config)],
    )
    assert result.exit_code == 0, result.output
    return result.output


@pytest.mark.vcr()
def test_hits_are_reported_registered_and_the_bare_work_is_counted(config, tmp_path):
    out = _add(config)

    assert "openalex: 3 articles" in out
    assert "3 distinct: 0 already in the catalog, 3 new" in out
    assert "1 OpenAlex works have no DOI, PMID or PMCID and were not registered" in out
    with Catalog.open(tmp_path / "catalog") as catalog:
        assert catalog.count_articles() == 2


@pytest.mark.vcr()
def test_a_second_run_finds_them_in_the_catalog(config, tmp_path):
    _add(config)
    out = _add(config)

    assert "2 already in the catalog, 1 new" in out
