"""`ingest data-sources pull` stores the dictionary; `export` writes it for neurostore."""

from __future__ import annotations

import csv
import json

import ingestion_workflow.cli.main as cli
import pytest
from ingestion_workflow.catalog import Catalog
from ingestion_workflow.data_sources.merge import curated_records
from typer.testing import CliRunner

runner = CliRunner()


@pytest.fixture()
def config(tmp_path, monkeypatch):
    monkeypatch.delenv("SCICRUNCH_API_KEY", raising=False)
    path = tmp_path / "settings.yaml"
    path.write_text(json.dumps({"data_root": str(tmp_path / "data"),
                                "cache_root": str(tmp_path / "cache"),
                                "catalog_root": str(tmp_path / "catalog"),
                                "log_to_file": False, "show_progress": False}),
                    encoding="utf-8")
    return path


def test_pull_of_the_curated_tsv_then_export(config, tmp_path):
    pulled = runner.invoke(cli.app, ["data-sources", "pull", "-p", "curated", "-c", str(config)])
    assert pulled.exit_code == 0, pulled.output
    curated = len(curated_records())
    assert f"{curated:,} data sources" in pulled.output
    with Catalog.open(tmp_path / "catalog") as catalog:
        assert catalog.data_sources_digest() is not None

    exported = runner.invoke(cli.app, ["data-sources", "export", str(tmp_path / "out"),
                                       "-c", str(config)])
    assert exported.exit_code == 0, exported.output
    with open(tmp_path / "out" / "data_sources.tsv", newline="") as f:
        assert len(list(csv.DictReader(f, delimiter="\t"))) == curated


def test_an_unknown_catalogue_is_refused(config):
    refused = runner.invoke(cli.app, ["data-sources", "pull", "-p", "scicrunch",
                                      "-c", str(config)])
    assert refused.exit_code != 0
    assert "cannot pull scicrunch" in refused.output
