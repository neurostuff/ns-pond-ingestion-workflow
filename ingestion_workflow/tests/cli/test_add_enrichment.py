"""A PubMed search returns PMIDs; pubget needs a PMCID.

Without enrichment every searched article is unreachable to pubget, which is
the source holding PMC full text -- the richest input the pipeline has.
"""

from __future__ import annotations

import json

import ingestion_workflow.cli.main as cli
import pytest
from ingestion_workflow.catalog import Catalog
from ingestion_workflow.models.ids import Identifier, Identifiers
from typer.testing import CliRunner

runner = CliRunner()


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
                "metadata_providers": ["pubmed"],
            }
        ),
        encoding="utf-8",
    )
    return path


class FakeLookup:
    """Stands in for a provider: fills the pmcid it knows."""

    KNOWN = {"111": "PMC111"}

    def __init__(self, settings, catalog=None):
        self.catalog = catalog

    def can_run(self):
        return True

    def find_identifiers(self, identifiers: Identifiers) -> Identifiers:
        for identifier in identifiers.identifiers:
            pmcid = self.KNOWN.get(identifier.pmid or "")
            if pmcid and not identifier.pmcid:
                identifier.pmcid = pmcid
                identifier.normalize()
        return identifiers


@pytest.fixture()
def patched(monkeypatch):
    import ingestion_workflow.services.id_lookup as lookup

    monkeypatch.setattr(lookup, "PubMedIDLookupService", FakeLookup, raising=False)
    monkeypatch.setattr(lookup, "SemanticScholarIDLookupService", FakeLookup, raising=False)
    monkeypatch.setattr(lookup, "OpenAlexIDLookupService", FakeLookup, raising=False)


def run(*args):
    result = runner.invoke(cli.app, list(args))
    assert result.exit_code == 0, result.output
    return result.output


def test_a_pmid_only_article_gains_its_pmcid(config, tmp_path, patched):
    run("add", "111", "--config", str(config))
    with Catalog.open(tmp_path / "catalog") as catalog:
        ref = catalog.resolve(Identifier(pmid="111"))
        assert catalog.identifier(ref.id).pmcid == "PMC111"


def test_no_enrich_leaves_it_alone(config, tmp_path, patched):
    run("add", "111", "--no-enrich", "--config", str(config))
    with Catalog.open(tmp_path / "catalog") as catalog:
        ref = catalog.resolve(Identifier(pmid="111"))
        assert catalog.identifier(ref.id).pmcid is None


def test_enrichment_does_not_create_duplicate_articles(config, tmp_path, patched):
    run("add", "111", "--config", str(config))
    with Catalog.open(tmp_path / "catalog") as catalog:
        assert catalog.count_articles() == 1


def test_an_unknown_pmid_survives_enrichment(config, tmp_path, patched):
    run("add", "999", "--config", str(config))
    with Catalog.open(tmp_path / "catalog") as catalog:
        ref = catalog.resolve(Identifier(pmid="999"))
        assert ref is not None
        assert catalog.identifier(ref.id).pmcid is None


def test_an_unconfigured_provider_is_skipped_not_fatal(config, tmp_path, monkeypatch):
    import ingestion_workflow.services.id_lookup as lookup

    class CannotRun(FakeLookup):
        def can_run(self):
            return False

    monkeypatch.setattr(lookup, "PubMedIDLookupService", CannotRun, raising=False)
    out = run("add", "111", "--config", str(config))
    assert "not configured" in out
