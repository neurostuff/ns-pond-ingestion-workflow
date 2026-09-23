"""The CLI surface: five verbs over one catalog."""

from __future__ import annotations

import json

import pytest
from ingestion_workflow.catalog import Catalog
from ingestion_workflow.cli.main import _parse_identifier, app
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
            }
        ),
        encoding="utf-8",
    )
    return path


def run(*args):
    result = runner.invoke(app, list(args))
    assert result.exit_code == 0, result.output
    return result.output


@pytest.mark.parametrize(
    "token,field",
    [
        ("37961286", "pmid"),
        ("PMC10634720", "pmcid"),
        ("10.1101/2023.10.21.563317", "doi"),
        ("doi:10.1/x", "doi"),
    ],
)
def test_identifiers_are_recognised_without_being_labelled(token, field):
    identifier = _parse_identifier(token)
    assert getattr(identifier, field)


def test_unrecognisable_identifiers_are_rejected():
    with pytest.raises(Exception):
        _parse_identifier("not-an-id")


def test_add_registers_articles(config, tmp_path):
    out = run("add", "37961286", "PMC10634720", "--config", str(config))
    assert "2 new articles" in out
    with Catalog.open(tmp_path / "catalog") as catalog:
        assert catalog.count_articles() == 2


def test_add_is_idempotent(config, tmp_path):
    run("add", "37961286", "--config", str(config))
    out = run("add", "37961286", "--config", str(config))
    assert "0 new articles" in out
    with Catalog.open(tmp_path / "catalog") as catalog:
        assert catalog.count_articles() == 1


def test_add_from_a_file(config, tmp_path):
    listing = tmp_path / "ids.txt"
    listing.write_text("# a comment\n37961286\nPMC10634720\n\n", encoding="utf-8")
    out = run("add", "--file", str(listing), "--config", str(config))
    assert "2 new articles" in out


def test_status_reports_an_empty_catalog(config):
    out = run("status", "--config", str(config))
    assert "0 articles" in out
    assert "no stage has run yet" in out


def test_dry_run_changes_nothing(config, tmp_path):
    run("add", "37961286", "--config", str(config))
    out = run("run", "--stage", "download", "--dry-run", "--config", str(config))
    assert "plan" in out
    with Catalog.open(tmp_path / "catalog") as catalog:
        assert catalog.status_counts() == {}


def test_show_explains_an_unknown_article(config):
    result = runner.invoke(app, ["show", "12345", "--config", str(config)])
    assert result.exit_code == 1
    assert "not in the catalog" in result.output


def test_show_lists_what_is_known(config, tmp_path):
    run("add", "37961286", "--config", str(config))
    out = run("show", "37961286", "--config", str(config))
    assert "pmid 37961286" in out


def test_show_resolves_every_kind_of_id(config, tmp_path):
    """A user has four different strings that name the same article, and should
    not have to know which kind the CLI wants."""
    from ingestion_workflow.catalog import Catalog
    from ingestion_workflow.models.ids import Identifier

    run("add", "PMC10634720", "--config", str(config))
    with Catalog.open(tmp_path / "catalog") as catalog:
        ref = catalog.resolve(Identifier(pmcid="PMC10634720"))
        catalog.add_aliases([(ref.id, "neurostore", "5Qk2mNpXyJKH")])
        article_id = ref.id

    for token in ("PMC10634720", "5Qk2mNpXyJKH", article_id):
        out = run("show", token, "--config", str(config))
        assert article_id in out, f"{token} did not resolve"


def test_show_still_rejects_an_unknown_token(config):
    run("add", "PMC10634720", "--config", str(config))
    result = runner.invoke(app, ["show", "zzzzzzzzzzzz", "--config", str(config)])
    assert result.exit_code == 1
    assert "not in the catalog" in result.output


def test_add_accepts_a_neurostore_id(config, tmp_path):
    """base_study_ids are opaque, so they cannot be sniffed like a PMID or DOI
    and need their own flag."""
    from ingestion_workflow.catalog import Catalog
    from ingestion_workflow.models.ids import Identifier

    run("add", "--neurostore", "5Qk2mNpXyJKH", "--config", str(config))
    with Catalog.open(tmp_path / "catalog") as catalog:
        ref = catalog.resolve(Identifier(neurostore="5Qk2mNpXyJKH"))
        assert ref is not None
        assert catalog.identifier(ref.id).neurostore == "5Qk2mNpXyJKH"


def test_a_neurostore_only_article_has_nothing_to_download(config):
    """No extractor can address an article by base_study_id alone -- pubget
    needs a pmcid, elsevier a pmid or doi. Such an article is inert until its
    bibliographic ids are known."""
    run("add", "--neurostore", "5Qk2mNpXyJKH", "--config", str(config))
    out = run("run", "--select", "all", "--dry-run", "--config", str(config))
    download = next(line for line in out.splitlines() if "download" in line)
    assert "0 pending" in download
    assert "1 skipped" in download
