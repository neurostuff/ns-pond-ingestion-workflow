"""`ingest repair`: count by default, back up before it writes anything."""

import json
import sqlite3
from contextlib import closing

import ingestion_workflow.cli.main as cli
import pytest
from ingestion_workflow.catalog import Catalog, Outcome
from ingestion_workflow.config import Settings
from ingestion_workflow.models import DownloadSource
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.pipeline.stages import DownloadStage
from typer.testing import CliRunner


@pytest.fixture()
def stranded(tmp_path):
    config = tmp_path / "settings.yaml"
    config.write_text(
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
    with Catalog.open(tmp_path / "catalog") as cat:
        a = cat.register(Identifier(doi="10.1/c1"))
        b = cat.register(Identifier(pmcid="PMC81"))
        survivor = cat.register(Identifier(doi="10.1/c1", pmcid="PMC81")).id
        retired = b.id if survivor == a.id else a.id
        cat.exclude([(survivor, "t1", "kept", "")])
        for table in ("t1", "t2"):
            cat._conn.execute(
                "INSERT INTO exclusions VALUES (?, ?, 'lost', '', '2026-01-01')",
                (retired, table),
            )
    return config, tmp_path, survivor, retired


def _repair(config, *args):
    result = CliRunner().invoke(cli.app, ["repair", *args, "--config", str(config)])
    assert result.exit_code == 0, result.output
    return result.output


def _rows(root):
    with closing(sqlite3.connect(root / "catalog" / "catalog.sqlite")) as conn:
        return sorted(conn.execute("SELECT article_id, table_id, reason FROM exclusions"))


def test_the_default_counts_and_writes_nothing(stranded):
    config, tmp_path, survivor, retired = stranded
    before = _rows(tmp_path)

    out = _repair(config)

    assert "2 exclusions" in out and "dry run" in out
    assert _rows(tmp_path) == before
    assert not list(tmp_path.glob("*.sqlite"))


def test_apply_copies_the_catalog_then_moves_the_rows_and_names_what_a_clash_dropped(stranded):
    config, tmp_path, survivor, retired = stranded
    before = _rows(tmp_path)
    backup = tmp_path / "before.sqlite"

    out = _repair(config, "--apply", "--backup", str(backup))

    assert "moved 1 exclusions, dropped 1" in out
    assert f"dropped {retired} t1: lost" in out
    assert _rows(tmp_path) == [(survivor, "t1", "kept"), (survivor, "t2", "lost")]
    with closing(sqlite3.connect(backup)) as old:
        assert sorted(old.execute("SELECT article_id, table_id, reason FROM exclusions")) == before
    assert "nothing to repair" in _repair(config)


def test_apply_needs_a_new_backup_path(stranded):
    config, tmp_path, *_ = stranded
    taken = tmp_path / "taken.sqlite"
    taken.write_text("x")

    for args in (["--apply"], ["--apply", "--backup", str(taken)]):
        result = CliRunner().invoke(cli.app, ["repair", *args, "--config", str(config)])
        assert result.exit_code != 0
    assert len(_rows(tmp_path)) == 3


@pytest.fixture()
def migrated(tmp_path):
    """One downloaded article as the migration imported it: no fingerprint."""
    config = tmp_path / "settings.yaml"
    config.write_text(json.dumps({
        "data_root": str(tmp_path / "data"), "cache_root": str(tmp_path / "cache"),
        "catalog_root": str(tmp_path / "catalog"), "ns_pond_root": str(tmp_path / "pond"),
        "download_sources": ["pubget"], "log_to_file": False, "show_progress": False,
    }), encoding="utf-8")
    with Catalog.open(tmp_path / "catalog") as cat:
        ref = cat.register(Identifier(pmid="1", pmcid="PMC1"))
        cat.record([Outcome(article_id=ref.id, stage="download", source="pubget",
                            payload={"files": []})])
    return config, tmp_path, ref.id


def _fingerprint(root, article_id):
    with closing(sqlite3.connect(root / "catalog" / "catalog.sqlite")) as conn:
        return conn.execute("SELECT fingerprint FROM artifacts WHERE article_id=?",
                            (article_id,)).fetchone()[0]


def test_the_default_counts_the_fingerprints_it_would_fill(migrated):
    config, tmp_path, article_id = migrated

    out = _repair(config)

    row = next(line.split() for line in out.splitlines() if line.split()[:1] == ["download"])
    assert row[1:] == ["1", "0", "0", "0", "0", "0"]
    assert "dry run" in out
    assert _fingerprint(tmp_path, article_id) == ""


def test_apply_copies_the_catalog_then_stores_the_fingerprints(migrated):
    config, tmp_path, article_id = migrated
    backup = tmp_path / "before.sqlite"

    out = _repair(config, "--apply", "--backup", str(backup))

    assert "stored 1 fingerprints" in out
    assert _fingerprint(tmp_path, article_id) == DownloadStage(Settings()).fingerprint_for(
        DownloadSource.PUBGET)
    with closing(sqlite3.connect(backup)) as old:
        assert old.execute("SELECT fingerprint FROM artifacts").fetchone()[0] == ""
    assert "nothing to repair" in _repair(config)
