"""The one-off repair of DOIs two articles hold in different case, on a real catalog."""

from __future__ import annotations

import csv
import importlib.util
import io
import sqlite3
from pathlib import Path

import pytest
from ingestion_workflow.catalog import Catalog, Outcome
from ingestion_workflow.models.ids import Identifier

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "repair_doi_collisions.py"
spec = importlib.util.spec_from_file_location("repair_doi_collisions", SCRIPT)
repair = importlib.util.module_from_spec(spec)
spec.loader.exec_module(repair)

ROWS = [
    # the production pattern: a foreign DOI on another journal's paper
    ("10.1093/CERCOR/BHI095", "drop", "neuroimage", "cercor"),
    ("10.3233/RNN-1999-00078", "merge-duplicate-pubmed-records", "rnn1", "rnn2"),
    ("10.1093/CERCOR/11.9.888", "keep", "award1", "award2"),
]
ALIASES = [
    ("cercor", "pmid", "15858162"), ("cercor", "doi", "10.1093/CERCOR/BHI095"),
    ("neuroimage", "pmid", "16406257"), ("neuroimage", "doi", "10.1093/cercor/bhi095"),
    ("neuroimage", "doi", "10.1016/j.neuroimage.2005.11.026"),
    ("rnn1", "pmid", "22387514"), ("rnn1", "doi", "10.3233/RNN-1999-00078"),
    ("rnn2", "pmid", "12671262"), ("rnn2", "doi", "10.3233/rnn-1999-00078"),
    ("award1", "pmid", "11552704"), ("award1", "doi", "10.1093/CERCOR/11.9.888"),
    ("award2", "pmid", "11532893"), ("award2", "doi", "10.1093/cercor/11.9.888"),
]


@pytest.fixture()
def version_two(tmp_path):
    """A catalog as production is before the lower-case migration."""
    root = tmp_path / "cat"
    with Catalog.open(root) as cat:
        for article, kind, value in ALIASES:
            cat._conn.execute("INSERT OR IGNORE INTO articles(id, created_at) VALUES(?, ?)",
                              (article, "2026-01-01"))
            cat._conn.execute("INSERT INTO aliases(kind, value, article_id) VALUES(?, ?, ?)",
                              (kind, value, article))
        cat.record([Outcome(article_id="rnn2", stage="download", source="ace",
                            payload={"n": 1})])
        cat._conn.execute("UPDATE meta SET value='2' WHERE key='schema_version'")
    table = tmp_path / "decisions.tsv"
    with open(table, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, delimiter="\t")
        writer.writerow(["doi", "action", "article", "other", "evidence"])
        writer.writerows([(*row, "test") for row in ROWS])
    return root, table


def _dump(root):
    with sqlite3.connect(f"file:{root / 'catalog.sqlite'}?mode=ro", uri=True) as conn:
        return (sorted(conn.execute("SELECT * FROM aliases")),
                sorted(conn.execute("SELECT id, merged_into FROM articles")))


def test_a_dry_run_writes_nothing(version_two):
    root, table = version_two
    before = _dump(root)
    counts = repair.run(root, table=table, out=io.StringIO())
    assert counts == {"dropped": 1, "merged": 1, "kept": 1}
    assert _dump(root) == before
    assert not (root / "catalog.pre-doi-repair.sqlite").exists()


def test_the_repair_settles_each_decision_and_the_migration_then_finds_the_owner(version_two):
    root, table = version_two
    counts = repair.run(root, apply=True, table=table, out=io.StringIO())
    assert counts == {"dropped": 1, "merged": 1, "kept": 1}
    assert (root / "catalog.pre-doi-repair.sqlite").exists()
    again = _dump(root)
    assert repair.run(root, apply=False, table=table, out=io.StringIO()) == {"done": 2, "kept": 1}
    assert _dump(root) == again

    with Catalog.open(root) as cat:
        assert cat.resolve(Identifier(doi="10.1093/cercor/bhi095")).id == "cercor"
        neuroimage = cat.resolve(Identifier(pmid="16406257")).identifier
        assert neuroimage.doi == "10.1016/j.neuroimage.2005.11.026"
        # one paper, both PubMed records: the older article survives (ties to the lower id)
        paper = cat.resolve(Identifier(doi="10.3233/RNN-1999-00078"))
        assert paper.id == "rnn1"
        assert cat.resolve(Identifier(pmid="12671262")).id == "rnn1"
        assert {v for k, v in cat._conn.execute(
            "SELECT kind, value FROM aliases WHERE article_id='rnn1'") if k == "pmid"} == {
            "22387514", "12671262"}
        assert paper.identifier.pmid == "12671262"   # the earlier PubMed record
        assert cat.artifact("rnn1", "download", "ace") is not None
        assert cat.register(Identifier(pmid="22387514", doi="10.3233/rnn-1999-00078")).id == "rnn1"
        # two papers sharing a DOI stay two, and the guard still keeps them apart
        assert cat.resolve(Identifier(doi="10.1093/cercor/11.9.888")).id == "award2"
        assert cat.resolve(Identifier(pmid="11552704")).id == "award1"
        named_both = cat.register(Identifier(pmid="11552704", doi="10.1093/cercor/11.9.888"))
        assert named_both.id == "award1"
        assert cat.resolve(Identifier(pmid="11532893")).id == "award2"


def test_a_doi_the_named_owner_no_longer_holds_is_not_dropped(version_two, tmp_path):
    """A stale row must not leave the DOI with nobody."""
    root, _ = version_two
    stale = tmp_path / "stale.tsv"
    stale.write_text("doi\taction\tarticle\tother\tevidence\n"
                     "10.1093/CERCOR/BHI095\tdrop\tneuroimage\taward1\ttest\n", encoding="utf-8")
    before = _dump(root)
    assert repair.run(root, apply=True, table=stale, out=io.StringIO()) == {"skipped": 1}
    assert _dump(root) == before


def test_the_repair_runs_only_before_the_migration(version_two):
    root, table = version_two
    Catalog.open(root).close()
    with pytest.raises(SystemExit, match="already at schema version 4"):
        repair.run(root, apply=True, table=table, out=io.StringIO())


def test_the_decision_table_settles_the_fourteen_production_collisions():
    with open(repair.TABLE, newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh, delimiter="\t"))
    assert len(rows) == len({r["doi"].lower() for r in rows}) == 14
    assert {r["action"] for r in rows} == {"drop", "merge-duplicate-pubmed-records", "keep"}
    assert all(r["evidence"] and r["article"] != r["other"] for r in rows)
