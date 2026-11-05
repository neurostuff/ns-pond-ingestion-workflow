"""Schema v5: a `runs` table and `artifacts.run_id`, added to a v4 catalog in place."""

from __future__ import annotations

import sqlite3

import pytest
from ingestion_workflow.catalog import Catalog, Outcome, Run, RunKind, Status

#: The artifacts table as schema v4 made it: no run_id.
V4_ARTIFACTS = """
CREATE TABLE articles (id TEXT PRIMARY KEY, created_at TEXT NOT NULL, merged_into TEXT);
CREATE TABLE artifacts (
    article_id  TEXT NOT NULL,
    stage       TEXT NOT NULL,
    source      TEXT NOT NULL DEFAULT '',
    status      TEXT NOT NULL,
    fingerprint TEXT NOT NULL DEFAULT '',
    blob        TEXT,
    summary     TEXT NOT NULL DEFAULT '{}',
    error       TEXT,
    updated_at  TEXT NOT NULL,
    PRIMARY KEY (article_id, stage, source)
);
CREATE INDEX artifacts_stage_status_idx ON artifacts(stage, status);
CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
INSERT INTO meta VALUES ('schema_version', '4');
INSERT INTO articles VALUES ('a1', '2026-01-01', NULL);
INSERT INTO artifacts VALUES ('a1', 'sync', '', 'ok', 'fp', NULL, '{"parse_id": "p"}', NULL,
                              '2026-01-02');
"""


def _v4(root):
    root.mkdir()
    with sqlite3.connect(root / "catalog.sqlite") as conn:
        conn.executescript(V4_ARTIFACTS)


def _run(**kwargs) -> Run:
    base = dict(run_id="r1", kind=RunKind.MODEL, stage="record", source="ns-27b",
                model="Qwen/Qwen3.8-27B",
                adapter="jdkent/qwen3.8-27b-ns-record@dbfd846bd3e1259039eb2f3117dc12bd733c640a",
                config={"max_len": 65536}, created_at="2026-10-31T00:00:00+00:00")
    base.update(kwargs)
    return Run(**base)


def test_a_v4_catalog_gains_run_ids_and_keeps_its_rows(tmp_path):
    root = tmp_path / "cat"
    _v4(root)
    with Catalog.open(root) as catalog:
        sync = catalog.artifact("a1", "sync")
        assert sync.summary == {"parse_id": "p"} and sync.run_id is None
        assert catalog.runs() == []
    with sqlite3.connect(root / "catalog.sqlite") as conn:
        assert conn.execute("SELECT value FROM meta WHERE key='schema_version'").fetchone() == ("5",)
        assert "run_id" in {row[1] for row in conn.execute("PRAGMA table_info(artifacts)")}
        assert conn.execute("SELECT name FROM sqlite_master WHERE name='artifacts_run_idx'"
                            ).fetchone()


def test_adding_run_ids_takes_no_copy_of_the_catalog(tmp_path):
    """v5 only adds; a production catalog is 6.7 GB on a disk that is 99% full."""
    root = tmp_path / "cat"
    _v4(root)
    Catalog.open(root).close()
    assert list(root.glob("catalog.pre-*")) == []


def test_an_artifact_keeps_the_run_it_came_from(tmp_path):
    with Catalog.open(tmp_path / "cat") as catalog:
        catalog._conn.execute("INSERT INTO articles(id, created_at) VALUES('a1', '2026-01-01')")
        catalog.record_run(_run())
        catalog.record([Outcome("a1", "record", "ns-27b", Status.OK, "fp", payload={"x": 1},
                                run_id="r1")])
        assert catalog.artifact("a1", "record", "ns-27b").run_id == "r1"
        kept = catalog.run("r1")
        assert kept.config == {"max_len": 65536} and kept.kind is RunKind.MODEL
        assert kept.imported_at


def test_a_run_id_names_one_run_for_good(tmp_path):
    with Catalog.open(tmp_path / "cat") as catalog:
        catalog.record_run(_run())
        catalog.record_run(_run(machine="vista"))  # where it ran is not what it is
        with pytest.raises(ValueError, match="another run"):
            catalog.record_run(_run(adapter="another/adapter"))


def test_two_runs_of_one_model_have_one_identity_and_one_id():
    first, again = (_run(run_id="r1", config={"shards": [1]}),
                    _run(run_id="r2", config={"shards": [2]}, machine="ls6",
                         pipeline="ns-pond offload", code_commit="abc"))
    assert first.identity == again.identity and first.minted_id == again.minted_id
    assert _run().identity != _run(prompt_version="v2").identity
    assert _run(stage="normalize", fit="f1").minted_id != _run(stage="normalize", fit="f2").minted_id


def test_a_rerun_is_kept_under_the_run_it_repeats(tmp_path):
    with Catalog.open(tmp_path / "cat") as catalog:
        catalog.record_run(_run())
        with pytest.raises(ValueError, match="is run r1"):
            catalog.record_run(_run(run_id="r2", machine="ls6"))
