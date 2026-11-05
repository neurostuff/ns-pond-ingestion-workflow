"""`ingest report coverage`: articles by stage, source, run, model and status, and what
changed since upload."""

from __future__ import annotations

import json
import sqlite3

import ingestion_workflow.cli.main as cli
from ingestion_workflow.catalog import Catalog, Outcome, Run, RunKind, Status
from ingestion_workflow.services.coverage import coverage
from typer.testing import CliRunner


def _catalog(root):
    with Catalog.open(root) as catalog:
        for article_id in ("a1", "a2", "a3"):
            catalog._conn.execute(
                "INSERT INTO articles(id, created_at) VALUES(?, '2026-01-01')", (article_id,))
        catalog.record_run(Run("r1", RunKind.MODEL, "record", "ns-27b",
                               model="Qwen/Qwen3.8-27B", created_at="2026-10-31"))
        catalog.record([Outcome(a, "sync", "", Status.OK, "fp", summary={"parse_id": f"p-{a}"})
                        for a in ("a1", "a2", "a3")])
        # a1 and a2 were uploaded with the parse they hold then
        catalog.record([Outcome(a, "upload", "", Status.OK, "fp", summary={"parse_id": f"p-{a}"})
                        for a in ("a1", "a2")])
        # a2's parse moved after its upload; a1 then gained a record
        catalog.record([Outcome("a2", "sync", "", Status.OK, "fp", summary={"parse_id": "p-new"})])
        catalog._conn.execute("UPDATE artifacts SET updated_at='2099-01-01' WHERE "
                              "article_id='a2' AND stage='sync'")
        catalog.record([
            Outcome("a1", "record", "ns-27b", Status.OK, "fp", payload={}, run_id="r1"),
            Outcome("a3", "record", "ns-27b", Status.FAILED, "fp", error="text hash differs",
                    run_id="r1"),
        ])
        catalog._conn.execute("UPDATE artifacts SET updated_at='2099-01-01' WHERE "
                              "stage='record'")


def test_articles_are_counted_by_stage_source_run_model_and_status(tmp_path):
    _catalog(tmp_path / "cat")
    found = coverage(tmp_path / "cat")
    rows = {(s["stage"], s["source"], s["run_id"], s["model"], s["status"]): s["articles"]
            for s in found["stages"]}
    assert rows == {
        ("sync", "", None, None, "ok"): 3,
        ("record", "ns-27b", "r1", "Qwen/Qwen3.8-27B", "ok"): 1,
        ("record", "ns-27b", "r1", "Qwen/Qwen3.8-27B", "failed"): 1,
        ("upload", "", None, None, "ok"): 2,
    }
    assert [s["stage"] for s in found["stages"]][-1] == "upload"
    assert found["runs"][0]["articles"] == {"ok": 1, "failed": 1}


def test_studies_changed_since_upload_are_counted_by_stage(tmp_path):
    _catalog(tmp_path / "cat")
    changed = coverage(tmp_path / "cat")["changed_since_upload"]
    assert changed == {"uploaded": 2, "any": 2,
                       "by_stage": {"sync": 1, "record": 1, "evidence": 0, "normalize": 0}}


def test_the_report_reads_a_v4_catalog_and_writes_nothing_to_it(tmp_path):
    root = tmp_path / "cat"
    root.mkdir()
    with sqlite3.connect(root / "catalog.sqlite") as conn:
        conn.executescript("""
            CREATE TABLE articles (id TEXT PRIMARY KEY, created_at TEXT, merged_into TEXT);
            CREATE TABLE artifacts (article_id TEXT, stage TEXT, source TEXT, status TEXT,
                fingerprint TEXT, blob TEXT, summary TEXT, error TEXT, updated_at TEXT);
            CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
            INSERT INTO meta VALUES ('schema_version', '4');
            INSERT INTO articles VALUES ('a1', '2026-01-01', NULL);
            INSERT INTO artifacts VALUES ('a1', 'sync', '', 'ok', 'fp', NULL, '{}', NULL, 'x');
        """)
    found = coverage(root)
    assert found["schema_version"] == 4 and found["runs"] == []
    assert found["stages"] == [{"stage": "sync", "source": "", "run_id": None, "model": None,
                                "status": "ok", "articles": 1}]


def test_the_command_prints_the_counts_and_writes_the_json(tmp_path):
    _catalog(tmp_path / "cat")
    out = tmp_path / "coverage.json"
    result = CliRunner().invoke(cli.app, ["report", "coverage", "--catalog",
                                          str(tmp_path / "cat"), "--json", str(out)])
    assert result.exit_code == 0, result.output
    assert "record       ns-27b      r1" in result.output
    assert "changed since upload: 2 of 2 uploaded studies" in result.output
    assert json.loads(out.read_text())["changed_since_upload"]["any"] == 2


def test_runs_list_names_each_run_and_its_articles(tmp_path):
    _catalog(tmp_path / "cat")
    result = CliRunner().invoke(cli.app, ["runs", "list", "--catalog", str(tmp_path / "cat")])
    assert result.exit_code == 0, result.output
    assert "r1  model record/ns-27b" in result.output
    assert "articles: 1 failed, 1 ok" in result.output
