"""`ingest repair-identity`: a reviewed plan, dry run by default, all or nothing with a backup."""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing

import ingestion_workflow.cli.main as cli
import pytest
from ingestion_workflow.catalog import Catalog, Outcome, Status
from ingestion_workflow.models.ids import Identifier, Identifiers
from ingestion_workflow.pipeline.selection import resolve_manifest
from typer.testing import CliRunner


@pytest.fixture()
def setup(tmp_path):
    config = tmp_path / "settings.yaml"
    config.write_text(json.dumps({
        "data_root": str(tmp_path / "data"),
        "cache_root": str(tmp_path / "cache"),
        "catalog_root": str(tmp_path / "catalog"),
        "ns_pond_root": str(tmp_path / "pond"),
        "log_to_file": False,
        "show_progress": False,
    }), encoding="utf-8")
    with Catalog.open(tmp_path / "catalog") as cat:
        # Two articles with each other's PMIDs; one holding a companion abstract's DOI and
        # its download; one holding a repository copy's DOI.
        ids = {
            "a": cat.register(Identifier(doi="10.1016/j.ajog.2011.02.050", pmid="21529779")).id,
            "b": cat.register(Identifier(doi="10.1016/j.jse.2011.01.043", pmid="21529758")).id,
            "c": cat.register(Identifier(doi="10.1016/j.bpsc.2021.12.008", pmid="34954395")).id,
            "d": cat.register(Identifier(doi="10.1176/ajp.2006.163.4.735", pmid="16585452")).id,
        }
        # Written as the catalog took them before registration refused a second journal DOI.
        cat._conn.executemany("INSERT INTO aliases VALUES ('doi', ?, ?)", [
            ("10.1016/j.biopsych.2021.02.423", ids["c"]),
            ("10.1176/appi.ajp.163.4.735", ids["d"]),
        ])
        cat.record([Outcome(ids["c"], "download", "elsevier", Status.OK, payload="abstract"),
                    Outcome(ids["c"], "download", "pubget", Status.OK, payload="paper")])
    return config, tmp_path, ids


def _plan(tmp_path, ids):
    a, b, c, d = ids["a"], ids["b"], ids["c"], ids["d"]
    lines = [
        {"op": "reassign", "moves": [
            {"kind": "pmid", "value": "21529779", "from": a, "to": b},
            {"kind": "pmid", "value": "21529758", "from": b, "to": a}],
         "why": "PubMed 21529758 is 10.1016/j.ajog.2011.02.050"},
        {"op": "split", "article": c, "aliases": [["doi", "10.1016/j.biopsych.2021.02.423"]],
         "artifacts": [["download", "elsevier"]]},
        {"op": "remove", "article": d, "kind": "doi", "value": "10.1176/appi.ajp.163.4.735"},
        {"op": "add", "article": d, "kind": "pmcid", "value": "PMC1234567"},
    ]
    path = tmp_path / "plan.jsonl"
    path.write_text("\n".join(json.dumps(line) for line in lines) + "\n", encoding="utf-8")
    return path


def _invoke(config, *args):
    return CliRunner().invoke(
        cli.app, ["repair-identity", *map(str, args), "--config", str(config)])


def _db(tmp_path):
    with closing(sqlite3.connect(tmp_path / "catalog" / "catalog.sqlite")) as conn:
        return [sorted(conn.execute(f"SELECT * FROM {t}"))
                for t in ("articles", "aliases", "artifacts")]


def test_the_default_runs_the_plan_and_writes_nothing(setup):
    config, tmp_path, ids = setup
    before = _db(tmp_path)

    result = _invoke(config, _plan(tmp_path, ids))

    assert result.exit_code == 0, result.output
    assert "4 of 4 operations would apply; 1 new articles; 5 articles touched" in result.output
    assert "dry run: nothing written" in result.output
    assert _db(tmp_path) == before


def test_apply_needs_a_new_backup_and_a_manifest(setup):
    config, tmp_path, ids = setup
    plan = _plan(tmp_path, ids)
    assert _invoke(config, plan, "--apply").exit_code == 2
    assert _invoke(config, plan, "--apply", "--backup", tmp_path / "b.sqlite").exit_code == 2
    (tmp_path / "taken.sqlite").write_text("")
    assert _invoke(config, plan, "--apply", "--backup", tmp_path / "taken.sqlite",
                   "--manifest", tmp_path / "m.jsonl").exit_code == 2


def test_apply_writes_the_plan_backs_up_first_and_lists_the_touched_articles(setup):
    config, tmp_path, ids = setup
    before = _db(tmp_path)
    backup, manifest = tmp_path / "pre-identity.sqlite", tmp_path / "touched.jsonl"

    result = _invoke(config, _plan(tmp_path, ids), "--apply", "--backup", backup,
                     "--manifest", manifest)

    assert result.exit_code == 0, result.output
    assert ("ingest run -s metadata --refresh metadata -s triage --refresh triage "
            "-s prose --refresh prose --manifest") in result.output
    with closing(sqlite3.connect(backup)) as conn:
        assert [sorted(conn.execute(f"SELECT * FROM {t}"))
                for t in ("articles", "aliases", "artifacts")] == before
    with Catalog.open(tmp_path / "catalog") as cat:
        assert cat.identifier(ids["a"]).pmid == "21529758"
        assert cat.identifier(ids["b"]).pmid == "21529779"
        companion = cat.resolve(Identifier(doi="10.1016/j.biopsych.2021.02.423")).id
        assert companion not in ids.values()
        assert cat.artifact(companion, "download", "elsevier") is not None
        assert cat.artifact(ids["c"], "download", "pubget") is not None
        assert cat.aliases(ids["d"], "doi") == ["10.1176/ajp.2006.163.4.735"]
        assert cat.identifier(ids["d"]).pmcid == "PMC1234567"

        listed = Identifiers.load(manifest).identifiers
        assert len(listed) == 5
        articles = cat.count_articles()
        # Registered again by `ingest run --manifest`, each line is the article it came from.
        refs = resolve_manifest(cat, manifest, register=True).refs
        assert sorted(r.id for r in refs) == sorted([*ids.values(), companion])
        assert cat.count_articles() == articles and cat.conflicts == 0


def test_a_plan_the_catalog_no_longer_matches_is_refused_whole(setup):
    config, tmp_path, ids = setup
    plan = _plan(tmp_path, ids)
    with Catalog.open(tmp_path / "catalog") as cat:
        # Since the plan was made, the repository DOI was settled by someone else.
        cat.remove_aliases([("doi", "10.1176/appi.ajp.163.4.735", ids["d"])])
    before = _db(tmp_path)

    result = _invoke(config, plan, "--apply", "--backup", tmp_path / "b.sqlite",
                     "--manifest", tmp_path / "m.jsonl")

    assert result.exit_code == 1
    assert ("refused line 3 (remove): doi 10.1176/appi.ajp.163.4.735 is held by no article"
            in result.output)
    assert "3 of 4 operations would apply" in result.output
    assert _db(tmp_path) == before
    assert not (tmp_path / "m.jsonl").exists()


def test_a_plan_adding_an_identifier_another_article_holds_is_refused(setup):
    config, tmp_path, ids = setup
    plan = tmp_path / "plan.jsonl"
    plan.write_text(json.dumps({"op": "add", "article": ids["a"], "kind": "pmid",
                                "value": "34954395"}) + "\n", encoding="utf-8")
    result = _invoke(config, plan)
    assert result.exit_code == 1
    assert f"pmid 34954395 is held by {ids['c']}" in result.output


@pytest.mark.parametrize("line, why", [
    ("{not json", "line 1: not JSON"),
    (json.dumps({"op": "rename"}), "line 1: op must be one of"),
    (json.dumps({"op": "merge"}), "line 1: merge needs articles"),
])
def test_a_malformed_plan_is_a_usage_error(setup, line, why):
    config, tmp_path, _ = setup
    plan = tmp_path / "plan.jsonl"
    plan.write_text(line + "\n", encoding="utf-8")
    result = _invoke(config, plan)
    assert result.exit_code == 2
    assert why in result.output
