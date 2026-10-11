"""The funnel report: every article placed once per stage, with its reason and source."""

from __future__ import annotations

import json
import re

import ingestion_workflow.cli.main as cli
import pytest
from ingestion_workflow.catalog import Catalog, Outcome, Status
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.pipeline.stage import HELD_FOR_REVIEW, NO_TEXT
from ingestion_workflow.services.funnel_report import (
    BLOCKED,
    FAILED,
    FILTERED,
    HELD,
    NOT_ATTEMPTED,
    PASSED,
    PENDING,
    PERMANENT,
    STALE,
    TAKEN_BACK,
    UNACCOUNTED,
    build_funnel,
    categorize,
    open_readonly,
    render_html,
)
from typer.testing import CliRunner

OK = Status.OK
#: The stages of the upload path after download, with what each passes on.
PATH = (("extract", "pubget", {"has_text": True}),
        ("metadata", "", {}),
        ("passages", "", {"source": "pubget", "passages": 2}),
        ("prose", "", {"source": "pubget", "coordinates": 3}),
        ("resolve", "", {"tables": 1}),
        ("roles", "", {"tables": 1}),
        ("space", "", {"tables": 1}),
        ("upload", "", {"base_study_id": "b"}),
        ("sync", "", {"base_study_id": "b"}))


def _ok(article, stage, src="", **summary):
    return Outcome(article_id=article, stage=stage, source=src, status=OK, summary=summary)


def _through(article, last):
    """Downloaded from pubget and passed every stage up to and including `last`."""
    out = [_ok(article, "download", "pubget")]
    for stage, source, summary in PATH:
        out.append(_ok(article, stage, source, **summary))
        if stage == last:
            break
    return out


@pytest.fixture()
def catalog(tmp_path):
    cat = Catalog.open(tmp_path / "catalog")
    ids = {name: cat.register(Identifier(pmid=str(1000 + i))).id
           for i, name in enumerate("ABCDEFGHIJ")}
    a, b, c, d, e, f, g, h, i, j = (ids[k] for k in "ABCDEFGHIJ")
    outcomes = _through(a, "sync") + [
        Outcome.failure(a, "download", "ace",
                        "ACE scrape failed: 'Settings' object has no attribute 'x'"),
        # Every source refused: the article never gets past download.
        Outcome.failure(b, "download", "pdf", "No open-access PDF URL found."),
        Outcome.failure(b, "download", "elsevier", "publisher does not allow full-text download",
                        permanent=True),
        _ok(c, "download", "ace"),
        Outcome.failure(c, "extract", "ace", "downloaded files missing on disk: 1"),
        # Read, but neither the tables nor the prose hold an analysis.
        *_through(d, "passages"), _ok(d, "prose", coordinates=0),
        _ok(d, "triage", source="pubget", passed=0),
        _ok(e, "download", "ace"), Outcome.failure(e, "extract", "ace", NO_TEXT),
        Outcome.failure(e, "passages", "", NO_TEXT),
        *_through(f, "space"),
        Outcome.failure(f, "upload", "", f"{HELD_FOR_REVIEW}: in 2 studysets"),
        *_through(g, "passages"),
        # Space never ran for h, but an upload from before is still there.
        *_through(h, "roles"), _ok(h, "upload"),
        # j: triage named a table the extraction no longer has.
        _ok(j, "download", "ace"), _ok(j, "extract", "ace"), _ok(j, "metadata"),
        _ok(j, "triage", source="ace", passed=1),
        Outcome(article_id=j, stage="analyses", status=Status.SKIPPED,
                summary={"tables": 0, "reason": "no coordinate tables"}),
        Outcome.failure(j, "passages", "", "boom </script><b>raw</b>"),
    ]
    cat.record(outcomes)
    # An attempt the catalog kept no outcome for.
    cat._conn.execute("INSERT INTO attempts(article_id, stage, source, attempted_at, ok) "
                      "VALUES (?, 'prose', '', '2026-10-11', 0)", (g,))
    cat.close()
    return tmp_path / "catalog", ids


@pytest.fixture()
def funnel(catalog):
    root, ids = catalog
    conn = open_readonly(root)
    try:
        yield build_funnel(conn, samples=50), ids
    finally:
        conn.close()


def _where(funnel, article_id, stage):
    """(state, reason, downloaded, judged, raw) of one article at one stage."""
    found = [(key[1], key[2], key[3], key[4], raw)
             for key, (_, sample) in funnel.cells.items() if key[0] == stage
             for aid, raw in sample if aid == article_id]
    assert len(found) == 1, found
    return found[0]


def test_every_stage_accounts_for_every_article(funnel):
    funnel, ids = funnel
    totals = funnel.stage_totals()
    assert funnel.total == len(ids)
    assert all(sum(counts.values()) == len(ids) for counts in totals.values())
    # The one attempt with no outcome is the only thing that does not reconcile.
    assert funnel.reconcile() == ["prose: 1 unaccounted"]
    assert totals["prose"][UNACCOUNTED] == 1


def test_failures_carry_the_recorded_reason(funnel):
    funnel, ids = funnel
    assert _where(funnel, ids["B"], "download")[:2] == (FAILED, "not open access")
    assert "publisher does not allow" in _where(funnel, ids["B"], "download")[4]
    assert _where(funnel, ids["C"], "extract")[:2] == (FAILED, "file or payload missing")
    assert _where(funnel, ids["E"], "extract")[:2] == (TAKEN_BACK, NO_TEXT)
    assert _where(funnel, ids["E"], "passages")[:2] == (BLOCKED, f"extract taken_back: {NO_TEXT}")
    assert _where(funnel, ids["F"], "upload")[:2] == (HELD, HELD_FOR_REVIEW)
    assert _where(funnel, ids["J"], "analyses")[:2] == (FILTERED, "no coordinate tables")
    assert _where(funnel, ids["D"], "triage")[:2] == (FILTERED, "triage passed no table")
    assert _where(funnel, ids["D"], "analyses")[:2] == (
        BLOCKED, "triage filtered: triage passed no table")
    for (stage, state, reason, *_), (count, _) in funnel.cells.items():
        if state in (FAILED, PERMANENT, BLOCKED):
            assert reason and reason != "no reason recorded", (stage, state)


def test_blocked_articles_name_where_they_dropped_out(funnel):
    funnel, ids = funnel
    assert _where(funnel, ids["B"], "upload")[:2] == (BLOCKED, "download failed: not open access")
    assert _where(funnel, ids["C"], "sync")[:2] == (
        BLOCKED, "extract failed: file or payload missing")
    assert _where(funnel, ids["D"], "resolve")[:2] == (
        FILTERED, "neither the tables nor the prose hold an analysis")
    assert _where(funnel, ids["D"], "roles")[0] == BLOCKED


def test_missing_states_are_told_apart(funnel):
    funnel, ids = funnel
    assert _where(funnel, ids["I"], "download")[0] == PENDING
    assert _where(funnel, ids["H"], "space")[0] == PENDING
    assert _where(funnel, ids["H"], "upload")[:2] == (STALE, "input space is pending")
    assert _where(funnel, ids["A"], "notices")[0] == NOT_ATTEMPTED
    assert _where(funnel, ids["A"], "sync")[0] == PASSED


def test_sources_are_attributed(funnel):
    funnel, ids = funnel
    # A's ace download failed: only pubget counts as a source it was downloaded from.
    assert _where(funnel, ids["A"], "upload")[2:4] == ("pubget", "pubget")
    assert _where(funnel, ids["J"], "triage")[2:4] == ("ace", "ace")
    assert _where(funnel, ids["B"], "download")[2:4] == ("none", "none")
    keys = {key[:4] for key in funnel.sources}
    assert ("download", "pdf", "failed", "not open access") in keys
    assert ("download", "elsevier", "permanent", "not open access") in keys
    assert ("download", "ace", "failed", "code error") in keys


def test_categories_keep_unknown_text_bounded():
    assert categorize("PDF fetch failed: 403 Client Error: Forbidden for url: https://x") == \
        "HTTP 403 forbidden"
    assert categorize("ACE scrape produced invalid HTML: API rate limit error.") == "rate limited"
    assert categorize("Elsevier did not return a payload for the requested article (pmid: 1).") \
        == "source returned no article"
    assert categorize("weird 12345 thing at https://a.b/c") == "other: weird N thing at <url>"


def test_html_embeds_the_data(funnel):
    funnel, ids = funnel
    page = render_html(funnel, "/cat")
    found = re.search(r'<script id="data" type="application/json">(.*?)</script>', page, re.S)
    data = json.loads(found.group(1))
    assert data["total"] == len(ids)
    assert data["problems"] == ["prose: 1 unaccounted"]
    assert set(data["strings"]["id"]) == set(ids.values())
    assert "boom </script><b>raw</b>" in data["strings"]["raw"]
    stages = data["strings"]["stage"]
    assert sum(c[6] for c in data["cells"] if stages[c[0]] == "upload") == len(ids)


def test_cli_writes_the_page_and_lists_a_bucket(catalog, tmp_path):
    root, ids = catalog
    out = tmp_path / "funnel.html"
    runner = CliRunner()
    result = runner.invoke(cli.app, ["report", "funnel", "--catalog", str(root),
                                     "--out", str(out)])
    assert result.exit_code == 0, result.output
    assert "10 articles" in result.output and "prose: 1 unaccounted" in result.output
    assert out.read_text().count('<script id="data"') == 1
    result = runner.invoke(cli.app, ["report", "funnel-ids", "--catalog", str(root),
                                     "--stage", "upload", "--state", "blocked",
                                     "--reason", "open access"])
    assert result.exit_code == 0, result.output
    assert result.output.split()[0] == ids["B"] and len(result.output.splitlines()) == 1
