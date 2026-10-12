"""The funnel report: every article placed once per stage, with its reason and source."""

from __future__ import annotations

import json
import re
import shlex
import shutil
import sqlite3
import subprocess
import threading
import urllib.request

import ingestion_workflow.cli.main as cli
import pytest
from ingestion_workflow.catalog import Catalog, Outcome, Status
from ingestion_workflow.catalog.blobs import BlobStore
from ingestion_workflow.config import Settings
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.pipeline.stage import HELD_FOR_REVIEW, NO_TEXT
from ingestion_workflow.pipeline.stages.extract import READ_FROM_BAD_PAGE
from ingestion_workflow.services.funnel_report import (
    _PAGE,
    BLOCKED,
    FAILED,
    FILTERED,
    HELD,
    NOT_ATTEMPTED,
    PASSED,
    PENDING,
    PERMANENT,
    REFUSED,
    STALE,
    STATES,
    TAKEN_BACK,
    UNACCOUNTED,
    build_funnel,
    categorize,
    filter_funnel,
    flow,
    open_readonly,
    page_model,
    reason_group,
    render_html,
    serve,
    trace_article,
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


def _bad(article, stage, source, reason):
    """A download or extraction failed as a page that is not the article."""
    return Outcome.failure(article, stage, source, f"bad page ({reason}): detail",
                           summary={"bad_page": reason})


def _read_from_ace(article, *, retracted=False):
    """Read through to sync from ace's text, then ace's page found not to be the article
    while pubget's download and extraction stay ok."""
    return [
        _ok(article, "download", "pubget"), _ok(article, "extract", "pubget"),
        _bad(article, "download", "ace", "paywall"), _bad(article, "extract", "ace", "paywall"),
        _ok(article, "metadata"), _ok(article, "triage", source="ace", passed=1, tables=1),
        _ok(article, "analyses", tables=1),
        _ok(article, "passages", source="ace", passages=2),
        _ok(article, "prose", source="ace", coordinates=3),
        _ok(article, "resolve", tables=1), _ok(article, "roles", tables=1),
        _ok(article, "space", tables=1),
        _ok(article, "upload", base_study_id="b", **({"retracted": True} if retracted else {})),
        _ok(article, "sync", base_study_id="b"),
    ]


@pytest.fixture()
def wider(tmp_path):
    """Articles for the branches the first fixture leaves out."""
    cat = Catalog.open(tmp_path / "wider")
    names = "KLMNOPQRSTVWYZXUHJ"
    ids = {name: cat.register(Identifier(pmid=str(2000 + i))).id for i, name in enumerate(names)}
    k, l_, m, n, o, p, q, r, s, t, v, w, y, z, x, u, h, j = (ids[c] for c in names)
    upto_prose = [_ok(a, "download", "pubget") for a in (l_, m, n, o)] + [
        _ok(a, "extract", "pubget") for a in (l_, m, n, o)] + [
        _ok(a, "passages", source="pubget", passages=2) for a in (l_, m)]
    cat.record([
        # Metadata found nothing, and triage still read the article.
        _ok(k, "download", "pubget"), _ok(k, "extract", "pubget"),
        Outcome.failure(k, "metadata", "", "No provider returned metadata"),
        _ok(k, "triage", source="pubget", passed=1, tables=1),
        *upto_prose,
        # Resolve has the prose's coordinates and no table to read.
        _ok(l_, "prose", source="pubget", coordinates=4),
        # Resolve has the tables and nothing from the prose.
        _ok(m, "prose", source="pubget", coordinates=0),
        _ok(m, "analyses", tables=2),
        # No passage: prose is free, and n's tables wait behind it.
        _ok(n, "passages", source="pubget", passages=0), _ok(n, "analyses", tables=3),
        _ok(o, "passages", source="pubget", passages=0),
        _ok(o, "metadata"), _ok(o, "triage", source="pubget", passed=0, tables=0),
        # Triage passed nothing; analyses records from before it gated on that.
        *_through(p, "metadata"), _ok(p, "triage", source="pubget", passed=0, tables=4),
        Outcome(article_id=p, stage="analyses", status=Status.SKIPPED,
                summary={"reason": "triage passed no table"}),
        *_through(q, "metadata"), _ok(q, "triage", source="pubget", passed=0, tables=2),
        _ok(q, "analyses", tables=3),
        *_through(r, "space"), _ok(r, "upload", base_study_id="b", retracted=True),
        *_read_from_ace(s),
        _bad(t, "download", "ace", "error_page"),
        *_read_from_ace(v, retracted=True),
        _ok(w, "download", "pubget"), _ok(w, "extract", "pubget"),
        _ok(y, "download", "ace"), Outcome.failure(y, "extract", "ace", NO_TEXT),
        _ok(y, "triage", source="ace", passed=0, tables=0),
        Outcome.failure(z, "download", "elsevier", "publisher does not allow full-text download"),
        Outcome.failure(x, "download", "elsevier", "publisher does not allow full-text download"),
        Outcome.failure(x, "download", "pdf", "PDF fetch failed: 403 Client Error: Forbidden"),
        *_through(u, "roles"), Outcome.failure(u, "upload", "", "boom"),
        *_through(h, "roles"), _ok(h, "upload", base_study_id="b", retracted=True),
        *_through(j, "metadata"), _ok(j, "triage", source="pubget", passed=1, tables=1),
        _ok(j, "analyses", tables=0),
    ])
    # w was merged into k, though its artifacts stayed behind.
    cat._conn.execute("UPDATE articles SET merged_into=? WHERE id=?", (k, w))
    cat._conn.execute("UPDATE articles SET created_at='2026-01-02T00:00:00' WHERE id IN (?, ?)",
                      (z, x))
    cat.close()
    return tmp_path / "wider", ids


@pytest.fixture()
def wide(wider):
    root, ids = wider
    conn = open_readonly(root)
    try:
        yield build_funnel(conn, samples=50), ids
    finally:
        conn.close()


def _ids(root, *args):
    result = CliRunner().invoke(cli.app, ["report", "funnel-ids", "--catalog", str(root), *args])
    assert result.exit_code == 0, result.output
    return {line.split("\t")[0] for line in result.output.splitlines()}


def test_triage_enters_on_a_failed_metadata(wide):
    funnel, ids = wide
    assert _where(funnel, ids["K"], "metadata")[0] == FAILED
    assert _where(funnel, ids["K"], "triage")[:2] == (PASSED, "")


def test_resolve_reads_both_the_prose_and_the_tables(wide):
    funnel, ids = wide
    assert _where(funnel, ids["L"], "resolve")[:2] == (PENDING, "input ready, not run yet")
    assert _where(funnel, ids["M"], "resolve")[:2] == (PENDING, "input ready, not run yet")


def test_pending_says_when_the_input_is_empty(wide):
    funnel, ids = wide
    waiting = "input ready, nothing to read (passages = 0); tables wait behind it"
    assert _where(funnel, ids["N"], "prose")[:2] == (PENDING, waiting)
    assert _where(funnel, ids["N"], "resolve")[:2] == (BLOCKED, f"prose pending: {waiting}")
    assert _where(funnel, ids["O"], "prose")[:2] == (
        PENDING, "input ready, nothing to read (passages = 0)")


def test_a_leftover_record_keeps_its_own_status_and_reason(wide):
    funnel, ids = wide
    assert _where(funnel, ids["P"], "analyses")[:2] == (
        BLOCKED,
        "triage filtered: triage passed no table; leftover skipped: triage passed no table")
    # Downstream of a leftover, the reason is where the article dropped out.
    assert _where(funnel, ids["U"], "upload")[:2] == (
        BLOCKED, "space pending: input ready, not run yet; leftover failed: other: boom")
    assert _where(funnel, ids["U"], "sync")[:2] == (
        BLOCKED, "space pending: input ready, not run yet")
    # An ok record that holds tables is stale: something downstream could read it.
    assert _where(funnel, ids["Q"], "analyses")[:2] == (STALE, "input triage is filtered")
    assert _where(funnel, ids["H"], "upload")[:2] == (STALE, "input space is pending; retracted")
    # Resolve reads the tables, so analyses without any holds nothing for it.
    assert _where(funnel, ids["J"], "analyses")[:2] == (FILTERED, "its summary has tables = 0")
    assert _where(funnel, ids["O"], "triage")[:2] == (FILTERED, "the extraction has no table")
    # The record behind a leftover is still counted as a record.
    assert funnel.reconcile() == ["2 artifacts belong to no live article"]


def test_reconcile_checks_every_record_was_placed(wide):
    funnel, _ = wide
    funnel.recorded["analyses"] -= 1
    assert "analyses: 6 articles placed from a record, but 7 have one" in funnel.reconcile()


def test_a_merged_article_with_artifacts_is_stranded(wide):
    funnel, ids = wide
    assert funnel.stranded == 2
    assert funnel.total == len(ids) - 1
    assert not any(aid == ids["W"] for _, (_, sample) in funnel.cells.items()
                   for aid, _ in sample)


def test_retracted_and_taken_back_keep_their_reason(wide):
    funnel, ids = wide
    assert _where(funnel, ids["R"], "upload")[:2] == (PASSED, "retracted")
    keys = {key[:3] for key in funnel.sources}
    assert ("extract", "ace", TAKEN_BACK) in keys


def test_bad_pages_are_their_own_reason(wide):
    funnel, ids = wide
    assert _where(funnel, ids["T"], "download")[:2] == (FAILED, "bad page: error_page")
    assert _where(funnel, ids["T"], "extract")[:2] == (
        BLOCKED, "download failed: bad page: error_page")
    keys = {key[:4] for key in funnel.sources}
    assert ("download", "ace", "failed", "bad page: paywall") in keys
    assert ("extract", "ace", "failed", "bad page: paywall") in keys
    # pubget's text stands in at extract, but prose read ace's passages.
    assert _where(funnel, ids["S"], "extract")[0] == PASSED
    assert _where(funnel, ids["S"], "prose")[:2] == (REFUSED, f"passages {READ_FROM_BAD_PAGE}")
    # The resolve made before stays, and is stale behind it.
    assert _where(funnel, ids["S"], "resolve")[:2] == (STALE, "input prose is refused")


def test_sync_refuses_prose_from_a_bad_page_unless_retracted(wider):
    root, ids = wider
    conn = open_readonly(root)
    try:
        funnel = build_funnel(conn, prose=False, samples=50)
    finally:
        conn.close()
    assert _where(funnel, ids["S"], "upload")[0] == PASSED
    assert _where(funnel, ids["S"], "sync")[:2] == (REFUSED, f"prose {READ_FROM_BAD_PAGE}")
    assert _where(funnel, ids["V"], "sync")[0] == PASSED


def test_dimensions_include_tried_sources_and_registration_day(wide):
    funnel, ids = wide
    cells = {(key[0], aid): key for key, (_, sample) in funnel.cells.items()
             for aid, _ in sample}
    assert cells[("download", ids["T"])][3] == "none"
    assert cells[("download", ids["T"])][6] == "ace"
    assert cells[("download", ids["S"])][6] == "ace+pubget"
    assert cells[("download", ids["Z"])][5] == "2026-01-02"
    assert cells[("download", ids["K"])][5] != "2026-01-02"


def test_funnel_ids_match_the_page(wider, wide):
    root, ids = wider
    funnel, _ = wide
    # The source table's "dl failed" cell for ace, and the command the page prints for it.
    page = {aid for key, (_, sample) in funnel.sources.items()
            if key[:3] == ("download", "ace", "failed") for aid, _ in sample}
    assert page == {ids["S"], ids["T"], ids["V"]}
    assert _ids(root, "--stage", "download", "--by-source", "ace", "--state", "failed") == page
    # Downloaded from means an ok download; tried means any download recorded.
    assert _ids(root, "--stage", "download", "--source", "ace") == {ids["Y"]}
    assert _ids(root, "--stage", "download", "--tried", "ace") == {
        ids["S"], ids["T"], ids["V"], ids["Y"]}
    assert _ids(root, "--stage", "download", "--tried", "ace", "--tried", "elsevier",
                "--state", "failed") == {ids["T"], ids["Z"], ids["X"]}
    # A clicked reason matches exactly; the reason box matches a substring.
    assert _ids(root, "--stage", "download", "--exact-reason", "not open access") == {ids["Z"]}
    assert _ids(root, "--stage", "download", "--reason", "open access") == {
        ids["Z"], ids["X"]}
    assert _ids(root, "--stage", "download", "--state", "failed", "--from", "2026-01-01",
                "--to", "2026-01-03") == {ids["Z"], ids["X"]}
    assert _ids(root, "--stage", "download", "--judged", "ace") == {
        ids["S"], ids["V"], ids["Y"]}
    # The stage graph decides what sync waits on.
    assert _ids(root, "--stage", "sync", "--state", "refused", "--no-prose") == {ids["S"]}
    assert _ids(root, "--stage", "sync", "--state", "stale", "--prose") == {ids["S"], ids["V"]}
    assert _ids(root, "--stage", "download", "--state", "failed", "--from", "2026-01-03") == {
        ids["T"]}


def test_page_carries_the_tried_dimension_and_the_graph(wide):
    funnel, ids = wide
    page = render_html(funnel, "/cat")
    data = json.loads(re.search(r'<script id="data" type="application/json">(.*?)</script>',
                                page, re.S).group(1))
    assert data["prose"] is True
    tried = data["strings"]["tried"]
    assert {tried[c[8]] for c in data["cells"]} >= {"ace", "ace+pubget", "pubget"}
    assert {tried[c[9]] for c in data["sources"]} >= {"ace", "elsevier+pdf"}
    assert "--by-source" in page and "--exact-reason" in page


def test_flow_adds_up_at_every_step(wide):
    funnel, ids = wide
    steps = flow(funnel)
    assert [step["stage"] for step in steps] == [
        "download", "extract", "passages", "prose", "resolve", "roles", "space", "upload", "sync"]
    assert steps[0]["in"] == funnel.total
    for before, step in zip([None] + steps, steps):
        total = step["passed"] + sum(i[2] for i in step["excluded"] + step["waiting"])
        assert step["adds_up"] and step["in"] == total, step
        assert step["follows"] and (before is None or step["in"] == before["passed"]), step
    prose = steps[3]
    assert [REFUSED, f"passages {READ_FROM_BAD_PAGE}", 2] in prose["excluded"]
    assert [PENDING, "input ready, nothing to read (passages = 0); tables wait behind it", 1] \
        in prose["waiting"]
    assert [FAILED, "not open access", 1] in steps[0]["excluded"]


def test_flow_check_fails_when_a_count_is_off(wide):
    funnel, _ = wide
    key = next(k for k in funnel.cells if k[0] == "extract" and k[1] == PASSED)
    funnel.cells[key][0] += 1
    steps = {step["stage"]: step for step in flow(funnel)}
    assert not steps["extract"]["adds_up"] and not steps["passages"]["follows"]


@pytest.fixture()
def stored(tmp_path):
    """One article whose extraction has a payload in the blob store."""
    cat = Catalog.open(tmp_path / "stored")
    a = cat.register(Identifier(pmid="3001")).id
    cat.record([
        Outcome(article_id=a, stage="download", source="pubget", status=OK,
                payload={"files": []}, summary={"files": 0}),
        Outcome(article_id=a, stage="extract", source="pubget", status=OK, fingerprint="old",
                payload={"text": "t"}, summary={"has_text": True}),
        Outcome.failure(a, "metadata", "", "No provider returned metadata"),
    ])
    digest = cat.artifact(a, "extract", "pubget").blob
    cat.close()
    return tmp_path / "stored", a, digest


def test_trace_shows_every_record_without_opening_a_payload(stored, monkeypatch):
    root, a, digest = stored

    def no_payload(self, digest):
        raise AssertionError("a payload was read")

    monkeypatch.setattr(BlobStore, "get", no_payload)
    found = trace_article(root, a)
    stages = {s["stage"]: s for s in found["stages"]}
    assert stages["extract"]["state"] == PASSED and stages["metadata"]["state"] == FAILED
    (extract,) = stages["extract"]["records"]
    assert extract["path"].endswith(f"blobs/{digest[:2]}/{digest}.json.gz") and extract["on_disk"]
    assert extract["fingerprint"] == "old" and extract["attempts"] == 1 and extract["written"]
    assert stages["metadata"]["records"][0]["reason"] == "no metadata found"
    assert stages["extract"]["built_from"][0]["stage"] == "download"
    assert "--config" in stages["extract"]["plan"]
    assert trace_article(root, "nope")["error"]


def test_trace_plans_each_stage_to_say_whether_it_is_fresh(stored, tmp_path, monkeypatch):
    root, a, _ = stored
    monkeypatch.setattr(BlobStore, "get", lambda self, d: (_ for _ in ()).throw(
        AssertionError("a payload was read")))
    settings = Settings(catalog_root=str(root), cache_root=str(tmp_path / "cache"),
                        data_root=str(tmp_path / "data"))
    found = trace_article(root, a, settings=settings)
    stages = {s["stage"]: s for s in found["stages"]}
    (extract,) = stages["extract"]["records"]
    assert extract["freshness"].startswith("stale: built from fingerprint old")
    assert stages["metadata"]["plan"]
    # A stage that cannot plan under these settings says why, and the rest still plan.
    unknown = {s["stage"]: s["plan"] for s in found["stages"] if s["plan"].startswith("not known")}
    assert set(unknown) == {"triage"} and "coordinate_gate_path" in unknown["triage"]


def test_cli_trace_and_serve(stored, tmp_path):
    root, a, _ = stored
    result = CliRunner().invoke(cli.app, ["report", "trace", a, "--catalog", str(root)])
    assert result.exit_code == 0, result.output
    assert json.loads(result.output)["article_id"] == a
    server = serve(root, port=0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        base = f"http://127.0.0.1:{server.server_address[1]}"
        with urllib.request.urlopen(base + "/") as page:
            assert '<script id="data"' in page.read().decode()
        with urllib.request.urlopen(f"{base}/trace?id={a}") as found:
            assert json.loads(found.read())["stages"][0]["stage"] == "download"
    finally:
        server.shutdown()
        server.server_close()


def test_reasons_group_under_their_primary_part():
    assert reason_group("not open access + source returned no article") == "not open access"
    assert reason_group("download failed: HTTP 403 forbidden + not open access + source "
                        "returned no article") == "download failed: HTTP 403 forbidden"
    assert reason_group("bad page: paywall + not open access") == "bad page: paywall"
    assert reason_group("code error + rate limited") == "code error"
    assert reason_group("not open access + rate limited") == "rate limited"
    assert reason_group("triage filtered: triage passed no table; leftover skipped: x") == \
        "triage filtered: triage passed no table"
    waiting = "input ready, nothing to read (passages = 0); tables wait behind it"
    assert reason_group(waiting) == waiting


def test_page_model_flows_through_every_gating_stage(wide):
    funnel, ids = wide
    model = page_model(funnel)
    steps = {step["stage"]: step for step in model["flow"]}
    assert list(steps) == ["download", "extract", "passages", "metadata", "notices", "triage",
                           "analyses", "prose", "resolve", "roles", "space", "upload", "sync"]
    for step in model["flow"]:
        assert step["adds_up"] and step["follows"], step
        for group in step["excluded"] + step["waiting"] + step["stale"]:
            assert group[2] == sum(n for _, n in group[3])
    # Triage takes in what metadata attempted, failed included.
    tried = sum(model["totals"]["metadata"][s] for s in (PASSED, FILTERED, FAILED, PERMANENT))
    assert steps["triage"]["in"] == tried
    # Download's joined reasons sit under their primary part.
    groups = {g[1]: g for g in steps["download"]["excluded"]}
    assert groups["HTTP 403 forbidden"][3] == [
        ["HTTP 403 forbidden + not open access", 1]]
    assert groups["not open access"][3] == [["not open access", 1]]
    # Stale records are listed apart, with the state's reasons.
    assert ["stale", "input space is pending; retracted", 1,
            [["input space is pending; retracted", 1]]] in steps["upload"]["stale"]
    # An extraction with no text hash waits for refresh-text before passages.
    assert any(g[1].startswith("waits for `ingest refresh-text`")
               for g in steps["passages"]["waiting"])


def test_funnel_ids_lists_a_reason_group(wider):
    root, ids = wider
    assert _ids(root, "--stage", "download", "--reason-group", "HTTP 403 forbidden") == {ids["X"]}
    assert _ids(root, "--stage", "extract", "--reason-group",
                "download failed: HTTP 403 forbidden") == {ids["X"]}


def _page_js(page: str, harness: str) -> dict:
    script = re.findall(r"<script>(.*?)</script>", page, re.S)[-1]
    data = re.search(r'<script id="data" type="application/json">(.*?)</script>', page,
                     re.S).group(1)
    run = subprocess.run(["node", "-e", f"const FUNNEL_DATA={data};\n{script}\n{harness}"],
                         capture_output=True, text=True, timeout=60)
    assert run.returncode == 0, run.stderr
    return json.loads(run.stdout)


@pytest.mark.skipif(shutil.which("node") is None, reason="needs node")
def test_page_recount_equals_python_and_the_cli(wider, wide):
    root, ids = wider
    funnel, _ = wide
    page = render_html(funnel, str(root))
    out = _page_js(page, """
        const out={unfiltered:model(D.cells),embedded:D.model};
        F.tried.add("ace");F.tried.add("elsevier");F.from="2026-01-03";
        out.filtered=model(cells());
        out.box=cliCmd({stage:"download",states:ENTERED});
        out.group=cliCmd({stage:"download",state:"failed",group:"bad page: error_page"});
        out.reason=cliCmd({stage:"download",state:"failed",reason:"bad page: error_page"});
        F.from="";const o={stage:"download",state:"failed",group:"HTTP 403 forbidden"};
        out.group2=cliCmd(o);out.drill=bucket(o);
        console.log(JSON.stringify(out));""")
    as_json = lambda value: json.loads(json.dumps(value))  # noqa: E731
    assert out["embedded"] == as_json(page_model(funnel))
    assert out["unfiltered"] == out["embedded"]
    chosen = filter_funnel(funnel, tried=["ace", "elsevier"], day_from="2026-01-03")
    assert out["filtered"] == as_json(page_model(chosen))

    def listed(cmd):
        args = shlex.split(cmd)
        assert args[:3] == ["ingest", "report", "funnel-ids"], cmd
        return _ids(root, *args[3:])

    entered = {aid for key, (_, sample) in chosen.cells.items()
               if key[0] == "download" and key[1] not in (BLOCKED, STALE) for aid, _ in sample}
    assert entered == {ids["S"], ids["T"], ids["V"], ids["Y"]}
    assert listed(out["box"]) == entered
    assert listed(out["group"]) == listed(out["reason"]) == {ids["T"]}
    # The drill-down and its command pick one group out of several failures.
    assert listed(out["group2"]) == {ids["X"]}
    assert out["drill"]["total"] == 1 and {r[0] for r in out["drill"]["rows"]} == {ids["X"]}


def test_state_labels_are_readable():
    found = re.search(r"const INK=\{(.*?)\};", _PAGE).group(1)
    ink = dict(re.findall(r'(\w+):"(#[0-9a-f]{6})"', found))

    def luminance(hex_colour):
        rgb = [int(hex_colour[i:i + 2], 16) / 255 for i in (1, 3, 5)]
        lin = [c / 12.92 if c <= 0.03928 else ((c + 0.055) / 1.055) ** 2.4 for c in rgb]
        return 0.2126 * lin[0] + 0.7152 * lin[1] + 0.0722 * lin[2]

    panel = luminance("#f6f7f9")
    assert set(STATES) <= set(ink)
    for state, colour in ink.items():
        assert (panel + 0.05) / (luminance(colour) + 0.05) >= 4.5, (state, colour)


@pytest.fixture()
def read_text(tmp_path):
    """Two articles with passages, whose plans open the extraction's payload."""
    from ingestion_workflow.pipeline.stages.passages import PassagesStage

    cat = Catalog.open(tmp_path / "text")
    a, b = (cat.register(Identifier(pmid=p)).id for p in ("4001", "4002"))
    cat.record([
        Outcome(article_id=a, stage="download", source="pubget", status=OK, summary={}),
        Outcome(article_id=a, stage="extract", source="pubget", status=OK, fingerprint="e",
                payload={"full_text_path": "/nowhere/text.txt"},
                summary={"has_text": True, "text_sha256": "abc"}),
        Outcome(article_id=b, stage="download", source="pubget", status=OK, summary={}),
        Outcome(article_id=b, stage="extract", source="pubget", status=OK, fingerprint="e",
                payload={"full_text_path": "/nowhere/text.txt"}, summary={"has_text": True}),
    ])
    extraction = cat.artifact(a, "extract", "pubget")
    cat.record([
        Outcome(article_id=a, stage="passages", source="", status=OK,
                fingerprint=PassagesStage.fingerprint_for(extraction, "abc"),
                summary={"source": "pubget", "passages": 1}),
        Outcome(article_id=b, stage="passages", source="", status=OK, fingerprint="p",
                summary={"source": "pubget", "passages": 1}),
    ])
    cat.close()
    return tmp_path / "text", a, b


def test_trace_never_reads_a_payload_and_says_so_plainly(read_text, tmp_path):
    root, a, b = read_text
    settings = Settings(catalog_root=str(root), cache_root=str(tmp_path / "cache"),
                        data_root=str(tmp_path / "data"), prose_model="m")
    plans = {}
    for article in (a, b):
        found = trace_article(root, article, settings=settings)
        plans[article] = {s["stage"]: s["plan"] for s in found["stages"]}
        for stage, plan in plans[article].items():
            assert not re.search(r"[A-Z][a-z]+Error|Traceback", plan), (stage, plan)
    # The plan wants the text file's path from the payload; the rows tell freshness.
    assert plans[a]["passages"].startswith("fresh: its fingerprint matches the extraction")
    assert plans[b]["passages"].startswith("waits for `ingest refresh-text`")


def test_trace_marks_a_missing_blob_and_side_inputs(stored):
    root, a, digest = stored
    (root / "blobs" / digest[:2] / f"{digest}.json.gz").unlink()
    found = trace_article(root, a)
    stages = {s["stage"]: s for s in found["stages"]}
    assert stages["extract"]["records"][0]["on_disk"] is False
    # Triage's fingerprint reads the extraction as well as its parent, metadata.
    assert [u["stage"] for u in stages["triage"]["built_from"]] == ["metadata", "extract"]


def test_trace_flags_an_input_rewritten_after_the_record(stored):
    root, a, _ = stored
    conn = sqlite3.connect(root / "catalog.sqlite")
    with conn:
        conn.execute("UPDATE artifacts SET updated_at='2099-01-01T00:00:00+00:00' "
                     "WHERE article_id=? AND stage='download'", (a,))
    conn.close()
    stages = {s["stage"]: s for s in trace_article(root, a)["stages"]}
    assert stages["extract"]["built_from"][0]["newer"] is True
    assert stages["metadata"]["built_from"][0]["newer"] is False


def test_a_stage_that_cannot_plan_names_the_setting(stored, tmp_path):
    root, a, _ = stored
    settings = Settings(catalog_root=str(root), cache_root=str(tmp_path / "cache"),
                        data_root=str(tmp_path / "data"))
    stages = {s["stage"]: s for s in trace_article(root, a, settings=settings)["stages"]}
    assert stages["triage"]["plan"] == \
        "not known: this stage needs coordinate_gate_path in the config to plan"
