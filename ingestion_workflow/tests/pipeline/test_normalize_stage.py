"""`normalize` in-process: each article's evidence record assigned against a saved fit.

The paper is article 26klegjq7mhi with its 27B record and ev1 evidence. The library
(`ingestion_workflow.normalization`, tested on its own) is stood in for at
`normalize_assign`'s calls into it, `describe`, `assign` and pondie's `apply_normalized`:
what is checked here is what the stage reads, which run it records, and when it runs again.
"""

from __future__ import annotations

import copy
import json
import shutil
from datetime import timedelta
from pathlib import Path

import pytest
from ingestion_workflow.catalog import Catalog, Outcome, Run, RunKind, Status
from ingestion_workflow.config import Settings
from ingestion_workflow.pipeline import Context
from ingestion_workflow.pipeline.stages.offloaded import (
    AWAITS_IMPORT,
    NO_NORMALIZE_INPUT,
    NormalizeStage,
    record_digest,
)
from ingestion_workflow.services import normalize_assign
from ingestion_workflow.services.offload.importer import import_run

DATA = Path(__file__).resolve().parents[1] / "data" / "offload"
ARTICLE, BASE = "26klegjq7mhi", "tS8tSUXeybTo"
PARSE = json.loads((DATA / "paper-26klegjq7mhi/parse/coordinate_parse.json").read_text())
RECORD = json.loads((DATA / "26klegjq7mhi.extraction.json").read_text())
EVIDENCED = json.loads((DATA / "26klegjq7mhi.ev1.extraction.json").read_text())
ADAPTER = "jdkent/qwen3.8-27b-ns-record@dbfd846bd3e1259039eb2f3117dc12bd733c640a"


def _run(stage, **kwargs) -> Run:
    held = dict(version=f"v-{stage}", model="m", adapter=ADAPTER, prompt_version="p",
                schema_version="s", created_at="2026-10-31T13:43:00+00:00")
    run = Run("", RunKind.MODEL, stage, "ns-27b", **{**held, **kwargs})
    return Run(**{**run.__dict__, "run_id": run.minted_id})


class Library:
    """The two calls into `ingestion_workflow.normalization`, and what they were given."""

    def __init__(self, fit_id="fit-2d313fcc75a9"):
        self.fit_id = fit_id
        self.calls = []

    def describe(self, fit_dir):
        return {"fit_id": self.fit_id, "vocabulary": {"mondo": "2026-09-01"},
                "version": "assign-0123456789ab",
                "model": "taskcat + sentence-transformers/all-MiniLM-L6-v2@c9745ed",
                "assigner": {"assign_code_sha256": "a" * 64}}

    def apply_normalized(self, record):
        for group in record.get("groups") or []:
            group.setdefault("age_unit", {}).setdefault("normalized", [{"term": "years"}])

    def assign(self, record, text, bibliography, fit_dir, article_id):
        self.calls.append({"article_id": article_id, "text": text, "bibliography": bibliography,
                           "fit_dir": fit_dir, "record": copy.deepcopy(record)})
        record.setdefault("study", {})
        return record


@pytest.fixture()
def library(monkeypatch):
    held = Library()
    monkeypatch.setattr(normalize_assign, "describe", held.describe)
    monkeypatch.setattr(normalize_assign, "assign", held.assign)
    monkeypatch.setattr(normalize_assign, "apply_normalized", held.apply_normalized)
    return held


@pytest.fixture()
def ctx(tmp_path):
    settings = Settings(data_root=tmp_path / "data", cache_root=tmp_path / "cache",
                        catalog_root=tmp_path / "cat", ns_pond_root=tmp_path / "pond",
                        normalize_fit_dir=tmp_path / "fits" / "fit-2d313fcc75a9")
    paper = tmp_path / "pond" / BASE
    shutil.copytree(DATA / "paper-26klegjq7mhi", paper)
    with Catalog.open(settings.catalog_root) as catalog:
        catalog._conn.execute("INSERT INTO articles(id, created_at) VALUES(?, '2026-01-01')",
                              (ARTICLE,))
        catalog.record([Outcome(ARTICLE, "sync", "", Status.OK, "sync-fp",
                                summary={"path": str(paper), "parse_id": PARSE["parse_id"],
                                         "analyses": 1, "base_study_id": BASE})])
        yield Context(settings, catalog, retry_after=timedelta(hours=24))


def _recorded(ctx, evidence="ok"):
    import_run(ctx.catalog, _run("record"), [{"article_id": ARTICLE, "status": "ok",
                                              "text_sha256": PARSE["text_sha256"],
                                              "record": RECORD}])
    digest = ctx.catalog.artifact(ARTICLE, "record", "ns-27b").summary["record_digest"]
    row = ({"status": "ok", "record_digest": digest, "record": EVIDENCED} if evidence == "ok"
           else {"status": "skipped", "reason": "over the input cap", "record": None})
    import_run(ctx.catalog, _run("evidence", model="numind/NuExtract3"),
               [{"article_id": ARTICLE, "text_sha256": PARSE["text_sha256"], **row}])


def _renormalized(library, record):
    """`record` as the stage hands it to the fit: its old normalizations made again."""
    record = copy.deepcopy(record)
    normalize_assign.drop_siblings(record)
    library.apply_normalized(record)
    return record


def _plan(ctx, stage):
    return stage.plan(ctx, [ctx.catalog.ref(ARTICLE)],
                      ctx.catalog.artifacts([ARTICLE], stage.name),
                      ctx.catalog.artifacts([ARTICLE], stage.requires))


def _normalize(ctx, stage=None):
    stage = stage or NormalizeStage(ctx.settings)
    plan = _plan(ctx, stage)
    outcomes = list(stage.execute(ctx, plan.pending))
    ctx.catalog.record(outcomes)
    return plan, outcomes


def test_without_a_fit_normalize_only_holds_imports(ctx, library):
    _recorded(ctx)
    plan = _plan(ctx, NormalizeStage(ctx.settings.model_copy(update={"normalize_fit_dir": None})))
    assert plan.pending == [] and plan.reasons == {AWAITS_IMPORT: 1}


def test_the_evidence_record_is_assigned_with_the_parse_s_text_under_the_named_fit(ctx, library):
    _recorded(ctx)
    plan, (outcome,) = _normalize(ctx)
    (call,) = library.calls
    evidence = ctx.catalog.artifact(ARTICLE, "evidence", "ns-27b")
    assert plan.pending[0].upstream.stage == "evidence"
    assert call["record"] == _renormalized(library, ctx.catalog.payload(evidence))
    assert call["text"] == (DATA / "paper-26klegjq7mhi/processed/ace/text.txt").read_bytes() \
        .decode("utf-8")
    assert call["article_id"] == ARTICLE and call["bibliography"] is None
    assert outcome.status is Status.OK
    kept = ctx.catalog.artifact(ARTICLE, "normalize", "ns-27b")
    assert kept.summary["read"] == "evidence" and kept.summary["fit"] == "fit-2d313fcc75a9"
    assert kept.summary["record_digest"] == evidence.summary["record_digest"]
    run = ctx.catalog.run(kept.run_id)
    assert (run.stage, run.fit, run.version, run.pipeline) == (
        "normalize", "fit-2d313fcc75a9", "assign-0123456789ab", "ns-pond normalize")
    assert run.run_id == run.minted_id
    assert _plan(ctx, NormalizeStage(ctx.settings)).fresh == 1


def test_a_skipped_evidence_run_s_article_is_normalized_from_its_record(ctx, library):
    _recorded(ctx, evidence="skipped")
    _normalize(ctx)
    (call,) = library.calls
    assert record_digest(call["record"]) == record_digest(_renormalized(library, RECORD))
    assert ctx.catalog.artifact(ARTICLE, "normalize", "ns-27b").summary["read"] == "record"


def test_a_new_fit_makes_every_normalization_stale_and_is_a_new_run(ctx, library):
    _recorded(ctx)
    _normalize(ctx)
    first = ctx.catalog.artifact(ARTICLE, "normalize", "ns-27b").run_id
    library.fit_id = "fit-a12952d2b727"
    plan, _ = _normalize(ctx)
    assert len(plan.pending) == 1
    kept = ctx.catalog.artifact(ARTICLE, "normalize", "ns-27b")
    assert kept.run_id != first and ctx.catalog.run(kept.run_id).fit == "fit-a12952d2b727"


def test_a_rerun_under_the_same_fit_is_the_run_first_kept(ctx, library):
    _recorded(ctx)
    _normalize(ctx)
    kept = ctx.catalog.run(ctx.catalog.artifact(ARTICLE, "normalize", "ns-27b").run_id)
    ctx.catalog._conn.execute("UPDATE artifacts SET status='failed' WHERE stage='normalize'")
    ctx.catalog._conn.execute("DELETE FROM attempts WHERE stage='normalize'")
    _normalize(ctx)
    again = ctx.catalog.run(ctx.catalog.artifact(ARTICLE, "normalize", "ns-27b").run_id)
    assert again == kept and len(ctx.catalog.runs()) == 3


def test_stale_evidence_is_not_normalized(ctx, library):
    _recorded(ctx)
    newer = copy.deepcopy(RECORD)
    newer["analyses"][0]["name"]["value"] = "a later run's name"
    import_run(ctx.catalog, _run("record", prompt_version="p2"),
               [{"article_id": ARTICLE, "status": "ok", "text_sha256": PARSE["text_sha256"],
                 "record": newer}])
    plan = _plan(ctx, NormalizeStage(ctx.settings))
    assert plan.pending == [] and plan.reasons == {NO_NORMALIZE_INPUT: 1}


def test_an_assign_that_raises_is_this_article_s_failure(ctx, library, monkeypatch):
    _recorded(ctx)

    def broken(*args, **kwargs):
        raise RuntimeError("pondie's negation parser is missing")

    monkeypatch.setattr(normalize_assign, "assign", broken)
    _, (outcome,) = _normalize(ctx)
    assert outcome.status is Status.FAILED and "negation parser" in outcome.error
    assert outcome.run_id and ctx.catalog.run(outcome.run_id).fit == "fit-2d313fcc75a9"


def test_workers_answer_in_order_and_share_the_first_job_s_loaded_fit(library):
    jobs = [(k, None if k == "b" else ({"local_id": k}, "text", None, "/fits/f", k))
            for k in ("a", "b", "c", "d", "e")]
    out = list(normalize_assign.assign_all(jobs, 2, "no parse"))
    assert [k for k, _ in out] == ["a", "b", "c", "d", "e"]
    assert out[1][1] == (None, "no parse")
    assert [rec["local_id"] for _, (rec, err) in out if rec] == ["a", "c", "d", "e"]
    # the first job ran here, before the fork; the rest in the workers
    assert [c["article_id"] for c in library.calls] == ["a"]


def test_the_old_normalized_siblings_are_dropped_and_made_again_as_sidecars(ctx, library):
    _recorded(ctx)
    _normalize(ctx)
    (call,) = library.calls
    (group,) = call["record"]["groups"]
    assert "age_unit_normalized" not in group and group["age_unit"]["normalized"]
    assert "category_normalized" not in json.dumps(call["record"])
    (held,) = EVIDENCED["groups"]
    assert "age_unit_normalized" in held  # the 27B's records carry them; the fixture is one
    assert normalize_assign.drop_siblings(copy.deepcopy(EVIDENCED)) == 3
