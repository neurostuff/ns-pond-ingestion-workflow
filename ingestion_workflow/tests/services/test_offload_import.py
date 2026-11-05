"""`ingest offload import`: what it keeps, what it refuses, and when what it kept goes stale.

The record is a real one: article 26klegjq7mhi of the 27B's collected set (beast
`ns-collect/set-20261101`), with its parse's text hash and its one key.
"""

from __future__ import annotations

import copy
import json
from datetime import timedelta
from pathlib import Path

import pytest
from ingestion_workflow.catalog import Catalog, Outcome, Run, RunKind, Status
from ingestion_workflow.config import Settings
from ingestion_workflow.pipeline import Context
from ingestion_workflow.pipeline.stages.offloaded import (
    AWAITS_IMPORT,
    EvidenceStage,
    NormalizeStage,
    RecordStage,
    record_digest,
)
from ingestion_workflow.services.offload.importer import (
    RECORD_MOVED,
    SCHEMA_INVALID,
    import_run,
)

DATA = Path(__file__).resolve().parents[1] / "data" / "offload"
RECORD = json.loads((DATA / "26klegjq7mhi.extraction.json").read_text())
TEXT_SHA256 = "067a69b2021e98a832a2f32080848e5e58b7337dffed66dc3ac895cca3f5d680"
KEY = "analysis#840c7bec806dd3ad"
ARTICLE, BASE_STUDY = "26klegjq7mhi", "tS8tSUXeybTo"


def _run(stage="record", run_id="ns-27b-e-20261031", **kwargs) -> Run:
    base = dict(run_id=run_id, kind=RunKind.MODEL, stage=stage, source="ns-27b",
                model="Qwen/Qwen3.8-27B",
                adapter="jdkent/qwen3.8-27b-ns-record@dbfd846bd3e1259039eb2f3117dc12bd733c640a",
                created_at="2026-10-31T13:43:00+00:00")
    base.update(kwargs)
    return Run(**base)


def _write_parse(paper: Path, text_sha256=TEXT_SHA256, keys=(KEY,)) -> None:
    (paper / "parse").mkdir(parents=True, exist_ok=True)
    (paper / "parse" / "coordinate_parse.json").write_text(json.dumps(
        {"text_sha256": text_sha256, "analyses": [{"key": key} for key in keys]}))


@pytest.fixture()
def ctx(tmp_path):
    settings = Settings(data_root=tmp_path / "data", cache_root=tmp_path / "cache",
                        catalog_root=tmp_path / "cat", ns_pond_root=tmp_path / "pond")
    paper = tmp_path / "pond" / BASE_STUDY
    _write_parse(paper)
    with Catalog.open(settings.catalog_root) as catalog:
        catalog._conn.execute("INSERT INTO articles(id, created_at) VALUES(?, '2026-01-01')",
                              (ARTICLE,))
        catalog._conn.execute("INSERT INTO aliases(kind, value, article_id) VALUES(?, ?, ?)",
                              ("neurostore", BASE_STUDY, ARTICLE))
        catalog.record([Outcome(ARTICLE, "sync", "", Status.OK, "sync-fp",
                                summary={"path": str(paper), "parse_id": "p1",
                                         "base_study_id": BASE_STUDY})])
        yield Context(settings, catalog, retry_after=timedelta(hours=24))


def _row(**kwargs):
    row = {"base_study_id": BASE_STUDY, "status": "ok", "text_sha256": TEXT_SHA256,
           "record": copy.deepcopy(RECORD)}
    row.update(kwargs)
    return row


def _plan(ctx, stage):
    refs = [ctx.catalog.ref(ARTICLE)]
    return stage.plan(ctx, refs, ctx.catalog.artifacts([ARTICLE], stage.name),
                      ctx.catalog.artifacts([ARTICLE], stage.requires))


def test_a_record_is_kept_with_its_run_in_place_of_the_placeholder(ctx):
    assert RECORD["extraction_metadata"] == {"extractor_model": "vista-compact",
                                             "extractor_version": "0"}
    report = import_run(ctx.catalog, _run(), [_row()])
    assert report.recorded == {"ok": 1} and not report.refused
    kept = ctx.catalog.artifact(ARTICLE, "record", "ns-27b")
    assert kept.run_id == "ns-27b-e-20261031"
    assert kept.summary["parse_keys"] == [KEY]
    assert ctx.catalog.payload(kept)["extraction_metadata"] == {
        "extractor_model": "Qwen/Qwen3.8-27B", "extractor_version": "ns-27b-e-20261031",
        "source_text_hash": TEXT_SHA256, "extraction_date": "2026-10-31"}
    assert _plan(ctx, RecordStage(ctx.settings)).fresh == 1


def test_a_record_naming_a_key_the_parse_lacks_is_refused(ctx):
    record = copy.deepcopy(RECORD)
    record["coordinate_sets"][0]["local_id"] = "analysis#840c7bec806dd3a"   # a digit dropped
    report = import_run(ctx.catalog, _run(), [_row(record=record)])
    kept = ctx.catalog.artifact(ARTICLE, "record", "ns-27b")
    assert kept.status is Status.FAILED and kept.run_id == "ns-27b-e-20261031"
    assert "parse key(s) the parse does not have: analysis#840c7bec806dd3a" in kept.error
    assert sum(report.refused.values()) == 1
    assert _plan(ctx, RecordStage(ctx.settings)).reasons == {AWAITS_IMPORT: 1}


def test_a_record_of_another_text_is_refused(ctx):
    import_run(ctx.catalog, _run(), [_row(text_sha256="0" * 64)])
    kept = ctx.catalog.artifact(ARTICLE, "record", "ns-27b")
    assert kept.status is Status.FAILED and kept.error.startswith("text hash differs")


def test_a_record_study_schema_refuses_is_refused(ctx):
    record = copy.deepcopy(RECORD)
    record["analyses"][0]["invented"] = 1
    import_run(ctx.catalog, _run(), [_row(record=record)])
    assert ctx.catalog.artifact(ARTICLE, "record", "ns-27b").error.startswith(SCHEMA_INVALID)


def test_an_article_the_catalog_does_not_know_is_left_out(ctx):
    report = import_run(ctx.catalog, _run(), [_row(base_study_id="nOtKnOwN1234")])
    assert report.unknown == 1 and not report.recorded


def test_too_long_is_skipped_and_done(ctx):
    import_run(ctx.catalog, _run(), [{"article_id": ARTICLE, "status": "skipped",
                                      "reason": "no prediction: length", "record": None}])
    assert ctx.catalog.artifact(ARTICLE, "record", "ns-27b").status is Status.SKIPPED
    assert _plan(ctx, RecordStage(ctx.settings)).fresh == 1


def test_a_refusal_never_replaces_another_run_s_record(ctx):
    import_run(ctx.catalog, _run(run_id="ns-27b-e-20261028", prompt_version="v4"), [_row()])
    report = import_run(ctx.catalog, _run(), [_row(text_sha256="0" * 64)])
    assert report.kept == 1
    kept = ctx.catalog.artifact(ARTICLE, "record", "ns-27b")
    assert kept.ok and kept.run_id == "ns-27b-e-20261028"


def test_a_parse_that_keeps_the_keys_keeps_the_record_fresh(ctx, tmp_path):
    import_run(ctx.catalog, _run(), [_row()])
    _write_parse(tmp_path / "pond" / BASE_STUDY, keys=(KEY, "analysis#0000000000000000"))
    assert _plan(ctx, RecordStage(ctx.settings)).fresh == 1


def test_a_parse_that_drops_a_key_or_changes_its_text_makes_the_record_stale(ctx, tmp_path):
    import_run(ctx.catalog, _run(), [_row()])
    paper = tmp_path / "pond" / BASE_STUDY
    _write_parse(paper, keys=("analysis#0000000000000000",))
    assert _plan(ctx, RecordStage(ctx.settings)).fresh == 0
    _write_parse(paper, text_sha256="1" * 64)
    assert _plan(ctx, RecordStage(ctx.settings)).fresh == 0
    _write_parse(paper)
    assert _plan(ctx, RecordStage(ctx.settings)).fresh == 1


def test_evidence_must_be_built_on_the_article_s_current_record(ctx):
    import_run(ctx.catalog, _run(), [_row()])
    evidence_run = _run(stage="evidence", run_id="ns-ev1-20261102", model="numind/NuExtract3")
    moved = copy.deepcopy(RECORD)
    moved["analyses"][0]["name"]["value"] = "something else"
    import_run(ctx.catalog, evidence_run, [_row(record_digest=record_digest(moved))])
    assert ctx.catalog.artifact(ARTICLE, "evidence", "ns-27b").error == RECORD_MOVED

    import_run(ctx.catalog, evidence_run, [_row(record_digest=record_digest(RECORD))])
    evidence = ctx.catalog.artifact(ARTICLE, "evidence", "ns-27b")
    assert evidence.ok and evidence.run_id == "ns-ev1-20261102"
    # the record's maker, not the evidence model's, is the record's provenance
    assert ctx.catalog.payload(evidence)["extraction_metadata"]["extractor_model"] == \
        "Qwen/Qwen3.8-27B"
    assert _plan(ctx, EvidenceStage(ctx.settings)).fresh == 1


def test_a_new_record_makes_its_evidence_and_normalization_stale(ctx):
    import_run(ctx.catalog, _run(), [_row()])
    digest = record_digest(RECORD)
    import_run(ctx.catalog, _run(stage="evidence", run_id="ev"), [_row(record_digest=digest)])
    import_run(ctx.catalog, _run(stage="normalize", run_id="norm", model="SapBERT + MONDO",
                                 config={"vocabulary": {"mondo": "2026-09-01"}}),
               [{"article_id": ARTICLE, "status": "ok", "record_digest": digest,
                 "record": {"tasks": [{"task": "t1", "canonical": "n-back"}]}}])
    assert _plan(ctx, NormalizeStage(ctx.settings)).fresh == 1
    assert _plan(ctx, EvidenceStage(ctx.settings)).fresh == 1

    newer = copy.deepcopy(RECORD)
    newer["analyses"][0]["name"]["value"] = "a later run's name"
    import_run(ctx.catalog, _run(run_id="ns-27b-e-20261105", prompt_version="v6"),
               [_row(record=newer)])
    assert _plan(ctx, NormalizeStage(ctx.settings)).fresh == 0
    assert _plan(ctx, EvidenceStage(ctx.settings)).fresh == 0


def test_a_normalization_of_another_record_is_refused(ctx):
    import_run(ctx.catalog, _run(), [_row()])
    import_run(ctx.catalog, _run(stage="normalize", run_id="norm"),
               [{"article_id": ARTICLE, "status": "ok", "record_digest": "0" * 64,
                 "record": {"tasks": []}}])
    assert ctx.catalog.artifact(ARTICLE, "normalize", "ns-27b").error == RECORD_MOVED
