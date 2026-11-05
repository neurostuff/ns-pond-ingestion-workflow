"""`record_upload`: a record goes to neurostore once its chain is fresh and its parse is there.

The paper is article 26klegjq7mhi as ns-pond holds it on beast, with the record its
2026-10-31 run made and the evidence ev1 added to it; neurostore is the contract-checked
stand-in (`ingestion_workflow.tests.neurostore`), on the spec neurostore#1910 merged.
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
from ingestion_workflow.pipeline.stages.offloaded import record_digest
from ingestion_workflow.pipeline.stages.record_upload import (
    NO_CHAIN,
    NO_PARSE_UPLOADED,
    RecordUploadStage,
    sent_run,
)
from ingestion_workflow.services.neurostore_ingest import NeurostoreIngester
from ingestion_workflow.services.offload.importer import import_run
from ingestion_workflow.tests import neurostore as mock

DATA = Path(__file__).resolve().parents[1] / "data" / "offload"
ARTICLE, BASE = "26klegjq7mhi", "tS8tSUXeybTo"
PARSE = json.loads((DATA / "paper-26klegjq7mhi/parse/coordinate_parse.json").read_text())
RECORD = json.loads((DATA / "26klegjq7mhi.extraction.json").read_text())
EVIDENCED = json.loads((DATA / "26klegjq7mhi.ev1.extraction.json").read_text())
ADAPTER = "jdkent/qwen3.8-27b-ns-record@dbfd846bd3e1259039eb2f3117dc12bd733c640a"


def _run(stage, **kwargs) -> Run:
    base = dict(run_id="", kind=RunKind.MODEL, stage=stage, source="ns-27b",
                pipeline="ns-pond offload", version=f"v-{stage}", model="m", adapter=ADAPTER,
                prompt_version="p", schema_version="s", code_commit="abc1234",
                machine="vista", config={"shards": 1}, bundle_id="b",
                created_at="2026-10-31T13:43:00+00:00")
    base.update(kwargs)
    run = Run(**base)
    return Run(**{**run.__dict__, "run_id": run.minted_id})


RECORD_RUN = _run("record", version="ns-27b-e")
EVIDENCE_RUN = _run("evidence", version="ns-ev1", model="numind/NuExtract3", machine="ls6")
NORMALIZE_RUN = _run("normalize", version="astra-norm", adapter=None, fit="fit-ee8b3879",
                     machine="beast (CPU)", config={"vocabulary": {"mondo": "2026-09-01"}})


@pytest.fixture()
def ctx(tmp_path, monkeypatch):
    settings = Settings(data_root=tmp_path / "data", cache_root=tmp_path / "cache",
                        catalog_root=tmp_path / "cat", ns_pond_root=tmp_path / "pond",
                        neurostore_api_url=mock.URL)
    paper = tmp_path / "pond" / BASE
    shutil.copytree(DATA / "paper-26klegjq7mhi", paper)
    with Catalog.open(settings.catalog_root) as catalog:
        catalog._conn.execute("INSERT INTO articles(id, created_at) VALUES(?, '2026-01-01')",
                              (ARTICLE,))
        catalog.record([Outcome(ARTICLE, "sync", "", Status.OK, "sync-fp",
                                summary={"path": str(paper), "parse_id": PARSE["parse_id"],
                                         "analyses": 1, "base_study_id": BASE})])
        yield Context(settings, catalog, retry_after=timedelta(hours=24))


@pytest.fixture()
def neurostore(monkeypatch):
    with mock.serve(monkeypatch) as server:
        yield server


def _chain(ctx, normalized=None):
    """The record, its evidence and its normalization, as `ingest offload import` keeps them."""
    import_run(ctx.catalog, RECORD_RUN, [{"article_id": ARTICLE, "status": "ok",
                                          "text_sha256": PARSE["text_sha256"],
                                          "record": RECORD}])
    digest = record_digest(ctx.catalog.payload(ctx.catalog.artifact(ARTICLE, "record", "ns-27b")))
    import_run(ctx.catalog, EVIDENCE_RUN, [{"article_id": ARTICLE, "status": "ok",
                                            "text_sha256": PARSE["text_sha256"],
                                            "record_digest": digest, "record": EVIDENCED}])
    import_run(ctx.catalog, NORMALIZE_RUN, [{"article_id": ARTICLE, "status": "ok",
                                             "record_digest": digest,
                                             "record": normalized or EVIDENCED}])


def _parse_uploaded(ctx, neurostore):
    """The article's parse sent first, as `upload` sends it, and its artifact kept."""
    with NeurostoreIngester.from_settings(ctx.settings) as ingester:
        sent = ingester.upload(Path(ctx.catalog.artifact(ARTICLE, "sync").summary["path"]))
    assert sent["status"] == "uploaded"
    ctx.catalog.record([Outcome(ARTICLE, "upload", "", Status.OK, "upload-fp",
                                summary={"backend": "ingester", "parse_id": PARSE["parse_id"],
                                         "base_study_id": sent["summary"]["base_study_id"]})])


def _plan(ctx, stage):
    return stage.plan(ctx, [ctx.catalog.ref(ARTICLE)],
                      ctx.catalog.artifacts([ARTICLE], stage.name),
                      ctx.catalog.artifacts([ARTICLE], stage.requires))


def _send(ctx, stage):
    plan = _plan(ctx, stage)
    outcomes = list(stage.execute(ctx, plan.pending))
    ctx.catalog.record(outcomes)
    return plan, outcomes


def test_the_normalized_record_goes_with_its_three_runs_as_kept(ctx, neurostore):
    _chain(ctx)
    _parse_uploaded(ctx, neurostore)
    stage = RecordUploadStage(ctx.settings)
    plan, (outcome,) = _send(ctx, stage)
    assert len(plan.pending) == 1 and outcome.status is Status.OK
    sent = neurostore.uploads[-1]
    assert sent["record"] == ctx.catalog.payload(ctx.catalog.artifact(ARTICLE, "normalize",
                                                                      "ns-27b"))
    assert (sent["parse_id"], sent["base_study_id"]) == (PARSE["parse_id"], BASE)
    runs = sent["pipeline"]["runs"]
    assert [r["stage"] for r in runs] == ["record", "evidence", "normalize"]
    assert runs == [sent_run(ctx.catalog.run(r.run_id))
                    for r in (RECORD_RUN, EVIDENCE_RUN, NORMALIZE_RUN)]
    # how ns-pond ran it stays behind: neurostore refuses fields it does not list
    assert not {"config", "bundle_id", "imported_at"} & set(runs[0])
    assert runs[2]["fit"] == "fit-ee8b3879" and runs[0]["kind"] == "model"
    assert sent["pipeline"]["version"] == "ns-27b-e+ns-ev1+astra-norm"
    kept = ctx.catalog.artifact(ARTICLE, "record_upload", "ns-27b")
    assert kept.ok and kept.summary["runs"] == [RECORD_RUN.run_id, EVIDENCE_RUN.run_id,
                                                NORMALIZE_RUN.run_id]
    assert _plan(ctx, stage).fresh == 1


def test_the_same_record_again_is_a_replay_and_a_newer_one_a_new_run_id(ctx, neurostore):
    from ingestion_workflow.pipeline.stages.record_upload import run_id_of

    assert run_id_of(EVIDENCED) == run_id_of(copy.deepcopy(EVIDENCED))
    newer = copy.deepcopy(EVIDENCED)
    newer["analyses"][0]["name"]["value"] = "a later run's name"
    assert run_id_of(newer) != run_id_of(EVIDENCED)


def test_a_record_waits_for_its_parse_on_neurostore(ctx, neurostore):
    _chain(ctx)
    plan = _plan(ctx, RecordUploadStage(ctx.settings))
    assert plan.pending == [] and plan.reasons == {NO_PARSE_UPLOADED: 1}
    # an upload of an earlier parse is not this one's
    ctx.catalog.record([Outcome(ARTICLE, "upload", "", Status.OK, "upload-fp",
                                summary={"parse_id": "an-earlier-parse", "base_study_id": BASE})])
    assert _plan(ctx, RecordUploadStage(ctx.settings)).reasons == {NO_PARSE_UPLOADED: 1}


def test_a_chain_without_evidence_or_normalization_sends_nothing(ctx, neurostore):
    import_run(ctx.catalog, RECORD_RUN, [{"article_id": ARTICLE, "status": "ok",
                                          "text_sha256": PARSE["text_sha256"],
                                          "record": RECORD}])
    _parse_uploaded(ctx, neurostore)
    assert _plan(ctx, RecordUploadStage(ctx.settings)).reasons == {NO_CHAIN: 1}


def test_a_run_neurostore_holds_with_other_fields_is_refused_and_recorded(ctx, neurostore):
    _chain(ctx)
    _parse_uploaded(ctx, neurostore)
    neurostore.runs[RECORD_RUN.run_id] = {**sent_run(RECORD_RUN), "machine": "somewhere else"}
    _, (outcome,) = _send(ctx, RecordUploadStage(ctx.settings))
    assert outcome.status is Status.PERMANENT and "RUN_CONFLICT" in outcome.error


def test_a_dry_run_writes_the_body_and_sends_nothing(ctx, neurostore):
    _chain(ctx)
    _parse_uploaded(ctx, neurostore)
    sent = len(neurostore.uploads)
    stage = RecordUploadStage(ctx.settings.model_copy(update={"dry_run": True}))
    plan = _plan(ctx, stage)
    assert list(stage.execute(ctx, plan.pending)) == []  # a dry run records nothing
    assert len(neurostore.uploads) == sent
    (line,) = [json.loads(x) for x in stage.dry_run_log().read_text().splitlines()]
    assert line["status"] == "dry_run" and line["run_id"].startswith("ns-pond-")
    body = json.loads(Path(line["ingester"]["payload_path"]).read_text())
    assert body["run_id"] == line["run_id"]
    assert body["record"] == ctx.catalog.payload(ctx.catalog.artifact(ARTICLE, "normalize",
                                                                      "ns-27b"))


def test_a_skipped_normalization_completes_the_chain_and_the_evidence_record_goes(
        ctx, neurostore):
    import_run(ctx.catalog, RECORD_RUN, [{"article_id": ARTICLE, "status": "ok",
                                          "text_sha256": PARSE["text_sha256"],
                                          "record": RECORD}])
    digest = record_digest(ctx.catalog.payload(ctx.catalog.artifact(ARTICLE, "record", "ns-27b")))
    import_run(ctx.catalog, EVIDENCE_RUN, [{"article_id": ARTICLE, "status": "ok",
                                            "text_sha256": PARSE["text_sha256"],
                                            "record_digest": digest, "record": EVIDENCED}])
    import_run(ctx.catalog, NORMALIZE_RUN, [{"article_id": ARTICLE, "status": "skipped",
                                             "reason": "no tasks to map", "record": None}])
    _parse_uploaded(ctx, neurostore)
    _, (outcome,) = _send(ctx, RecordUploadStage(ctx.settings))
    sent = neurostore.uploads[-1]
    assert outcome.status is Status.OK
    assert sent["record"] == ctx.catalog.payload(ctx.catalog.artifact(ARTICLE, "evidence",
                                                                      "ns-27b"))
    assert [r["stage"] for r in sent["pipeline"]["runs"]] == ["record", "evidence"]
