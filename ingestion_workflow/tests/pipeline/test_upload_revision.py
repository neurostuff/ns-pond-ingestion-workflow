"""A paper uploaded before goes to Neurostore as a revision of the parse it holds, and its
27B record follows the revision instead of going back to the model.

The paper is article 26klegjq7mhi (`test_record_upload`): the parse uploaded to production
with v21, its 27B record and ev1 evidence, and the parse v23 gave it on 2026-11-24, which
keeps the one analysis's key and reads its role as `reference`. The later parses rename,
add to and extend it as v23 did other papers (`test_parse_revision`).
"""

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

from ingestion_workflow.catalog import Outcome, Status
from ingestion_workflow.pipeline.stages.offloaded import (
    AWAITS_IMPORT,
    EvidenceStage,
    RecordStage,
    carried_from,
    Parse,
)
from ingestion_workflow.pipeline.stages.record_upload import RecordUploadStage
from ingestion_workflow.pipeline.stages.upload import GATED, UploadStage
from study_schema.keys import key_for
from study_schema.models import paper_parse as pp
from test_record_upload import ARTICLE, BASE, PARSE, _chain, _plan, _send, ctx, neurostore  # noqa: F401

V23 = json.loads((Path(__file__).resolve().parents[1] / "data" / "parse_revision"
                  / "26klegjq7mhi.v23.coordinate_parse.json").read_text())
KEY = PARSE["analyses"][0]["key"]


def _study(ctx) -> Path:
    return Path(ctx.catalog.artifact(ARTICLE, "sync").summary["path"])


def _synced(ctx, parse, n=[0]):
    """`parse` written where sync writes it, and sync's artifact naming it."""
    n[0] += 1
    (_study(ctx) / "parse" / "coordinate_parse.json").write_text(json.dumps(parse))
    ctx.catalog.record([Outcome(ARTICLE, "sync", "", Status.OK, f"sync-fp-{n[0]}",
                                summary={"path": str(_study(ctx)), "parse_id": parse["parse_id"],
                                         "analyses": len(parse["analyses"]),
                                         "base_study_id": BASE})])


def _rekeyed(parse, tag):
    """`parse` with each analysis keyed by its content, and a parse_id of its own."""
    parse = copy.deepcopy(parse)
    for analysis in parse["analyses"]:
        analysis["key"] = key_for(pp.ParsedAnalysis.model_validate(analysis))
    parse["parse_id"] = hashlib.sha256(tag.encode()).hexdigest()
    return parse


def _renamed(parse=V23):
    """v23's section prefix on the one analysis: the same point, another name and key."""
    out = copy.deepcopy(parse)
    out["analyses"][0]["name"] = "Prior meta-analysis — Laird et al. (2011)"
    return _rekeyed(out, "renamed")


def _upload(ctx):
    """The upload stage, its upstream gate let through: the fixture holds no readings."""
    stage = UploadStage(ctx.settings)
    stage.held_back = lambda *args: {}
    try:
        return _send(ctx, stage)
    finally:
        stage.close()


def _uploaded(ctx):
    return ctx.catalog.artifact(ARTICLE, "upload")


def test_the_first_upload_is_a_plain_parse_and_keeps_what_it_sent(ctx, neurostore):
    _, (outcome,) = _upload(ctx)
    assert outcome.status is Status.OK
    assert neurostore.uploads[-1]["coordinate_parse"]["revision_of"] is None
    kept = _uploaded(ctx)
    assert ctx.catalog.payload(kept) == PARSE
    assert (kept.summary["text_sha256"], kept.summary["text_length"]) == (
        PARSE["text_sha256"], 40321)
    assert kept.summary["revisions"] == []


def test_a_new_parse_goes_as_a_revision_of_the_one_neurostore_holds(ctx, neurostore):
    """v23 keeps the key and changes the role: an accept, never a relabel, which is a
    reviewer's correction Neurostore keeps against every later parse."""
    _upload(ctx)
    _synced(ctx, V23)
    _, (outcome,) = _upload(ctx)
    assert outcome.status is Status.OK
    sent = neurostore.uploads[-1]["coordinate_parse"]
    assert sent["revision_of"] == PARSE["parse_id"] and sent["parse_id"] == V23["parse_id"]
    assert [(v["key"], v["verdict"], v["replaced_by"]) for v in sent["verdicts"]] == [
        (KEY, "accept", [KEY])]
    assert neurostore.current[BASE] == V23["parse_id"]
    (step,) = _uploaded(ctx).summary["revisions"]
    assert (step["revision_of"], step["carries_all"], step["text"]) == (
        PARSE["parse_id"], True, "same")
    assert outcome.summary["ingester"]["revision"]["verdicts"] == {"accept": 1}


def test_a_renamed_analysis_carries_the_record_which_is_not_sent_again(ctx, neurostore):
    _chain(ctx)
    _upload(ctx)
    _, (sent,) = _send(ctx, RecordUploadStage(ctx.settings))
    assert sent.status is Status.OK and sent.summary["parse_id"] == PARSE["parse_id"]

    renamed = _renamed()
    _synced(ctx, renamed)
    # Before the revision is uploaded, the record names a key the parse lacks.
    assert _plan(ctx, RecordStage(ctx.settings)).reasons == {AWAITS_IMPORT: 1}
    _upload(ctx)
    sent = neurostore.uploads[-1]["coordinate_parse"]
    assert [(v["verdict"], v["replaced_by"]) for v in sent["verdicts"]] == [
        ("edit", [renamed["analyses"][0]["key"]])]

    record = ctx.catalog.artifact(ARTICLE, "record", "ns-27b")
    parse = Parse.of(ctx.catalog.root, ctx.catalog.artifact(ARTICLE, "sync"))
    assert carried_from(record, parse, _uploaded(ctx)) == PARSE["parse_id"]
    assert _plan(ctx, RecordStage(ctx.settings)).fresh == 1
    assert _plan(ctx, EvidenceStage(ctx.settings)).fresh == 1
    # Neurostore carried the claims when it took the revision: nothing is sent again.
    before = len(neurostore.uploads)
    plan = _plan(ctx, RecordUploadStage(ctx.settings))
    assert plan.fresh == 1 and plan.pending == [] and len(neurostore.uploads) == before


def test_a_record_never_sent_goes_against_the_parse_it_read(ctx, neurostore):
    _chain(ctx)
    _upload(ctx)
    _synced(ctx, _renamed())
    _upload(ctx)
    _, (outcome,) = _send(ctx, RecordUploadStage(ctx.settings))
    assert outcome.status is Status.OK
    assert neurostore.uploads[-1]["parse_id"] == PARSE["parse_id"]


def test_an_added_analysis_sends_the_paper_back_to_the_27b(ctx, neurostore):
    """A null result v23 reads that v21 did not: an add, which no record describes."""
    _chain(ctx)
    _upload(ctx)
    added = copy.deepcopy(_renamed())
    null = copy.deepcopy(added["analyses"][0])
    null.update(name="Laird et al. (2011): no difference", points=[], role="result")
    added["analyses"].append(null)
    added = _rekeyed(added, "added")
    _synced(ctx, added)
    _upload(ctx)
    verdicts = neurostore.uploads[-1]["coordinate_parse"]["verdicts"]
    assert sorted(v["verdict"] for v in verdicts) == ["add", "edit"]
    assert _uploaded(ctx).summary["revisions"][-1]["carries_all"] is False
    assert _plan(ctx, RecordStage(ctx.settings)).reasons == {AWAITS_IMPORT: 1}


def test_a_supplement_appended_to_the_text_keeps_the_record_and_its_evidence(ctx, neurostore):
    """98.9% of the texts that changed on 2026-11-24 kept the old text and appended a
    supplement's, so every offset the record and its evidence hold still points where it
    did."""
    _chain(ctx)
    _upload(ctx)
    study = _study(ctx)
    paper_path = study / "parse" / "parsed_paper.json"
    paper = json.loads(paper_path.read_text())
    text_path = study / paper["text_path"]
    text = text_path.read_bytes().decode("utf-8") + "\n\nSupplementary material\n\nTable S1 ...\n"
    text_path.write_bytes(text.encode("utf-8"))
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    paper.update(text_sha256=digest, text_length=len(text))
    paper_path.write_text(json.dumps(paper))
    extended = _renamed()
    extended["text_sha256"] = digest
    _synced(ctx, extended)
    _, (outcome,) = _upload(ctx)
    assert outcome.status is Status.OK, outcome.error
    assert _uploaded(ctx).summary["revisions"][-1]["text"] == "extended"
    assert _plan(ctx, RecordStage(ctx.settings)).fresh == 1
    assert _plan(ctx, EvidenceStage(ctx.settings)).fresh == 1


def test_a_paper_whose_stored_parse_is_not_kept_is_held_back(ctx, neurostore, tmp_path):
    """Uploaded before uploads kept what they sent: nothing goes until `upload_revised_root`
    names a tree holding that parse, since a plain parse would supersede it."""
    _upload(ctx)
    held = _uploaded(ctx)
    ctx.catalog.record([Outcome(ARTICLE, "upload", "", Status.OK, held.fingerprint,
                                summary={k: v for k, v in held.summary.items()
                                         if k not in ("text_sha256", "text_length",
                                                      "revisions")})])
    ctx.catalog._conn.execute("UPDATE artifacts SET blob=NULL WHERE stage='upload'")
    backup = tmp_path / "presync" / _study(ctx).name / "parse"
    backup.mkdir(parents=True)
    for name in ("coordinate_parse.json", "parsed_paper.json"):
        (backup / name).write_bytes((_study(ctx) / "parse" / name).read_bytes())
    _synced(ctx, _renamed())
    sent = len(neurostore.uploads)
    _, (outcome,) = _upload(ctx)
    assert outcome.status is Status.FAILED and outcome.error.startswith(GATED)
    assert PARSE["parse_id"] in outcome.error and len(neurostore.uploads) == sent

    ctx.settings = ctx.settings.model_copy(update={"upload_revised_root": backup.parents[1]})
    _, (outcome,) = _upload(ctx)
    assert outcome.status is Status.OK
    assert neurostore.uploads[-1]["coordinate_parse"]["revision_of"] == PARSE["parse_id"]
    assert _uploaded(ctx).summary["revisions"][-1]["text"] == "same"


def test_neurostore_refuses_a_revision_of_a_parse_that_is_not_current(ctx, neurostore):
    """A reviewer's revision made since (pondie's): Neurostore's current parse is not the
    one ns-pond sent, so the revision is refused and nothing there is replaced."""
    _upload(ctx)
    neurostore.current[BASE] = "a-reviewers-revision"
    _synced(ctx, _renamed())
    _, (outcome,) = _upload(ctx)
    assert outcome.status is Status.PERMANENT and "NOT_CURRENT" in outcome.error
