"""One rule for a model server that cannot be used: the stage stops, for roles and prose alike.

The scheduler stops the stage for the run, records no failure for the articles the model did
not answer, and says why in the report. An article answered before the stop keeps its outcome.
"""

from __future__ import annotations

import socket

import pytest
from ingestion_workflow.pipeline import Context
from ingestion_workflow.pipeline import scheduler
from ingestion_workflow.pipeline.scheduler import batched, run_stages
from ingestion_workflow.pipeline.stages.passages import PassagesStage
from ingestion_workflow.pipeline.stages.prose import ProseStage
from ingestion_workflow.pipeline.stages.roles import RolesStage
from ingestion_workflow.catalog import Outcome, Status
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.tests.pipeline import test_prose_stages as prose
from ingestion_workflow.tests.pipeline import test_roles_served as roles
from ingestion_workflow.tests.pipeline.test_prose_stages import env as prose_env  # noqa: F401
from ingestion_workflow.tests.pipeline.test_roles_served import env as roles_env  # noqa: F401
from ingestion_workflow.tests.pipeline.test_roles_served import stub  # noqa: F401


def _closed_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _prose_articles(settings, catalog, path, pmids):
    ctx = Context(settings, catalog)
    refs = [catalog.register(Identifier(pmid=p)) for p in pmids]
    for ref in refs:
        prose._record_upstream(catalog, ref, path)
        prose._run(PassagesStage(settings), ctx, catalog, ref)
    return ctx, refs


def _role_articles(catalog, ref, *names):
    """`ref`, then one article per name, each with a table of that set."""
    others = []
    for n, name in enumerate(names):
        other = catalog.register(Identifier(pmid=str(20 + n)))
        catalog.record([Outcome(article_id=other.id, stage="analyses", source="",
                                fingerprint=f"an-{20 + n}", payload=roles._tables(name),
                                summary={"tables": 1})])
        others.append(other)
    return [ref, *others]


# --------------------------------------------------------------------------- prose


def test_an_unreachable_prose_server_stops_the_stage_for_the_run(prose_env, monkeypatch):
    settings, catalog, path = prose_env
    settings.prose_api_base = f"http://127.0.0.1:{_closed_port()}/v1"
    settings.llm_max_retries = 1
    ctx, refs = _prose_articles(settings, catalog, path, ("1", "2", "3"))
    monkeypatch.setattr(scheduler, "batched", lambda refs: batched(refs, 1))
    report = run_stages(ctx, [ProseStage(settings)], refs)
    stage = report.stages["prose"]
    assert "did not answer in 2 tries" in stage.stopped
    assert stage.planned == 1  # the later batches are never planned: the stage has stopped
    assert catalog.artifacts([r.id for r in refs], "prose") == {}
    assert f"STOPPED: {stage.stopped}" in stage.line()


@pytest.mark.vcr
def test_a_refused_key_stops_prose(prose_env):
    settings, catalog, path = prose_env
    settings.prose_api_base = "http://prose.test/v1"
    ctx, refs = _prose_articles(settings, catalog, path, ("1", "2"))
    report = run_stages(ctx, [ProseStage(settings)], refs)
    stage = report.stages["prose"]
    assert "HTTP 401" in stage.stopped and "nu-prose-v2" in stage.stopped
    assert (stage.ok, stage.failed) == (0, 0)
    assert catalog.artifacts([r.id for r in refs], "prose") == {}


@pytest.mark.vcr
def test_prose_reads_a_passage_when_the_server_answers(prose_env):
    settings, catalog, path = prose_env
    settings.prose_api_base = "http://prose.test/v1"
    ctx, refs = _prose_articles(settings, catalog, path, ("1",))
    report = run_stages(ctx, [ProseStage(settings)], refs)
    stage = report.stages["prose"]
    assert (stage.ok, stage.failed, stage.stopped) == (1, 0, None)
    (read,) = catalog.artifacts([refs[0].id], "prose")[refs[0].id].values()
    assert read.status is Status.OK


# --------------------------------------------------------------------------- roles


def test_a_roles_server_that_stops_answering_stops_the_stage_and_keeps_what_it_answered(
    roles_env, stub, monkeypatch  # noqa: F811
):
    settings, catalog, ref = roles_env
    refs = _role_articles(catalog, ref, "patients")  # [ref (has Lee), patients-only]
    monkeypatch.setattr("ingestion_workflow.services.set_roles.served.time.sleep", lambda s: None)

    def answers(body):
        if "[NAME] Lee" not in str(body["messages"]):
            return 200, roles._choice('{"role": "result"}', roles.RESULT_LIKELY)
        stub.healthy = False  # it goes down on the next article, after the plan listed the model
        return 503, "down"

    stub.reply = answers
    report = run_stages(Context(settings, catalog), [RolesStage(settings)], [refs[1], refs[0]])
    stage = report.stages["roles"]
    assert "did not answer in 3 tries: HTTP 503" in stage.stopped
    assert (stage.ok, stage.failed) == (1, 0)
    assert set(catalog.artifacts([r.id for r in refs], "roles")) == {refs[1].id}


def test_a_refused_key_stops_roles(roles_env, stub):  # noqa: F811
    settings, catalog, ref = roles_env
    stub.reply = lambda body: (401, {"error": {"message": "Incorrect API key provided."}})
    report = run_stages(Context(settings, catalog), [RolesStage(settings)], [ref])
    stage = report.stages["roles"]
    assert "configuration error:" in stage.stopped and "HTTP 401" in stage.stopped
    assert (stage.ok, stage.failed) == (0, 0)
    assert catalog.artifacts([ref.id], "roles") == {}


def test_roles_decides_every_set_when_the_server_answers(roles_env, stub):  # noqa: F811
    settings, catalog, ref = roles_env
    report = run_stages(Context(settings, catalog), [RolesStage(settings)], [ref])
    stage = report.stages["roles"]
    assert (stage.ok, stage.failed, stage.stopped) == (1, 0, None)
