"""The roles stage's served NuExtract3 role models, on a stub vLLM server."""

from __future__ import annotations

import hashlib
import json
import math
import os
import signal
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
from ingestion_workflow.clients.model_server import ModelUnavailable
from ingestion_workflow.catalog import Catalog, Outcome, Status
from ingestion_workflow.config import RoleModel, Settings
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.pipeline.scheduler import run_stages
from ingestion_workflow.pipeline import Context
from ingestion_workflow.pipeline.stages import roles as roles_module
from ingestion_workflow.pipeline.stages.roles import (
    RolesStage,
    UnansweredSets,
    assign_roles,
    raw_tables,
    set_contexts,
)
from ingestion_workflow.services.set_roles import (
    COORDINATE_ROLES,
    Prediction,
    Unanswered,
    decide,
    served_prompt,
    table_context,
)
from ingestion_workflow.services.set_roles import served as served_module
from ingestion_workflow.services.set_roles.served import (
    ServedClassifier,
    role_distribution,
)

MODEL = "nx-roles-table"

# --------------------------------------------------------------------------- the decision


class FakeBackend:
    """A classifier answering from the set's name, as a served model would."""

    def __init__(self, answers, source="fake@1"):
        self.answers = answers
        self.source = source
        self.seen = []

    def predict(self, texts):
        self.seen += texts
        out = []
        for text in texts:
            answer = next(v for k, v in self.answers.items() if f"[NAME] {k}" in text)
            if isinstance(answer, Unanswered):
                out.append(answer)
                continue
            role, probabilities, *kind = answer
            out.append(Prediction(role, kind[0] if kind else None, probabilities))
        return out


def _analysis(name, caption="", **meta):
    return {
        "name": name,
        "table_caption": caption,
        "table_footer": "",
        "description": None,
        "metadata": {"table_metadata": {"table_label": "Table 2"}, **meta},
        "coordinates": [{"x": 1.0, "y": 2.0, "z": 3.0, "space": "MNI"}],
    }


def _tables(*names):
    return {"t2": {"slug": "a::t2", "coordinate_space": "MNI", "identifier": None,
                   "analyses": [_analysis(n) for n in names]}}


def _probs(**given):
    return {r: given.get(r, 0.0) for r in COORDINATE_ROLES}


def test_a_reference_is_from_a_prior_study_and_no_other_role_says_either_way():
    answers = {
        "reference": Prediction("reference", None, _probs(reference=0.9, result=0.1)),
        "result": Prediction("result", None, _probs(result=1.0)),
        "anchor": Prediction("anchor", "seed", _probs(anchor=1.0)),
        "localization": Prediction("localization", None, _probs(localization=1.0)),
        "other": Prediction("other", None, _probs(other=1.0)),
    }
    got = {r: decide(p, source="m", origin="table").from_prior_study for r, p in answers.items()}
    # Nothing decides that a set is not from a prior study, so it is null, never False.
    assert got == {"reference": True, "result": None, "anchor": None,
                   "localization": None, "other": None}


def test_the_stage_records_the_model_s_answer_its_probability_and_its_name():
    backend = FakeBackend(
        {"patients": ("result", _probs(result=0.97, other=0.03)),
         "Lee": ("reference", _probs(result=0.35, reference=0.65))},
        source="nx-roles-table@v5")
    out, summary = assign_roles(_tables("patients", "Lee"), [], None, {"table": backend})
    first, second = (a["metadata"]["set_role"] for a in out["t2"]["analyses"])
    assert (first["role"], first["role_confidence"], first["role_source"]) == (
        "result", 0.97, "nx-roles-table@v5")
    # The answer stands, however likely another role is: there is no threshold.
    assert (second["role"], second["role_confidence"]) == ("reference", 0.65)
    assert summary["sources"] == {"table": "nx-roles-table@v5"}


def test_a_set_without_a_usable_answer_gives_the_article_no_roles_and_says_why():
    backend = FakeBackend({"patients": ("result", _probs(result=1.0)),
                           "Lee": Unanswered("server error on this input after 3 tries: boom")})
    with pytest.raises(UnansweredSets) as raised:
        assign_roles(_tables("patients", "Lee"), [], None, {"table": backend})
    assert raised.value.reasons == {"t2#1": "server error on this input after 3 tries: boom"}
    assert "t2#1: server error on this input after 3 tries: boom" in str(raised.value)


# --------------------------------------------------------------------------- the prompt


#: sha256 of the instruction the models were trained and scored with, as their
#: evaluation's metrics record it.
TRAINED = "531886df6f1c9575a0fce38163d7bd3cf1731272852be00b6e0e16a40fccff5f"
TEMPLATE_SHA = "7499daa7039b6f20b92fc29f6a278a60a1ab80cf8f51f3a824cc25810174e8f1"
SCHEMA_SHA = "35d00fbf6b4e35a276a3c0771b179f6e8ba3c6416b1b9df5cdc108db0035e6eb"


def _sha(text):
    return hashlib.sha256(text.encode()).hexdigest()


def test_the_prompt_is_the_one_the_models_were_trained_with():
    prompt = served_prompt.PROMPT
    assert prompt.version == "v5" and _sha(prompt.instruction) == TRAINED
    assert _sha(prompt.template()) == TEMPLATE_SHA
    assert _sha(json.dumps(prompt.schema(), sort_keys=True)) == SCHEMA_SHA
    assert prompt.content("[ORIGIN] table") == prompt.instruction + "\n\n[ORIGIN] table"
    prompt.check("table")
    prompt.check("text")


def test_a_prompt_for_other_contexts_or_other_roles_is_refused():
    stale = served_prompt.RolePrompt("v5", "x", context_versions={"table": 1, "text": 4})
    with pytest.raises(ValueError, match="table context version 1; this code builds 2"):
        stale.check("table")
    stale.check("text")
    other = served_prompt.RolePrompt("v5", "x", roles=("result", "seed"))
    with pytest.raises(ValueError, match="not study_schema's"):
        other.check("text")


# --------------------------------------------------------------------------- a stub vLLM


def _logprobs(alternatives, merged=False):
    """`logprobs.content` for `{"role": "<top>", ...}`, with `alternatives` at the role token."""
    top = max(alternatives, key=alternatives.get)
    alts = [{"token": t, "logprob": lp} for t, lp in alternatives.items()]
    if merged:  # one token closes the key's quote and begins the value
        alts = [{"token": '":"' + a["token"], "logprob": a["logprob"]} for a in alts]
        return [{"token": '{"', "logprob": 0.0}, {"token": "role", "logprob": 0.0},
                {"token": '":"' + top, "logprob": alternatives[top], "top_logprobs": alts},
                {"token": '"}', "logprob": 0.0}]
    return [{"token": '{"', "logprob": 0.0}, {"token": "role", "logprob": 0.0},
            {"token": '":', "logprob": 0.0}, {"token": ' "', "logprob": 0.0},
            {"token": top, "logprob": alternatives[top], "top_logprobs": alts},
            {"token": '"}', "logprob": 0.0}]


def _choice(content, alternatives=None, finish_reason="stop"):
    logprobs = {"content": _logprobs(alternatives)} if alternatives else None
    return {"choices": [{"message": {"content": content}, "logprobs": logprobs,
                         "finish_reason": finish_reason}]}


#: vLLM's answer to a request longer than its max_model_len.
TOO_LONG = (400, {"error": {"message": (
    "This model's maximum context length is 4096 tokens. However, you requested 64 output "
    "tokens and your prompt contains 1500 input tokens"), "type": "BadRequestError", "code": 400}})


#: Role tokens holding 0.8 of the mass, and "re", which begins two roles, 0.15.
RESULT_LIKELY = {"result": math.log(0.6), "anchor": math.log(0.1), "re": math.log(0.15),
                 "other": math.log(0.1)}


class _Server(ThreadingHTTPServer):
    # vLLM's backlog, not http.server's 5: a stage asks for more connections at once than that.
    request_queue_size = 2048


class Stub:
    """An OpenAI-compatible server: `/v1/models` lists `models`; `reply(body)` answers a chat."""

    def __init__(self, models=(MODEL,)):
        self.models = list(models)
        #: The cards `/v1/models` lists; by default one per name, as vLLM writes them.
        self.cards = None
        #: False: the model listing answers 503 too, as a server that is down does.
        self.healthy = True
        self.requests = []
        #: Model listings answered, and an Event a listing waits on when set.
        self.listings = 0
        self.gate = None
        self.reply = lambda body: (200, _choice('{"role": "result"}', RESULT_LIKELY))
        stub = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def _send(self, status, payload):
                text = payload if isinstance(payload, str) else json.dumps(payload)
                data = text.encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def do_GET(self):
                stub.listings += 1
                if stub.gate is not None:
                    stub.gate.wait(10)
                if not stub.healthy:
                    self._send(503, "down")
                    return
                cards = stub.cards or [{"id": m, "root": f"/models/{m}"} for m in stub.models]
                self._send(200, {"data": cards})

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                stub.requests.append(body)
                status, payload = stub.reply(body)
                self._send(status, payload)

        self.server = _Server(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}/v1"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture
def stub():
    server = Stub()
    yield server
    server.close()


def _served(stub, **kw):
    kw.setdefault("sleep", lambda s: None)
    return ServedClassifier(stub.url, MODEL, source=f"{MODEL}@v5", **kw)


def test_each_set_is_asked_exactly_as_the_model_was_trained_and_scored(stub):
    prompt = served_prompt.PROMPT
    (prediction,) = _served(stub).predict(["[ORIGIN] table [NAME] x"])
    assert stub.requests == [{
        "model": MODEL, "temperature": 0.0, "max_completion_tokens": 64,
        "messages": [
            {"role": "user", "content": prompt.instruction + "\n\n[ORIGIN] table [NAME] x"}
        ],
        "chat_template_kwargs": {"template": prompt.template(), "enable_thinking": False},
        "structured_outputs": {"json": prompt.schema()},
        "logprobs": True, "top_logprobs": 20,
    }]
    assert prediction.role == "result"


@pytest.mark.parametrize("merged", [False, True])
def test_p_result_is_the_first_role_token_s_mass_renormalised_over_role_tokens(merged):
    content = _logprobs(RESULT_LIKELY, merged=merged)
    probabilities = role_distribution(content, COORDINATE_ROLES)
    # "re" begins both result and reference: it is left out, and the rest renormalised.
    assert probabilities["result"] == pytest.approx(0.6 / 0.8)
    assert probabilities["anchor"] == pytest.approx(0.1 / 0.8)
    assert probabilities["other"] == pytest.approx(0.1 / 0.8)
    assert probabilities["reference"] == 0.0
    assert role_distribution(content[:2], COORDINATE_ROLES) is None


def test_the_served_answer_is_the_role_through_the_stage(stub):
    stub.reply = lambda body: (200, _choice(
        '{"role": "anchor", "anchor_kind": "seed"}',
        {"anchor": math.log(0.55), "result": math.log(0.45)}))
    out = assign_roles(_tables("seed"), [], None, {"table": _served(stub)})[0]
    role = out["t2"]["analyses"][0]["metadata"]["set_role"]
    assert (role["role"], role["anchor_kind"], role["role_source"]) == (
        "anchor", "seed", f"{MODEL}@v5")
    assert role["role_confidence"] == pytest.approx(0.55)


@pytest.mark.parametrize("content, alternatives, finish_reason, why", [
    # Cut at max_completion_tokens, or decoded without the schema.
    ('{"role": "res', RESULT_LIKELY, "length", "is not the role schema's"),
    ("not json", RESULT_LIKELY, "stop", "is not the role schema's"),
    ('["result"]', RESULT_LIKELY, "stop", "is not the role schema's"),
    ('{"role": "banana"}', RESULT_LIKELY, "stop", "is not the role schema's"),
    ("{}", RESULT_LIKELY, "stop", "is not the role schema's"),
    ('{"role": null}', RESULT_LIKELY, "stop", "is not the role schema's"),
    ('{"role": "anchor", "anchor_kind": "voxel"}', RESULT_LIKELY, "stop",
     "is not the role schema's"),
    # No logprobs, or every alternative at the role token begins two roles or none.
    ('{"role": "result"}', None, "stop", "no role-token logprobs"),
    ('{"role": "result"}', {"re": math.log(0.7), '"': math.log(0.3)}, "stop",
     "no role-token logprobs"),
])
def test_an_answer_outside_the_schema_is_a_configuration_error_that_stops_the_run(
    stub, content, alternatives, finish_reason, why
):
    stub.reply = lambda body: (200, _choice(content, alternatives, finish_reason))
    classifier = _served(stub)
    with pytest.raises(ModelUnavailable, match="configuration error") as raised:
        classifier.predict(["x"])
    assert why in str(raised.value)
    with pytest.raises(ModelUnavailable) as again:
        classifier.predict(["y"])
    assert str(again.value) == str(raised.value)
    assert len(stub.requests) == 1


def test_an_anchor_without_its_kind_is_an_anchor_with_no_kind(stub):
    # Not a result: that is the distinction recorded.
    stub.reply = lambda body: (200, _choice('{"role": "anchor", "anchor_kind": null}',
                                            RESULT_LIKELY))
    (answer,) = _served(stub).predict(["x"])
    assert (answer.role, answer.anchor_kind) == ("anchor", None)


def test_a_kind_on_an_answer_that_is_not_an_anchor_is_dropped(stub):
    stub.reply = lambda body: (200, _choice('{"role": "result", "anchor_kind": "roi"}',
                                            RESULT_LIKELY))
    (answer,) = _served(stub).predict(["x"])
    assert (answer.role, answer.anchor_kind) == ("result", None)


def test_a_context_longer_than_the_server_s_max_model_len_stops_the_run(stub):
    stub.reply = lambda body: (TOO_LONG if "LONGSET" in str(body["messages"])
                               else (200, _choice('{"role": "result"}', RESULT_LIKELY)))
    classifier = _served(stub)
    with pytest.raises(ModelUnavailable) as raised:
        classifier.predict(["LONGSET"])
    said = str(raised.value)
    assert said.startswith(f"configuration error: {stub.url}/chat/completions answered HTTP 400")
    assert "maximum context length is 4096 tokens" in said
    # Not asked again, and no other set is asked in this run either.
    with pytest.raises(ModelUnavailable, match="maximum context length"):
        classifier.predict(["SHORTSET"])
    assert len(stub.requests) == 1


def test_a_5xx_or_a_timeout_is_asked_again(stub, monkeypatch):
    replies = iter([(503, "busy"), (502, "gateway")])
    stub.reply = lambda body: next(replies, (200, _choice('{"role": "result"}', RESULT_LIKELY)))
    waits = []
    (answer,) = _served(stub, sleep=waits.append).predict(["x"])
    assert isinstance(answer, Prediction) and len(stub.requests) == 3 and waits == [1, 2]

    def slow_once(body):
        if len(stub.requests) == 4:
            time.sleep(0.5)
        return 200, _choice('{"role": "result"}', RESULT_LIKELY)

    stub.reply = slow_once
    monkeypatch.setattr(served_module, "TIMEOUT", 0.2)
    (answer,) = _served(stub).predict(["x"])
    assert isinstance(answer, Prediction) and len(stub.requests) == 5


def test_a_429_is_asked_again(stub):
    replies = iter([(429, "slow down")])
    stub.reply = lambda body: next(replies, (200, _choice('{"role": "result"}', RESULT_LIKELY)))
    waits = []
    (answer,) = _served(stub, sleep=waits.append).predict(["x"])
    assert isinstance(answer, Prediction) and len(stub.requests) == 2 and waits == [1]


def test_a_429_that_does_not_clear_fails_that_set_only_while_the_server_lists_the_model(stub):
    stub.reply = lambda body: ((429, "slow down") if "BUSY" in str(body["messages"])
                               else (200, _choice('{"role": "result"}', RESULT_LIKELY)))
    classifier = _served(stub)
    busy, fine = classifier.predict(["BUSY", "FINE"])
    assert isinstance(busy, Unanswered) and "HTTP 429" in busy.reason
    assert isinstance(fine, Prediction)
    assert isinstance(classifier.predict(["FINE AGAIN"])[0], Prediction)  # not stopped


def test_a_server_that_lists_only_other_models_is_not_up_for_this_one(stub):
    stub.models = ["some-other-model"]
    stub.reply = lambda body: (503, "busy")
    classifier = _served(stub)
    with pytest.raises(ModelUnavailable, match="did not answer in 3 tries"):
        classifier.predict(["x"])
    asked = len(stub.requests)
    with pytest.raises(ModelUnavailable, match="did not answer in 3 tries"):
        classifier.predict(["y"])
    assert len(stub.requests) == asked


def test_a_server_that_never_answers_stops_the_run_and_is_not_asked_again(stub):
    stub.reply = lambda body: (503, "down")
    stub.healthy = False
    classifier = _served(stub)
    with pytest.raises(ModelUnavailable, match="did not answer in 3 tries: HTTP 503"):
        classifier.predict(["x"])
    asked = len(stub.requests)
    assert asked == 3
    with pytest.raises(ModelUnavailable):
        classifier.predict(["y"])
    assert len(stub.requests) == asked


# --------------------------------------------------------------------------- the stage


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def env(tmp_path, stub, evidence):
    settings = Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c",
                        catalog_root=tmp_path / "k", ns_pond_root=tmp_path / "pond")
    settings.role_model_table = RoleModel(endpoint=stub.url, model=MODEL)
    with Catalog.open(settings.catalog_root, evidence=evidence) as catalog:
        ref = catalog.register(Identifier(pmid="7"))
        catalog.record([Outcome(article_id=ref.id, stage="analyses", source="", fingerprint="an-1",
                                payload=_tables("patients", "Lee"), summary={"tables": 1})])
        yield settings, catalog, ref


def _run(stage, settings, catalog, ref):
    ctx = Context(settings, catalog)
    plan = stage.plan(ctx, [ref], catalog.artifacts([ref.id], stage.name),
                      catalog.artifacts([ref.id], stage.requires))
    return plan, list(stage.execute(ctx, plan.pending))


def test_a_served_model_listed_by_its_server_decides_every_set(env, stub):
    settings, catalog, ref = env
    stage = RolesStage(settings)
    source, reason = stage.model_state("table")
    assert reason is None and source.startswith(f"{MODEL}@v5+")
    plan, (done,) = _run(stage, settings, catalog, ref)
    assert (plan.blocked, done.status) == (0, Status.OK)
    roles = [a["metadata"]["set_role"] for a in done.payload["t2"]["analyses"]]
    assert {r["role_source"] for r in roles} == {source}
    assert [r["role_confidence"] for r in roles] == [pytest.approx(0.75)] * 2


@pytest.mark.parametrize("change, reason", [
    (lambda s, stub: setattr(stub, "models", ["another-model"]),
     f"the table role model {MODEL} at {{url}} is not served there (it serves ['another-model'])"),
    (lambda s, stub: setattr(s.role_model_table, "endpoint",
                             f"http://127.0.0.1:{_free_port()}/v1"),
     "cannot be reached: ConnectError"),
])
def test_a_served_model_that_cannot_be_used_blocks_the_article_at_plan_time(
    env, stub, change, reason
):
    settings, catalog, ref = env
    change(settings, stub)
    plan, outcomes = _run(RolesStage(settings), settings, catalog, ref)
    assert (plan.blocked, plan.pending, outcomes) == (1, [], [])
    (said,) = plan.reasons
    assert reason.format(url=stub.url) in said
    assert stub.requests == []


def test_a_served_model_trained_on_other_contexts_blocks_the_article(env, stub, monkeypatch):
    settings, catalog, ref = env
    stale = served_prompt.RolePrompt("v5", served_prompt.PROMPT.instruction,
                                     context_versions={"table": 1, "text": 4})
    monkeypatch.setattr("ingestion_workflow.pipeline.stages.roles.PROMPT", stale)
    plan, outcomes = _run(RolesStage(settings), settings, catalog, ref)
    assert (plan.blocked, outcomes) == (1, [])
    (said,) = plan.reasons
    assert "trained on table context version 1; this code builds 2; retrain them" in said


def test_an_anchor_without_its_kind_is_recorded_as_an_anchor_through_the_stage(env, stub):
    settings, catalog, ref = env
    stub.reply = lambda body: (
        (200, _choice('{"role": "anchor", "anchor_kind": null}', RESULT_LIKELY))
        if "[NAME] Lee" in str(body["messages"])
        else (200, _choice('{"role": "result"}', RESULT_LIKELY)))
    _, (done,) = _run(RolesStage(settings), settings, catalog, ref)
    assert done.status is Status.OK
    roles = {a["name"]: a["metadata"] for a in done.payload["t2"]["analyses"]}
    lee = roles["Lee"]["set_role"]
    assert (lee["role"], lee["anchor_kind"], lee["from_prior_study"]) == ("anchor", None, None)
    assert roles["Lee"]["held"] is False


def test_an_over_long_context_stops_the_stage_and_spends_no_retry(env, stub):
    """A context the server cannot fit is how the model is served, not the article's failure."""
    settings, catalog, ref = env
    other = catalog.register(Identifier(pmid="8"))
    catalog.record([Outcome(article_id=other.id, stage="analyses", source="", fingerprint="an-2",
                            payload=_tables("patients"), summary={"tables": 1})])
    stub.reply = lambda body: (TOO_LONG if "[NAME] Lee" in str(body["messages"])
                               else (200, _choice('{"role": "result"}', RESULT_LIKELY)))
    ids = [ref.id, other.id]
    # `other` is answered first and kept; `ref` is the one the server refuses.
    report = run_stages(Context(settings, catalog), [RolesStage(settings)], [other, ref])
    roles = report.stages["roles"]
    assert "configuration error: " in roles.stopped
    assert "maximum context length is 4096 tokens" in roles.stopped
    assert (roles.ok, roles.failed) == (1, 0)
    assert catalog.artifacts(ids, "roles").keys() == {other.id}
    assert catalog.attempt_counts(ids, "roles", "") in ({}, {i: (0, None) for i in ids})


def test_a_server_down_mid_run_leaves_every_article_pending_and_spends_no_retry(
    env, stub, monkeypatch
):
    settings, catalog, ref = env
    other = catalog.register(Identifier(pmid="8"))
    catalog.record([Outcome(article_id=other.id, stage="analyses", source="", fingerprint="an-2",
                            payload=_tables("patients"), summary={"tables": 1})])
    monkeypatch.setattr("ingestion_workflow.services.set_roles.served.time.sleep", lambda s: None)

    def crashed(body):  # the server goes down after the plan listed the model
        stub.healthy = False
        return 503, "down"

    ctx = Context(settings, catalog)
    ids = [ref.id, other.id]
    for _ in range(ctx.max_attempts + 1):
        stub.healthy, stub.reply = True, crashed
        report = run_stages(ctx, [RolesStage(settings)], [ref, other])
        assert "did not answer in 3 tries: HTTP 503" in report.stages["roles"].stopped
        assert catalog.artifacts(ids, "roles") == {}
    assert catalog.attempt_counts(ids, "roles", "") in ({}, {i: (0, None) for i in ids})
    stub.healthy = True
    stub.reply = lambda body: (200, _choice('{"role": "result"}', RESULT_LIKELY))
    report = run_stages(ctx, [RolesStage(settings)], [ref, other])
    assert (report.stages["roles"].ok, report.stages["roles"].stopped) == (2, None)


def _card(root):
    return {"id": MODEL, "object": "model", "root": str(root)}


def test_a_retrained_model_under_the_same_name_is_a_new_source(env, stub, tmp_path):
    settings, _, _ = env
    merged = tmp_path / "merged"
    merged.mkdir()
    (merged / "config.json").write_text('{"a": 1}', encoding="utf-8")
    (merged / "model.safetensors").write_bytes(b"w1")
    stub.cards = [_card(merged)]
    stage = RolesStage(settings)
    first = stage.model_state("table")
    source = first[0]
    assert source.startswith(f"{MODEL}@v5+") and first[1] is None
    fingerprint = stage.model_fingerprint("table", first)
    # Merged again into the same path: its config and weights change.
    (merged / "config.json").write_text('{"a": 2}', encoding="utf-8")
    second = stage.model_state("table")
    assert second[0] != source
    assert stage.model_fingerprint("table", second) != fingerprint
    # Another checkpoint served under the same name.
    stub.cards = [_card(tmp_path / "elsewhere")]
    third = stage.model_state("table")
    assert third[0] not in (source, second[0])
    stub.cards = [_card(tmp_path / "elsewhere")]
    assert stage.model_state("table") == third


def test_the_role_source_recorded_is_the_checkpoint_s(env, stub, tmp_path):
    settings, catalog, ref = env
    stub.cards = [_card(tmp_path / "ckpt-a")]
    _, (done,) = _run(RolesStage(settings), settings, catalog, ref)
    source = done.payload["t2"]["analyses"][0]["metadata"]["set_role"]["role_source"]
    assert source == RolesStage(settings).model_state("table")[0]
    stub.cards = [_card(tmp_path / "ckpt-b")]
    assert RolesStage(settings).model_state("table")[0] != source


# --------------------------------------------------------------------------- the set's table


TABLE = ("<table><tr><th>Region</th><th>x</th><th>y</th><th>z</th></tr>"
         "<tr><td>Amygdala</td><td>1</td><td>2</td><td>3</td></tr></table>")


class _Ctx:
    def __init__(self, payloads, root):
        self.payloads, self.root = payloads, root

    def payload(self, artifact):
        return self.payloads.get(artifact.fingerprint)

    def recorded_path(self, path):
        return None if not path else (Path(path) if Path(path).is_absolute() else self.root / path)


def test_a_table_is_read_from_the_extraction_when_its_metadata_names_no_file(tmp_path):
    from ingestion_workflow.catalog import Artifact

    (tmp_path / "t2.html").write_text(TABLE, encoding="utf-8")
    triage = Artifact(article_id="a", stage="triage", status=Status.OK, fingerprint="tr",
                      summary={"source": "pubget"})
    extraction = Artifact(article_id="a", stage="extract", source="pubget", status=Status.OK,
                          fingerprint="ex")
    ctx = _Ctx({"ex": {"tables": [{"table_id": "t2", "raw_content_path": "t2.html"}]}}, tmp_path)
    found = raw_tables(triage, {"pubget": extraction}, ctx)
    assert found == {"t2": tmp_path / "t2.html"}
    ((*_, module, context),) = set_contexts(_tables("patients"), [], None, found)
    assert "[ROWS] Amygdala | 1 | 2 | 3" in module.serialize(context)
    ((*_, module, context),) = set_contexts(_tables("patients"), [], None)
    assert "[ROWS]" not in table_context.serialize(context)


def test_a_role_model_is_configured_per_origin_in_yaml_or_the_environment(tmp_path, monkeypatch):
    from ingestion_workflow.config import load_settings

    roots = {k: str(tmp_path / k) for k in ("data_root", "cache_root", "catalog_root")}
    config = tmp_path / "c.yaml"
    config.write_text(json.dumps({**roots, "role_model_prose": {
        "endpoint": "http://localhost:8351/v1", "model": "nx-roles-text"}}), encoding="utf-8")
    settings = load_settings(config)
    assert settings.role_model_prose == RoleModel(
        endpoint="http://localhost:8351/v1", model="nx-roles-text")
    assert settings.role_model_table is None
    monkeypatch.setenv("ROLE_MODEL_TABLE", json.dumps({"endpoint": "http://h/v1", "model": "t"}))
    assert Settings(**roots).role_model_table == RoleModel(endpoint="http://h/v1", model="t")


def test_a_role_model_is_loaded_one_way_only(env, stub, tmp_path):
    """A served model, by its endpoint and name: no model directory, device or threshold."""
    settings, _, _ = env
    assert [f for f in Settings.model_fields if f.startswith("role_")] == [
        "role_model_table", "role_model_prose"]
    assert set(RoleModel.model_fields) == {"endpoint", "model"}
    roots = {k: str(tmp_path / k) for k in ("data_root", "cache_root", "catalog_root")}
    for retired in ("/models/set-roles-table-1",
                    {"endpoint": stub.url, "model": MODEL, "result_threshold": 0.3}):
        with pytest.raises(ValueError, match="role_model_table"):
            Settings(**roots, role_model_table=retired)
    stage = RolesStage(settings)
    classifier = stage.classifier("table")
    assert type(classifier) is ServedClassifier
    assert (classifier.url, classifier.model, classifier.source) == (
        f"{stub.url}/chat/completions", MODEL, stage.model_state("table")[0])


def test_the_stage_reads_each_table_from_the_extraction_triage_judged(env, stub, tmp_path):
    settings, catalog, ref = env
    (tmp_path / "t2.html").write_text(TABLE, encoding="utf-8")
    catalog.record([
        Outcome(article_id=ref.id, stage="triage", source="", fingerprint="tr-1",
                payload={}, summary={"source": "pubget"}),
        Outcome(article_id=ref.id, stage="extract", source="pubget", fingerprint="ex-1",
                payload={"tables": [{"table_id": "t2",
                                     "raw_content_path": str(tmp_path / "t2.html")}]}),
    ])
    _, (done,) = _run(RolesStage(settings), settings, catalog, ref)
    assert done.status is Status.OK
    asked = [b["messages"][0]["content"] for b in stub.requests]
    assert len(asked) == 2 and all("[ROWS] Amygdala | 1 | 2 | 3" in a for a in asked)


def test_a_served_prose_model_trained_on_other_prose_contexts_is_refused(env, monkeypatch):
    settings, _, _ = env
    settings.prose_model = "nu-prose"
    settings.role_model_prose = settings.role_model_table.model_copy()
    for versions in ({"table": 2, "text": 3}, {"table": 2}):
        stale = served_prompt.RolePrompt("v5", "x", context_versions=versions)
        monkeypatch.setattr("ingestion_workflow.pipeline.stages.roles.PROMPT", stale)
        stage = RolesStage(settings)
        assert stage.model_state("table")[1] is None
        source, reason = stage.model_state("text")
        assert source is None
        assert f"trained on text context version {versions.get('text')}; this code builds 4" in (
            reason)


def test_every_table_read_without_its_header_and_rows_is_recorded_with_why(tmp_path):
    (tmp_path / "t2.html").write_text(TABLE, encoding="utf-8")
    payload = {
        "t2": _tables("stale path")["t2"],
        "t3": _tables("gone")["t2"],
        "t4": _tables("never recorded")["t2"],
    }
    payload["t2"]["analyses"][0]["metadata"]["table_metadata"]["raw_content_path"] = str(
        tmp_path / "moved.html")
    raw = {"t2": tmp_path / "t2.html", "t3": tmp_path / "gone.html"}
    backend = FakeBackend({"stale path": ("result", _probs(result=1.0)),
                           "gone": ("result", _probs(result=1.0)),
                           "never recorded": ("result", _probs(result=1.0))})
    unread = {}
    _, summary = assign_roles(payload, [], None, {"table": backend}, raw, unread)
    # A stale path in the metadata does not hide the extraction's file.
    assert "[ROWS] Amygdala | 1 | 2 | 3" in backend.seen[0]
    assert unread == {
        "t3": f"its raw table file is missing ({tmp_path / 'gone.html'})",
        "t4": "no raw table file is recorded for it; the extraction has no table 't4'",
    }
    unread = {}
    assign_roles(_tables("never recorded"), [], None, {"table": backend}, None, unread)
    assert unread == {"t2": "no raw table file is recorded for it"}


def test_the_stage_records_each_table_it_read_without_text(env):
    settings, catalog, ref = env
    _, (done,) = _run(RolesStage(settings), settings, catalog, ref)
    assert done.status is Status.OK
    assert done.summary["tables_without_text"] == {"t2": "no raw table file is recorded for it"}


def test_a_table_file_that_is_not_utf8_is_read_as_nu_v21_read_it(tmp_path):
    # nu-v21's reader drops undecodable bytes rather than replacing them.
    (tmp_path / "t2.html").write_bytes(TABLE.replace("Amygdala", "Amygdala\xe9").encode("latin-1"))
    ((*_, module, context),) = set_contexts(_tables("patients"), [], None,
                                            {"t2": tmp_path / "t2.html"})
    text = module.serialize(context)
    assert "[ROWS] Amygdala | 1 | 2 | 3" in text and "�" not in text


def test_a_server_up_but_failing_on_one_input_fails_that_set_only(stub):
    stub.reply = lambda body: ((500, "engine error") if "POISON" in str(body["messages"])
                               else (200, _choice('{"role": "result"}', RESULT_LIKELY)))
    classifier = _served(stub)
    poisoned, fine = classifier.predict(["POISON", "FINE"])
    assert isinstance(poisoned, Unanswered)
    assert poisoned.reason.startswith("server error on this input after 3 tries: HTTP 500")
    assert isinstance(fine, Prediction)
    assert isinstance(classifier.predict(["FINE AGAIN"])[0], Prediction)  # not stopped
    (after,) = classifier.predict(["FINE"])
    assert isinstance(after, Prediction)


def test_an_article_the_server_always_fails_on_does_not_starve_the_rest(env, stub, monkeypatch):
    settings, catalog, ref = env
    other = catalog.register(Identifier(pmid="8"))
    catalog.record([Outcome(article_id=other.id, stage="analyses", source="", fingerprint="an-2",
                            payload=_tables("fine"), summary={"tables": 1})])
    monkeypatch.setattr("ingestion_workflow.services.set_roles.served.time.sleep", lambda s: None)
    stub.reply = lambda body: ((500, "engine error") if "[NAME] Lee" in str(body["messages"])
                               else (200, _choice('{"role": "result"}', RESULT_LIKELY)))
    ctx = Context(settings, catalog)
    stage = RolesStage(settings)
    ids = [ref.id, other.id]
    planned = stage.plan(ctx, [ref, other], catalog.artifacts(ids, "roles"),
                         catalog.artifacts(ids, "analyses"))
    by_article = {o.article_id: o for o in stage.execute(ctx, planned.pending)}
    poisoned, fine = by_article[ref.id], by_article[other.id]
    assert poisoned.status is Status.FAILED
    assert "server error on this input" in poisoned.summary["unanswered"]["t2#1"]
    assert fine.status is Status.OK


def test_a_weights_only_re_merge_into_the_same_path_is_a_new_source(env, stub, tmp_path):
    settings, _, _ = env
    merged = tmp_path / "merged"
    merged.mkdir()
    (merged / "config.json").write_text('{"a": 1}', encoding="utf-8")
    weights = merged / "model-00001-of-00002.safetensors"
    weights.write_bytes(b"w1")
    stub.cards = [_card(merged)]
    stage = RolesStage(settings)
    before = stage.model_state("table")[0]
    # Same architecture: same config, same size; only the shard's write time changes.
    weights.write_bytes(b"w2")
    stat = weights.stat()
    os.utime(weights, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000_000))
    assert stage.model_state("table")[0] != before


def test_a_table_file_that_cannot_be_serialised_is_recorded_with_why(tmp_path, monkeypatch):
    from ingestion_workflow.services.create_analyses import CreateAnalysesService

    (tmp_path / "t2.html").write_text(TABLE, encoding="utf-8")

    def broken(markup, key):
        raise RuntimeError("bad markup")

    monkeypatch.setattr(CreateAnalysesService, "_serialise", staticmethod(broken))
    unread = {}
    list(set_contexts(_tables("patients"), [], None, {"t2": tmp_path / "t2.html"}, unread))
    assert unread["t2"].startswith(
        f"its raw table file cannot be read ({tmp_path / 't2.html'}): RuntimeError: bad markup")


# --------------------------------------------------------------------------- articles at once


TEXT_MODEL = "nx-roles-text"


def _prose_sets(*names):
    return {"p": {"analyses": [
        {"name": n, "description": None, "metadata": {"source": "prose", "passages": [0]},
         "coordinates": [{"x": 1.0, "y": 2.0, "z": 3.0, "space": "MNI"}]} for n in names]}}


@pytest.fixture
def both(tmp_path, evidence):
    """A table server and a text server, and a run with prose on that uses them."""
    table, text = Stub(), Stub(models=(TEXT_MODEL,))
    settings = Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c",
                        catalog_root=tmp_path / "k", ns_pond_root=tmp_path / "pond",
                        prose_model="nu-prose")
    settings.role_model_table = RoleModel(endpoint=table.url, model=MODEL)
    settings.role_model_prose = RoleModel(endpoint=text.url, model=TEXT_MODEL)
    with Catalog.open(settings.catalog_root, evidence=evidence) as catalog:
        yield settings, catalog, table, text
    table.close()
    text.close()


def _resolved(catalog, n, payload, passages=None):
    """An article whose resolve holds `payload`, with the prose and passages it was read from."""
    ref = catalog.register(Identifier(pmid=str(100 + n)))
    prose = "p" in (payload or {})
    summary = ({"tables": 0, "prose_analyses": 1, "prose_read": f"pr-{n}"} if prose
               else {"tables": 1, "prose_analyses": 0, "basis": f"an-{n}"})
    catalog.record([
        Outcome(article_id=ref.id, stage="resolve", source="", fingerprint=f"r-{n}",
                payload=payload, summary=summary),
        Outcome(article_id=ref.id, stage="prose", source="", fingerprint=f"pr-{n}",
                payload={}, summary={"passages_read": f"pa-{n}"}),
        Outcome(article_id=ref.id, stage="passages", source="", fingerprint=f"pa-{n}",
                payload=passages or {}),
    ])
    return ref


def _timed(stub, spans, hold):
    """Answer as `stub` does after `hold` seconds, recording when each request was in flight."""
    reply = stub.reply

    def timed(body):
        start = time.monotonic()
        time.sleep(hold(body) if callable(hold) else hold)
        spans.append((stub, start, time.monotonic()))
        return reply(body)

    stub.reply = timed


def _most_at_once(spans):
    edges = sorted([(s, 1) for s, _ in spans] + [(e, -1) for _, e in spans])
    most = now = 0
    for _, step in edges:
        now += step
        most = max(most, now)
    return most


def test_the_table_and_text_servers_are_asked_at_the_same_time(both):
    """Articles are classified together, so neither server waits on the other's articles."""
    settings, catalog, table, text = both
    refs = [_resolved(catalog, n, _tables("patients", "controls") if n % 2 else
                      _prose_sets("seed"))
            for n in range(12)]
    text.reply = lambda body: (200, _choice('{"role": "anchor", "anchor_kind": "seed"}',
                                            {"anchor": math.log(0.9), "result": math.log(0.1)}))
    spans = []
    _timed(table, spans, 0.1)
    _timed(text, spans, 0.1)
    stage = RolesStage(settings)
    ctx = Context(settings, catalog)
    ids = [r.id for r in refs]
    plan = stage.plan(ctx, refs, catalog.artifacts(ids, "roles"),
                      catalog.artifacts(ids, "resolve"))
    assert len(plan.pending) == 12
    listed = (table.listings, text.listings)
    outcomes = list(stage.execute(ctx, plan.pending))
    assert sorted(o.article_id for o in outcomes) == sorted(ids)
    assert {o.status for o in outcomes} == {Status.OK}
    # Each classifier is loaded once, by one article, however many ask for it at once.
    assert (table.listings, text.listings) == (listed[0] + 1, listed[1] + 1)
    by_server = {s: [(a, b) for k, a, b in spans if k is s] for s in (table, text)}
    # More than one article's sets at a server at once, and the two servers busy together.
    assert _most_at_once(by_server[table]) > 2 and _most_at_once(by_server[text]) > 1
    assert any(a < d and c < b for a, b in by_server[table] for c, d in by_server[text])


def _planned(stage, settings, catalog, refs):
    ctx = Context(settings, catalog)
    ids = [r.id for r in refs]
    plan = stage.plan(ctx, refs, catalog.artifacts(ids, "roles"),
                      catalog.artifacts(ids, "resolve"))
    assert len(plan.pending) == len(refs)
    return ctx, plan.pending


def test_a_server_is_asked_32_sets_at_once_across_every_article(both):
    """The cap is the server's, shared by the articles under way, not each article's."""
    settings, catalog, table, _ = both
    refs = [_resolved(catalog, n, _tables(*[f"set {i}" for i in range(10)])) for n in range(8)]
    spans = []
    _timed(table, spans, 0.3)
    stage = RolesStage(settings)
    outcomes = list(stage.execute(*_planned(stage, settings, catalog, refs)))
    assert {o.status for o in outcomes} == {Status.OK} and len(outcomes) == 8
    assert _most_at_once([(a, b) for _, a, b in spans]) == 32


def test_no_more_than_64_articles_are_under_way(both, monkeypatch):
    # The server's cap above the window, so the sets in flight are the articles under way.
    monkeypatch.setattr(served_module, "WORKERS", 100)
    settings, catalog, table, _ = both
    refs = [_resolved(catalog, n, _tables("patients")) for n in range(70)]
    spans = []
    _timed(table, spans, 1.0)
    stage = RolesStage(settings)
    outcomes = list(stage.execute(*_planned(stage, settings, catalog, refs)))
    assert len(outcomes) == 70
    assert _most_at_once([(a, b) for _, a, b in spans]) == 64


def test_an_interrupt_stops_the_stage_at_once_and_sends_no_other_set(both):
    """Ctrl-C leaves while sets are held at the server; the queued ones are never sent."""
    settings, catalog, table, _ = both
    previous = signal.signal(signal.SIGINT, signal.default_int_handler)
    refs = [_resolved(catalog, n, _tables("patients", "controls")) for n in range(40)]
    release, reply = threading.Event(), table.reply

    def held(body):
        release.wait(10)
        return reply(body)

    table.reply = held
    stage = RolesStage(settings)
    ctx, works = _planned(stage, settings, catalog, refs)

    def interrupt():
        while len(table.requests) < 32:
            time.sleep(0.01)
        os.kill(os.getpid(), signal.SIGINT)

    threading.Thread(target=interrupt, daemon=True).start()
    began = time.monotonic()
    try:
        with pytest.raises(KeyboardInterrupt):
            list(stage.execute(ctx, works))
        took = time.monotonic() - began
    finally:
        signal.signal(signal.SIGINT, previous)
    sent = len(table.requests)
    release.set()
    time.sleep(0.5)  # the held sets are answered, and the threads asking them are free
    assert took < 5, f"the interrupt waited {took:.1f}s for the window"
    assert sent == len(table.requests) == 32


def test_an_outage_starts_no_article_and_keeps_those_ahead_of_it(both, monkeypatch):
    """Once a model stops the stage no article is begun; those ahead that finish are kept."""
    settings, catalog, table, text = both
    monkeypatch.setattr(roles_module, "ARTICLES", 3)
    refs = [_resolved(catalog, 0, _prose_sets("first")),
            _resolved(catalog, 1, _prose_sets("second")),
            _resolved(catalog, 2, _tables("refused")),
            _resolved(catalog, 3, _prose_sets("late"))]
    table.reply = lambda body: TOO_LONG
    text.reply = lambda body: (200, _choice('{"role": "result"}', RESULT_LIKELY))
    spans = []
    _timed(text, spans, lambda body: 0.5 if "[NAME] first" in str(body["messages"]) else 1)
    ids = [r.id for r in refs]
    report = run_stages(Context(settings, catalog), [RolesStage(settings)], refs)
    roles = report.stages["roles"]
    assert "maximum context length is 4096 tokens" in roles.stopped
    assert (roles.ok, roles.failed, roles.left_pending) == (2, 0, 2)
    # Room in the window once `first` was done, but `late` was not begun: the table model stopped.
    assert not any("[NAME] late" in str(b["messages"]) for b in text.requests)
    assert catalog.artifacts(ids, "roles").keys() == {ids[0], ids[1]}
    assert catalog.attempt_counts(ids, "roles", "") in ({}, {i: (0, None) for i in ids})


def test_an_article_s_failure_among_many_at_once_fails_that_article_only(both, monkeypatch):
    settings, catalog, table, text = both
    monkeypatch.setattr("ingestion_workflow.services.set_roles.served.time.sleep", lambda s: None)
    refs = [_resolved(catalog, n, _tables("poison" if n == 3 else "patients")) for n in range(8)]
    table.reply = lambda body: ((500, "engine error") if "[NAME] poison" in str(body["messages"])
                                else (200, _choice('{"role": "result"}', RESULT_LIKELY)))
    ids = [r.id for r in refs]
    report = run_stages(Context(settings, catalog), [RolesStage(settings)], refs)
    roles = report.stages["roles"]
    assert (roles.ok, roles.failed, roles.stopped) == (7, 1, None)
    found = catalog.artifacts(ids, "roles")
    assert [found[i][""].status for i in ids] == [Status.FAILED if n == 3 else Status.OK
                                                 for n in range(8)]


def test_an_outage_deep_in_the_run_starts_no_later_article(both):
    """The outage is the 101st of 200 articles: no article is started once it is met.

    After it, every other article is prose: one started then would be sent to
    the text server, which is still up.
    """
    settings, catalog, table, text = both
    refs = [_resolved(catalog, n, _tables("refused") if n == 100
                      else _prose_sets(f"late {n}") if n > 100 and n % 2
                      else _tables(f"set {n}"))
            for n in range(200)]
    answered, arrived, refused_at = table.reply, [], []

    def reply(body):
        arrived.append((time.monotonic(), body))
        if "[NAME] refused" in str(body["messages"]):
            refused_at.append(time.monotonic())
            return TOO_LONG
        return answered(body)

    table.reply = reply
    _timed(text, [], 0.05)
    ids = [r.id for r in refs]
    report = run_stages(Context(settings, catalog), [RolesStage(settings)], refs)
    roles = report.stages["roles"]
    assert "maximum context length is 4096 tokens" in roles.stopped
    # 101st started only once no more than 63 were under way: 37 or more were done, and kept.
    assert roles.ok >= 37 and roles.failed == 0 and roles.ok + roles.left_pending == 200
    kept = catalog.artifacts(ids, "roles")
    assert ids[100] not in kept and len(kept) == roles.ok
    # Before the refusal was read, at most the 63 under way with it finished, so at most
    # 63 more were started: none from the 164th on.
    asked = {json.dumps(b["messages"]) for b in table.requests + text.requests}
    assert not any(f"[NAME] {kind} {n}" in a
                   for a in asked for kind in ("set", "late") for n in range(164, 200))
    (stop,) = refused_at
    assert not [t for t, _ in arrived if t > stop + 0.2]


def test_a_model_loaded_after_a_stop_sends_nothing(both):
    """An article under way that first needs the other origin's model after a stop is not sent."""
    settings, catalog, table, text = both
    refused = _resolved(catalog, 0, _tables("refused"))
    prose = _resolved(catalog, 1, _prose_sets("seed"))
    table.reply = lambda body: TOO_LONG
    stage = RolesStage(settings)
    with pytest.raises(ModelUnavailable, match="maximum context length"):
        list(stage.execute(*_planned(stage, settings, catalog, [refused])))
    ctx, works = _planned(stage, settings, catalog, [prose])
    listed = text.listings
    with pytest.raises(ModelUnavailable, match="maximum context length"):
        list(stage.execute(ctx, works))
    assert (text.requests, text.listings) == ([], listed)


def test_an_interrupt_while_a_model_is_loading_sends_it_nothing(both):
    """Ctrl-C while the text model's listing is awaited: once loaded, it is already stopped."""
    settings, catalog, _, text = both
    stage = RolesStage(settings)
    ctx, works = _planned(stage, settings, catalog, [_resolved(catalog, 0, _prose_sets("seed"))])
    listed, text.gate = text.listings, threading.Event()
    previous = signal.signal(signal.SIGINT, signal.default_int_handler)

    def interrupt():
        while text.listings == listed:
            time.sleep(0.01)
        os.kill(os.getpid(), signal.SIGINT)

    threading.Thread(target=interrupt, daemon=True).start()
    try:
        with pytest.raises(KeyboardInterrupt):
            list(stage.execute(ctx, works))
    finally:
        signal.signal(signal.SIGINT, previous)
    text.gate.set()
    time.sleep(0.5)  # the model is loaded, and its article's set would have been asked
    assert text.requests == []


def test_a_failed_article_does_not_stop_the_others_from_starting(both, monkeypatch):
    settings, catalog, table, _ = both
    monkeypatch.setattr(roles_module, "ARTICLES", 2)
    monkeypatch.setattr("ingestion_workflow.services.set_roles.served.time.sleep", lambda s: None)
    refs = [_resolved(catalog, n, _tables("poison" if n == 0 else f"set {n}")) for n in range(6)]
    table.reply = lambda body: ((500, "engine error") if "[NAME] poison" in str(body["messages"])
                                else (200, _choice('{"role": "result"}', RESULT_LIKELY)))
    stage = RolesStage(settings)
    outcomes = {o.article_id: o.status for o in
                stage.execute(*_planned(stage, settings, catalog, refs))}
    assert outcomes == {r.id: Status.FAILED if n == 0 else Status.OK for n, r in enumerate(refs)}


def test_each_way_an_article_fails_leaves_the_others_decided(both):
    """An analysis that is not one, a passages text that changed, an upstream payload gone."""
    settings, catalog, _, _ = both
    refs = [_resolved(catalog, n, _tables("patients")) for n in range(6)]
    broken = [
        _resolved(catalog, 6, {"t2": {"analyses": ["not a set"]}}),
        _resolved(catalog, 7, _tables("patients"),
                  passages={"passages": [{"span": [0, 4]}], "full_text_path": "/gone.txt"}),
        _resolved(catalog, 8, None),
    ]
    stage = RolesStage(settings)
    ctx, works = _planned(stage, settings, catalog, refs[:3] + broken + refs[3:])
    outcomes = {o.article_id: o for o in stage.execute(ctx, works)}
    assert {outcomes[r.id].status for r in refs} == {Status.OK}
    not_a_set, changed, gone = (outcomes[r.id] for r in broken)
    assert {o.status for o in (not_a_set, changed, gone)} == {Status.FAILED}
    assert not_a_set.error.startswith("AttributeError: ")
    assert changed.error == "LookupError: the text the passages index is gone or has changed"
    assert gone.error == "the resolve payload is gone"
    assert gone.fingerprint == next(w.fingerprint for w in works if w.article_id == broken[2].id)
