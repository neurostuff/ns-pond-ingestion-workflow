"""The roles stage's classifier interface, and its served NuExtract3 backend on a stub server."""

from __future__ import annotations

import hashlib
import json
import math
import os
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
from ingestion_workflow.catalog import Catalog, Outcome, Status
from ingestion_workflow.config import ServedRoleModel, Settings
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.pipeline import Context
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
from ingestion_workflow.services.set_roles.served import (
    RoleModelUnavailable,
    ServedClassifier,
    role_distribution,
)

MODEL = "nx-roles-table"

# --------------------------------------------------------------------------- the interface


class FakeBackend:
    """A RoleClassifier answering from the set's name, as a served model would."""

    def __init__(self, answers, source="fake@1", role_thresholds=None):
        self.answers = answers
        self.source = source
        self.role_thresholds = role_thresholds or {}
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
            out.append(Prediction(None, probabilities, {}, None, answer=role,
                                  answer_kind=kind[0] if kind else None))
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


def test_a_result_threshold_turns_a_likely_result_into_a_result():
    anchor = Prediction(None, _probs(result=0.4, anchor=0.6), {"roi": 1.0}, None,
                        answer="anchor", answer_kind="roi")
    assert decide(anchor, source="m@v5", origin="table").role == "anchor"
    lowered = decide(anchor, source="m@v5", origin="table", role_thresholds={"result": 0.3})
    assert (lowered.role, lowered.anchor_kind, lowered.confidence) == ("result", None, 0.4)
    kept = decide(anchor, source="m@v5", origin="table", role_thresholds={"result": 0.5})
    assert (kept.role, kept.anchor_kind, kept.confidence) == ("anchor", "roi", 0.6)


def test_the_threshold_never_takes_a_result_away():
    result = Prediction(None, _probs(result=0.2, other=0.8), {}, None, answer="result")
    decided = decide(result, source="m@v5", origin="text", role_thresholds={"result": 0.9})
    assert (decided.role, decided.confidence) == ("result", 0.2)


def test_a_model_without_coordinates_or_prior_answers_calls_only_a_quote_another_study_s():
    reference = Prediction(None, _probs(reference=0.9, result=0.1), {}, None, answer="reference")
    result = Prediction(None, _probs(result=1.0), {}, None, answer="result")
    assert decide(reference, source="m", origin="table").from_prior_study is True
    decided = decide(result, source="m", origin="table")
    assert (decided.role, decided.from_prior_study) == ("result", False)


def test_the_stage_records_the_backend_s_name_its_p_result_and_its_threshold():
    backend = FakeBackend(
        {"patients": ("result", _probs(result=0.97, other=0.03)),
         "Lee": ("reference", _probs(result=0.35, reference=0.65))},
        source="nx-roles-table@v5", role_thresholds={"result": 0.3})
    out, summary = assign_roles(_tables("patients", "Lee"), [], None, {"table": backend})
    first, second = (a["metadata"]["set_role"] for a in out["t2"]["analyses"])
    assert (first["role"], first["role_confidence"], first["role_source"]) == (
        "result", 0.97, "nx-roles-table@v5")
    assert (second["role"], second["role_confidence"]) == ("result", 0.35)
    assert summary["sources"] == {"table": "nx-roles-table@v5"}


def test_a_set_without_a_usable_answer_gives_the_article_no_roles_and_says_why():
    backend = FakeBackend({"patients": ("result", _probs(result=1.0)),
                           "Lee": Unanswered("the answer is not JSON: 'oops'")})
    with pytest.raises(UnansweredSets) as raised:
        assign_roles(_tables("patients", "Lee"), [], None, {"table": backend})
    assert raised.value.reasons == {"t2#1": "the answer is not JSON: 'oops'"}
    assert "t2#1: the answer is not JSON" in str(raised.value)


# --------------------------------------------------------------------------- the prompt


#: sha256 of the instruction each version's models were trained and scored with; their
#: evaluation's metrics record the v5 hash.
TRAINED = {
    "v4": "aa81d460bfa1963f47133e2b7a3b60e73391071773c6d37d5bc4c729af9b9e94",
    "v5": "531886df6f1c9575a0fce38163d7bd3cf1731272852be00b6e0e16a40fccff5f",
}
TEMPLATE_SHA = "7499daa7039b6f20b92fc29f6a278a60a1ab80cf8f51f3a824cc25810174e8f1"
SCHEMA_SHA = "35d00fbf6b4e35a276a3c0771b179f6e8ba3c6416b1b9df5cdc108db0035e6eb"


def _sha(text):
    return hashlib.sha256(text.encode()).hexdigest()


@pytest.mark.parametrize("version", sorted(TRAINED))
def test_each_prompt_is_the_one_its_models_were_trained_with(version):
    prompt = served_prompt.prompt(version)
    assert _sha(prompt.instruction) == TRAINED[version]
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
    with pytest.raises(ValueError, match="no role prompt version 'v9'"):
        served_prompt.prompt("v9")


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


def _choice(content, alternatives=None):
    logprobs = {"content": _logprobs(alternatives)} if alternatives else None
    return {"choices": [{"message": {"content": content}, "logprobs": logprobs}]}


#: Role tokens holding 0.8 of the mass, and "re", which begins two roles, 0.15.
RESULT_LIKELY = {"result": math.log(0.6), "anchor": math.log(0.1), "re": math.log(0.15),
                 "other": math.log(0.1)}


class Stub:
    """An OpenAI-compatible server: `/v1/models` lists `models`; `reply(body)` answers a chat."""

    def __init__(self, models=(MODEL,)):
        self.models = list(models)
        #: The cards `/v1/models` lists; by default one per name, as vLLM writes them.
        self.cards = None
        #: False: the model listing answers 503 too, as a server that is down does.
        self.healthy = True
        self.requests = []
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

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
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
    return ServedClassifier(stub.url, MODEL, served_prompt.prompt("v5"), **kw)


def test_each_set_is_asked_exactly_as_the_model_was_trained_and_scored(stub):
    prompt = served_prompt.prompt("v5")
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


def test_the_threshold_applies_to_the_served_answer_through_the_stage(stub):
    stub.reply = lambda body: (200, _choice(
        '{"role": "anchor", "anchor_kind": "seed"}',
        {"anchor": math.log(0.55), "result": math.log(0.45)}))
    plain = assign_roles(_tables("seed"), [], None, {"table": _served(stub)})[0]
    lowered = assign_roles(_tables("seed"), [], None,
                           {"table": _served(stub, result_threshold=0.4)})[0]
    plain_role = plain["t2"]["analyses"][0]["metadata"]["set_role"]
    lowered_role = lowered["t2"]["analyses"][0]["metadata"]["set_role"]
    assert (plain_role["role"], plain_role["anchor_kind"]) == ("anchor", "seed")
    assert plain_role["role_confidence"] == pytest.approx(0.55)
    assert (lowered_role["role"], lowered_role["anchor_kind"]) == ("result", None)
    assert lowered_role["role_confidence"] == pytest.approx(0.45)
    assert lowered_role["role_source"] == f"{MODEL}@v5"


@pytest.mark.parametrize("content, alternatives, why", [
    ("not json", RESULT_LIKELY, "not JSON"),
    ('["result"]', RESULT_LIKELY, "not an object"),
    ('{"role": "banana"}', RESULT_LIKELY, "role 'banana' is not one of"),
    ('{"role": "anchor", "anchor_kind": null}', RESULT_LIKELY, "an anchor with anchor_kind None"),
    ('{"role": "result"}', None, "no role-token logprobs"),
])
def test_an_answer_that_cannot_be_read_is_recorded_never_given_a_default(
    stub, content, alternatives, why
):
    stub.reply = lambda body: (200, _choice(content, alternatives))
    (answer,) = _served(stub).predict(["x"])
    assert isinstance(answer, Unanswered) and why in answer.reason


def test_a_refused_input_is_that_set_s_reason_and_the_others_are_answered(stub):
    stub.reply = lambda body: ((400, {"error": "too long"}) if "LONGSET" in str(body["messages"])
                               else (200, _choice('{"role": "result"}', RESULT_LIKELY)))
    short, long_ = _served(stub).predict(["SHORTSET", "LONGSET"])
    assert isinstance(short, Prediction)
    assert isinstance(long_, Unanswered) and "too long" in long_.reason


def test_a_5xx_or_a_timeout_is_asked_again(stub):
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
    (answer,) = _served(stub, timeout=0.2).predict(["x"])
    assert isinstance(answer, Prediction) and len(stub.requests) == 5


def test_a_server_that_never_answers_stops_the_run_and_is_not_asked_again(stub):
    stub.reply = lambda body: (503, "down")
    stub.healthy = False
    classifier = _served(stub, attempts=3)
    with pytest.raises(RoleModelUnavailable, match="did not answer in 3 tries: HTTP 503"):
        classifier.predict(["x"])
    asked = len(stub.requests)
    assert asked == 3
    with pytest.raises(RoleModelUnavailable):
        classifier.predict(["y"])
    assert len(stub.requests) == asked


# --------------------------------------------------------------------------- the stage


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
def env(tmp_path, stub):
    settings = Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c",
                        catalog_root=tmp_path / "k", ns_pond_root=tmp_path / "pond")
    settings.role_served_table = ServedRoleModel(endpoint=stub.url, model=MODEL,
                                                 prompt_version="v5", attempts=2)
    with Catalog.open(settings.catalog_root) as catalog:
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
    (lambda s, stub: setattr(s.role_served_table, "endpoint",
                             f"http://127.0.0.1:{_free_port()}/v1"),
     "cannot be reached: ConnectError"),
    (lambda s, stub: setattr(s.role_served_table, "prompt_version", "v9"),
     "cannot be used: no role prompt version 'v9'"),
    (lambda s, stub: setattr(s, "role_model_table", Path("/models/table")),
     "both role_model_table and role_served_table are set"),
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
    stale = served_prompt.RolePrompt("v5", served_prompt.PROMPTS["v5"].instruction,
                                     context_versions={"table": 1, "text": 4})
    monkeypatch.setitem(served_prompt.PROMPTS, "v5", stale)
    plan, outcomes = _run(RolesStage(settings), settings, catalog, ref)
    assert (plan.blocked, outcomes) == (1, [])
    (said,) = plan.reasons
    assert "trained on table context version 1; this code builds 2; retrain them" in said


def test_the_threshold_is_part_of_the_fingerprint(env):
    settings, _, _ = env
    stage = RolesStage(settings)
    before = stage.model_fingerprint("table", stage.model_state("table"))
    settings.role_served_table.result_threshold = 0.3
    assert stage.model_fingerprint("table", stage.model_state("table")) != before


def test_an_unreadable_answer_fails_the_article_with_each_set_s_reason(env, stub):
    settings, catalog, ref = env
    stub.reply = lambda body: ((200, _choice("oops")) if "[NAME] Lee" in str(body["messages"])
                               else (200, _choice('{"role": "result"}', RESULT_LIKELY)))
    _, (failed,) = _run(RolesStage(settings), settings, catalog, ref)
    assert failed.status is Status.FAILED and failed.payload is None
    assert failed.summary["unanswered"] == {"t2#1": "the answer is not JSON: 'oops'"}
    assert "t2#1: the answer is not JSON" in failed.error


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

    stub.reply = crashed
    ctx = Context(settings, catalog)
    refs, ids = [ref, other], [ref.id, other.id]

    def plan(stage):
        stub.healthy = True
        return stage.plan(ctx, refs, catalog.artifacts(ids, "roles"),
                          catalog.artifacts(ids, "analyses"))

    for _ in range(ctx.max_attempts + 1):
        stage = RolesStage(settings)
        planned = plan(stage)
        assert len(planned.pending) == 2
        outcomes = list(stage.execute(ctx, planned.pending))
        assert outcomes == []
        catalog.record(outcomes)
        # The rest of the run is blocked with the reason, not attempted.
        again = plan(stage)
        assert (again.blocked, again.pending) == (2, [])
        (said,) = again.reasons
        assert "stopped answering in this run" in said and "HTTP 503" in said
    assert catalog.attempt_counts(ids, "roles", "") in ({}, {i: (0, None) for i in ids})
    stub.reply = lambda body: (200, _choice('{"role": "result"}', RESULT_LIKELY))
    stage = RolesStage(settings)
    planned = plan(stage)
    assert (len(planned.pending), planned.permanent) == (2, 0)
    assert {o.status for o in stage.execute(ctx, planned.pending)} == {Status.OK}


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


def test_a_served_model_is_configured_per_origin_in_yaml_or_the_environment(tmp_path, monkeypatch):
    from ingestion_workflow.config import load_settings

    roots = {k: str(tmp_path / k) for k in ("data_root", "cache_root", "catalog_root")}
    config = tmp_path / "c.yaml"
    config.write_text(json.dumps({**roots, "role_served_prose": {
        "endpoint": "http://localhost:8351/v1", "model": "nx-roles-text",
        "prompt_version": "v5", "result_threshold": 0.3}}), encoding="utf-8")
    settings = load_settings(config)
    assert settings.role_served_prose == ServedRoleModel(
        endpoint="http://localhost:8351/v1", model="nx-roles-text", prompt_version="v5",
        result_threshold=0.3)
    assert settings.role_served_table is None
    monkeypatch.setenv("ROLE_SERVED_TABLE", json.dumps(
        {"endpoint": "http://h/v1", "model": "t", "prompt_version": "v4"}))
    assert Settings(**roots).role_served_table.prompt_version == "v4"
    with pytest.raises(ValueError, match="result_threshold"):
        ServedRoleModel(endpoint="e", model="m", prompt_version="v5", result_threshold=1.5)


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


def test_a_probability_equal_to_the_threshold_is_a_result():
    anchor = Prediction(None, _probs(result=0.25, anchor=0.75), {"roi": 1.0}, None,
                        answer="anchor", answer_kind="roi")
    assert decide(anchor, source="m", origin="table", role_thresholds={"result": 0.25}).role == (
        "result")


def test_a_served_prose_model_trained_on_other_prose_contexts_is_refused(env, monkeypatch):
    settings, _, _ = env
    settings.prose_model = "nu-prose"
    settings.role_served_prose = settings.role_served_table.model_copy()
    for versions in ({"table": 2, "text": 3}, {"table": 2}):
        stale = served_prompt.RolePrompt("v5", "x", context_versions=versions)
        monkeypatch.setitem(served_prompt.PROMPTS, "v5", stale)
        stage = RolesStage(settings)
        assert stage.model_state("table")[1] is None
        source, reason = stage.model_state("text")
        assert source is None
        assert f"trained on text context version {versions.get('text')}; this code builds 4" in (
            reason)


@pytest.mark.parametrize("content, alternatives", [
    ("{}", RESULT_LIKELY),
    ('{"role": null}', RESULT_LIKELY),
    # Every alternative at the role token begins two roles, or none.
    ('{"role": "result"}', {"re": math.log(0.7), '"': math.log(0.3)}),
])
def test_a_missing_role_or_role_probability_is_never_filled_in(stub, content, alternatives):
    stub.reply = lambda body: (200, _choice(content, alternatives))
    (answer,) = _served(stub).predict(["x"])
    assert isinstance(answer, Unanswered)


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
    classifier = _served(stub, attempts=2)
    poisoned, fine = classifier.predict(["POISON", "FINE"])
    assert isinstance(poisoned, Unanswered)
    assert poisoned.reason.startswith("server error on this input after 2 tries: HTTP 500")
    assert isinstance(fine, Prediction) and classifier.down is None
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
    poisoned, fine = stage.execute(ctx, planned.pending)
    assert (poisoned.article_id, poisoned.status) == (ref.id, Status.FAILED)
    assert "server error on this input" in poisoned.summary["unanswered"]["t2#1"]
    assert (fine.article_id, fine.status) == (other.id, Status.OK)


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
