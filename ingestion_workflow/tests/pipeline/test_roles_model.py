"""The roles stage with its decision model: loaded once, shared by every article under way."""

from __future__ import annotations

import json
import os
import signal
import threading
import time
from pathlib import Path

import pytest
from ingestion_workflow.catalog import Catalog, Outcome, Status
from ingestion_workflow.clients.model_server import ModelUnavailable
from ingestion_workflow.config import RoleModel, Settings
from ingestion_workflow.models.ids import Identifier
from ingestion_workflow.pipeline import Context
from ingestion_workflow.pipeline.scheduler import run_stages
from ingestion_workflow.pipeline.stages import roles as roles_module
from ingestion_workflow.pipeline.stages.roles import (
    RolesStage,
    assign_roles,
    raw_tables,
    set_contexts,
)
from ingestion_workflow.services.set_roles import (
    COORDINATE_ROLES,
    Prediction,
    decision,
    table_context,
)

torch = pytest.importorskip("torch")

from ingestion_workflow.tests.decision_model import write_model  # noqa: E402

RESULT = {"coordinates": True, "space": "template", "role": "result", "source": "this_study"}


def _analysis(name, caption="", **meta):
    return {
        "name": name,
        "table_caption": caption,
        "table_footer": "",
        "description": None,
        "metadata": {"table_metadata": {"table_label": "Table 2"}, **meta},
        "coordinates": [{"x": -42.0, "y": 18.0, "z": 24.0, "space": "MNI"}],
    }


def _tables(*names):
    return {"t2": {"slug": "a::t2", "coordinate_space": "MNI", "identifier": None,
                   "analyses": [_analysis(n) for n in names]}}


def _prose_sets(*names):
    return {"p": {"analyses": [
        {"name": n, "description": None, "metadata": {"source": "prose", "passages": [0]},
         "coordinates": [{"x": -42.0, "y": 18.0, "z": 24.0, "space": "MNI"}]} for n in names]}}


def _result():
    return Prediction("result", None, {r: float(r == "result") for r in COORDINATE_ROLES},
                      "template", "this_study", 1.0)


class Recorder:
    """A classifier that answers `result` for every set and keeps what it read."""

    source = "recorder@1"

    def __init__(self):
        self.seen = []

    def predict(self, texts):
        self.seen += texts
        return [_result() for _ in texts]


# --------------------------------------------------------------------------- the real model


@pytest.fixture
def model_dir(tmp_path):
    return write_model(tmp_path / "model", RESULT)


@pytest.fixture
def env(tmp_path, model_dir, evidence):
    settings = Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c",
                        catalog_root=tmp_path / "k", ns_pond_root=tmp_path / "pond",
                        role_model=RoleModel(path=str(model_dir), device="cpu"))
    with Catalog.open(settings.catalog_root) as catalog:
        ref = catalog.register(Identifier(pmid="7"), evidence)
        catalog.record([Outcome(article_id=ref.id, stage="analyses", source="", fingerprint="an-1",
                                payload=_tables("patients", "Lee"), summary={"tables": 1})])
        yield settings, catalog, ref


def _run(stage, settings, catalog, ref):
    ctx = Context(settings, catalog)
    plan = stage.plan(ctx, [ref], catalog.artifacts([ref.id], stage.name),
                      catalog.artifacts([ref.id], stage.requires))
    return plan, list(stage.execute(ctx, plan.pending))


def test_the_configured_model_decides_every_set(env, model_dir):
    settings, catalog, ref = env
    stage = RolesStage(settings)
    source, reason = stage.model_state()
    assert reason is None and source.startswith("role-test@v1+")
    plan, (done,) = _run(stage, settings, catalog, ref)
    assert (plan.blocked, done.status) == (0, Status.OK)
    roles = [a["metadata"]["set_role"] for a in done.payload["t2"]["analyses"]]
    assert {r["role_source"] for r in roles} == {source}
    assert [(r["role"], r["space"], r["coordinate_source"], r["from_prior_study"])
            for r in roles] == [
        ("result", "template", "this_study", False)] * 2
    assert done.summary["model"] == source
    assert type(stage.classifier()) is decision.DecisionModel


@pytest.mark.parametrize("change, reason", [
    (lambda root: (root / "decision.json").unlink(), "has no decision.json"),
    (lambda root: write_model(root, RESULT, context_versions={"table": 1, "text": 4}),
     "it was trained on context versions table 1, and this code builds table 2"),
])
def test_a_model_that_cannot_be_used_blocks_the_article_at_plan_time(env, model_dir, change,
                                                                      reason):
    settings, catalog, ref = env
    change(model_dir)
    plan, outcomes = _run(RolesStage(settings), settings, catalog, ref)
    assert (plan.blocked, plan.pending, outcomes) == (1, [], [])
    (said,) = plan.reasons
    assert said.startswith(f"the role model at {model_dir}") and reason in said


def test_a_model_that_cannot_be_loaded_stops_the_stage_and_spends_no_retry(
    env, model_dir, evidence
):
    """Its card is fine when the stage is planned, its weights are not when it is loaded."""
    settings, catalog, ref = env
    (model_dir / "encoder" / "model.safetensors").write_bytes(b"truncated")
    ctx = Context(settings, catalog)
    for _ in range(ctx.max_attempts + 1):
        report = run_stages(ctx, [RolesStage(settings)], [ref])
        assert f"the role model at {model_dir} cannot be loaded" in report.stages["roles"].stopped
        assert catalog.artifacts([ref.id], "roles") == {}
    assert catalog.attempt_counts([ref.id], "roles", "") in ({}, {ref.id: (0, None)})
    write_model(model_dir, RESULT)
    report = run_stages(ctx, [RolesStage(settings)], [ref])
    assert (report.stages["roles"].ok, report.stages["roles"].stopped) == (1, None)


def test_a_set_the_model_puts_in_another_frame_is_held_and_written_as_other_space(
    env, model_dir
):
    settings, catalog, ref = env
    write_model(model_dir, {**RESULT, "space": "other", "role": "anchor", "kind": "seed"})
    _, (done,) = _run(RolesStage(settings), settings, catalog, ref)
    first = done.payload["t2"]["analyses"][0]["metadata"]
    assert (first["set_role"]["role"], first["set_role"]["space"], first["held"]) == (
        "anchor", "other", True)
    assert (done.summary["held"], done.summary["other_space"]) == (2, 2)


def test_a_role_model_is_one_directory_configured_in_yaml_or_the_environment(tmp_path,
                                                                            monkeypatch):
    from ingestion_workflow.config import load_settings

    roots = {k: str(tmp_path / k) for k in ("data_root", "cache_root", "catalog_root")}
    config = tmp_path / "c.yaml"
    config.write_text(json.dumps({**roots, "role_model": {"path": "/models/role/v1"}}),
                      encoding="utf-8")
    assert load_settings(config).role_model == RoleModel(path="/models/role/v1")
    monkeypatch.setenv("ROLE_MODEL", json.dumps({"path": "/m", "device": "cuda:1"}))
    assert Settings(**roots).role_model == RoleModel(path="/m", device="cuda:1")
    assert [f for f in Settings.model_fields if f.startswith("role_")] == ["role_model"]
    # The served models' settings are gone, and refused rather than ignored.
    monkeypatch.delenv("ROLE_MODEL")
    with pytest.raises(ValueError, match="role_model"):
        Settings(**roots, role_model={"endpoint": "http://h/v1", "model": "role-table"})


def test_a_retrained_model_in_the_same_directory_is_a_new_source(env, model_dir):
    settings, _, _ = env
    stage = RolesStage(settings)
    first = stage.model_state()
    fingerprints = stage.models()
    write_model(model_dir, {**RESULT, "role": "anchor"})
    assert stage.model_state()[0] != first[0]
    assert all(stage.models()[o] != fingerprints[o] for o in fingerprints)
    # Within `remembering()` the directory is read once, as upload's gate plans.
    with stage.remembering():
        held = stage.model_state()
        write_model(model_dir, RESULT, version="v2")
        assert stage.model_state() == held
    assert stage.model_state()[0].startswith("role-test@v2+")


# --------------------------------------------------------------------------- the set's table


TABLE = ("<table><tr><th>Region</th><th>x</th><th>y</th><th>z</th></tr>"
         "<tr><td>Amygdala</td><td>-42</td><td>18</td><td>24</td></tr></table>")


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
    assert "[ROWS] Amygdala | -42 | 18 | 24" in module.serialize(context)
    ((*_, module, context),) = set_contexts(_tables("patients"), [], None)
    assert "[ROWS]" not in table_context.serialize(context)


def test_the_stage_reads_each_table_from_the_extraction_triage_judged(env, tmp_path,
                                                                     monkeypatch):
    settings, catalog, ref = env
    (tmp_path / "t2.html").write_text(TABLE, encoding="utf-8")
    catalog.record([
        Outcome(article_id=ref.id, stage="triage", source="", fingerprint="tr-1",
                payload={}, summary={"source": "pubget"}),
        Outcome(article_id=ref.id, stage="extract", source="pubget", fingerprint="ex-1",
                payload={"tables": [{"table_id": "t2",
                                     "raw_content_path": str(tmp_path / "t2.html")}]}),
    ])
    stage, recorder = RolesStage(settings), Recorder()
    monkeypatch.setattr(stage, "classifier", lambda: recorder)
    _, (done,) = _run(stage, settings, catalog, ref)
    assert done.status is Status.OK
    assert len(recorder.seen) == 2
    assert all("[ROWS] Amygdala | -42 | 18 | 24" in a for a in recorder.seen)


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
    recorder = Recorder()
    unread = {}
    assign_roles(payload, [], None, recorder, raw, unread)
    # A stale path in the metadata does not hide the extraction's file.
    assert "[ROWS] Amygdala | -42 | 18 | 24" in recorder.seen[0]
    assert unread == {
        "t3": f"its raw table file is missing ({tmp_path / 'gone.html'})",
        "t4": "no raw table file is recorded for it; the extraction has no table 't4'",
    }
    unread = {}
    assign_roles(_tables("never recorded"), [], None, recorder, None, unread)
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
    assert "[ROWS] Amygdala | -42 | 18 | 24" in text and "�" not in text


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


class StandIn:
    """Takes the decision model's place in the stage, so a test sets when and how it answers.

    `reply(text)` answers one set, or raises `ModelUnavailable` through `stop` as the model
    does on a failure of its own; `loading`, when set, is an Event the load waits on.
    """

    reply = None
    loading = None
    made = []

    def __init__(self, path, *, source, device="auto", batch_size=32):
        if StandIn.loading is not None:
            StandIn.loading.wait(10)
        self.source = source
        self.asked = []
        self._stopped = None
        StandIn.made.append(self)

    def stop(self, reason):
        self._stopped = self._stopped or reason
        return ModelUnavailable(self._stopped)

    def predict(self, texts):
        out = []
        for text in texts:
            if self._stopped:
                raise ModelUnavailable(self._stopped)
            self.asked.append(text)
            try:
                out.append(StandIn.reply(text) if StandIn.reply else _result())
            except ModelUnavailable as exc:
                raise self.stop(str(exc)) from None
        return out


@pytest.fixture
def both(tmp_path, model_dir, monkeypatch):
    """A run with prose on, its model replaced by the stand-in."""
    monkeypatch.setattr(roles_module, "DecisionModel", StandIn)
    monkeypatch.setattr(StandIn, "reply", None)
    monkeypatch.setattr(StandIn, "loading", None)
    monkeypatch.setattr(StandIn, "made", [])
    settings = Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c",
                        catalog_root=tmp_path / "k", ns_pond_root=tmp_path / "pond",
                        prose_model="nu-prose", role_model=RoleModel(path=str(model_dir)))
    with Catalog.open(settings.catalog_root) as catalog:
        yield settings, catalog


def _resolved(catalog, n, payload, passages=None, *, evidence):
    """An article whose resolve holds `payload`, with the prose and passages it was read from."""
    ref = catalog.register(Identifier(pmid=str(100 + n)), evidence)
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


def _planned(stage, settings, catalog, refs):
    ctx = Context(settings, catalog)
    ids = [r.id for r in refs]
    plan = stage.plan(ctx, refs, catalog.artifacts(ids, "roles"),
                      catalog.artifacts(ids, "resolve"))
    assert len(plan.pending) == len(refs)
    return ctx, plan.pending


def _refuse(name):
    def reply(text):
        if f"[NAME] {name}" in text:
            raise ModelUnavailable("the role model at /m failed: RuntimeError: CUDA error")
        return _result()

    return reply


def test_table_and_prose_sets_are_read_by_one_model_loaded_once(both, evidence):
    settings, catalog = both
    refs = [_resolved(catalog, n, _tables("patients") if n % 2 else _prose_sets("seed"),
                      evidence=evidence) for n in range(12)]
    stage = RolesStage(settings)
    outcomes = list(stage.execute(*_planned(stage, settings, catalog, refs)))
    assert {o.status for o in outcomes} == {Status.OK} and len(outcomes) == 12
    (model,) = StandIn.made
    assert {t.split()[1] for t in model.asked} == {"table", "text"}


def test_no_more_than_64_articles_are_under_way(both, evidence):
    settings, catalog = both
    refs = [_resolved(catalog, n, _tables("patients"), evidence=evidence) for n in range(70)]
    now, most, lock, full = [0], [0], threading.Lock(), threading.Event()

    def held(text):
        with lock:
            now[0] += 1
            most[0] = max(most[0], now[0])
            if now[0] >= 64:
                full.set()
        full.wait(5)  # until the window is full, or long enough to show it never fills
        with lock:
            now[0] -= 1
        return _result()

    StandIn.reply = held
    stage = RolesStage(settings)
    outcomes = list(stage.execute(*_planned(stage, settings, catalog, refs)))
    assert len(outcomes) == 70
    assert most[0] == 64


def test_an_interrupt_stops_the_stage_at_once_and_runs_no_other_set(both, evidence):
    """Ctrl-C leaves while sets are held in the model; the queued ones are never run."""
    settings, catalog = both
    previous = signal.signal(signal.SIGINT, signal.default_int_handler)
    refs = [_resolved(catalog, n, _tables("patients", "controls"),
            evidence=evidence) for n in range(80)]
    release, asked = threading.Event(), []

    def held(text):
        asked.append(text)
        release.wait(10)
        return _result()

    StandIn.reply = held
    stage = RolesStage(settings)
    ctx, works = _planned(stage, settings, catalog, refs)

    def interrupt():
        while len(asked) < 64:
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
    ran = len(asked)
    release.set()
    time.sleep(0.5)  # the held sets are answered; their articles' second sets are refused
    assert took < 5, f"the interrupt waited {took:.1f}s for the window"
    assert ran == len(asked) == 64


def test_an_outage_starts_no_article_and_keeps_those_ahead_of_it(both, monkeypatch, evidence):
    """Once the model stops the stage no article is begun; those ahead that finish are kept."""
    settings, catalog = both
    monkeypatch.setattr(roles_module, "ARTICLES", 3)
    refs = [_resolved(catalog, 0, _prose_sets("first"), evidence=evidence),
            _resolved(catalog, 1, _prose_sets("second"), evidence=evidence),
            _resolved(catalog, 2, _tables("refused"), evidence=evidence),
            _resolved(catalog, 3, _prose_sets("late"), evidence=evidence)]
    refuse = _refuse("refused")

    def reply(text):
        time.sleep(0.5 if "[NAME] first" in text else 1 if "[NAME] second" in text else 0)
        return refuse(text)

    StandIn.reply = reply
    ids = [r.id for r in refs]
    report = run_stages(Context(settings, catalog), [RolesStage(settings)], refs)
    roles = report.stages["roles"]
    assert "CUDA error" in roles.stopped
    assert (roles.ok, roles.failed, roles.left_pending) == (2, 0, 2)
    # Room in the window once `first` was done, but `late` was not begun: the model stopped.
    (model,) = StandIn.made
    assert not any("[NAME] late" in t for t in model.asked)
    assert catalog.artifacts(ids, "roles").keys() == {ids[0], ids[1]}
    assert catalog.attempt_counts(ids, "roles", "") in ({}, {i: (0, None) for i in ids})


def test_after_a_stop_the_model_runs_nothing_and_is_not_loaded_again(both, evidence):
    settings, catalog = both
    refused = _resolved(catalog, 0, _tables("refused"), evidence=evidence)
    prose = _resolved(catalog, 1, _prose_sets("seed"), evidence=evidence)
    StandIn.reply = _refuse("refused")
    stage = RolesStage(settings)
    with pytest.raises(ModelUnavailable, match="CUDA error"):
        list(stage.execute(*_planned(stage, settings, catalog, [refused])))
    ctx, works = _planned(stage, settings, catalog, [prose])
    with pytest.raises(ModelUnavailable, match="CUDA error"):
        list(stage.execute(ctx, works))
    (model,) = StandIn.made
    assert not any("[NAME] seed" in t for t in model.asked)


def test_an_interrupt_while_the_model_is_loading_leaves_it_stopped(both, evidence):
    """Ctrl-C while the model loads: once loaded, it is already stopped and runs nothing."""
    settings, catalog = both
    stage = RolesStage(settings)
    ctx, works = _planned(stage, settings, catalog, [_resolved(catalog, 0, _prose_sets("seed"),
                                                               evidence=evidence)])
    StandIn.loading = threading.Event()
    previous = signal.signal(signal.SIGINT, signal.default_int_handler)

    def interrupt():
        time.sleep(0.2)
        os.kill(os.getpid(), signal.SIGINT)

    threading.Thread(target=interrupt, daemon=True).start()
    try:
        with pytest.raises(KeyboardInterrupt):
            list(stage.execute(ctx, works))
    finally:
        signal.signal(signal.SIGINT, previous)
    StandIn.loading.set()
    time.sleep(0.5)  # the model is loaded, and its article's set would have been run
    (model,) = StandIn.made
    assert model.asked == [] and model._stopped.startswith("the roles stage stopped")


def test_each_way_an_article_fails_leaves_the_others_decided(both, evidence):
    """An analysis that is not one, a passages text that changed, an upstream payload gone."""
    settings, catalog = both
    refs = [_resolved(catalog, n, _tables("patients"), evidence=evidence) for n in range(6)]
    broken = [
        _resolved(catalog, 6, {"t2": {"analyses": ["not a set"]}}, evidence=evidence),
        _resolved(catalog, 7, _tables("patients"),
                  passages={"passages": [{"span": [0, 4]}], "full_text_path": "/gone.txt"},
                  evidence=evidence),
        _resolved(catalog, 8, None, evidence=evidence),
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
