"""`normalize` in-process: each article's evidence record renormalized, its tasks' designs asked of the
task-design model, its contrasts assembled.

The paper is article ggrxjtagxq4g (two tasks, four conditions) with its 27B record and ev1 evidence, and
task-design v4's answers for its tasks as beast's server gave them (`ggrxjtagxq4g.task-design-v4.json`). The server is stood in for by a client
that answers from those; pondie's `repair`, `apply_derived` and `apply_normalized`, the medical linking and the
sidecar writing are stood in for too (each is tested on its own): what is checked here is what the stage reads
and asks, which run it records, what it keeps, and when it runs again. Where pondie is installed, the sidecars
are written for real once, and pondie's fixes run on the paper.
"""

from __future__ import annotations

import copy
import json
import shutil
from datetime import timedelta
from pathlib import Path

import pytest
from ingestion_workflow.catalog import Catalog, Outcome, Run, RunKind, Status  # noqa: E402
from ingestion_workflow.config import Settings, TaskDesignModel  # noqa: E402
from ingestion_workflow.normalization import api, design  # noqa: E402
from ingestion_workflow.pipeline import Context  # noqa: E402
from ingestion_workflow.pipeline.stages.offloaded import (  # noqa: E402
    AWAITS_IMPORT,
    NO_NORMALIZE_INPUT,
    NO_SERVER,
    NormalizeStage,
    record_digest,
)
from ingestion_workflow.services import normalize_assign  # noqa: E402
from ingestion_workflow.services.offload.importer import import_run  # noqa: E402

DATA = Path(__file__).resolve().parents[1] / "data" / "offload"
ARTICLE, BASE = "ggrxjtagxq4g", "HHQLir9yUauj"
PAPER = DATA / f"paper-{ARTICLE}"
PARSE = json.loads((PAPER / "parse/coordinate_parse.json").read_text())
RECORD = json.loads((DATA / f"{ARTICLE}.extraction.json").read_text())
EVIDENCED = json.loads((DATA / f"{ARTICLE}.ev1.extraction.json").read_text())
ANSWERS = json.loads((DATA / f"{ARTICLE}.task-design-v4.json").read_text())
TEXT = (PAPER / "processed/pubget/text.txt").read_bytes().decode("utf-8")
#: A 27B record that still carries the per-field siblings study_schema#56 removed, and its paper.
OLD = DATA / "paper-26klegjq7mhi"
OLD_EVIDENCED = json.loads((DATA / "26klegjq7mhi.ev1.extraction.json").read_text())
ADAPTER = "jdkent/qwen3.8-27b-ns-record@dbfd846bd3e1259039eb2f3117dc12bd733c640a"
MODEL = "task-design-v4"
#: The library's own `finish`, before a test stands in for it.
REAL_FINISH = api.finish


def _run(stage, **kwargs) -> Run:
    held = dict(version=f"v-{stage}", model="m", adapter=ADAPTER, prompt_version="p",
                schema_version="s", created_at="2026-10-31T13:43:00+00:00")
    run = Run("", RunKind.MODEL, stage, "ns-27b", **{**held, **kwargs})
    return Run(**{**run.__dict__, "run_id": run.minted_id})


class Server:
    """The served model: answers each prompt with what v4 answered for it, and counts what it was asked."""

    def __init__(self):
        self.asked, self.served_names, self.fail, self.reply = [], [MODEL], None, None
        self.by_prompt = {a["prompt_sha256"]: a for part in ("answers", "record_answers")
                          for a in ANSWERS[part].values()}

    def client(self, url, model, timeout_s=1800):
        server = self

        class Client:
            def served(self):
                return server.served_names

            def ask(self, prompt):
                import hashlib

                server.asked.append(prompt)
                if server.fail:
                    raise normalize_assign.RequestFailed(server.fail)
                if server.reply:
                    return server.reply
                held = server.by_prompt[hashlib.sha256(prompt.encode()).hexdigest()]
                return held["finish"], held["content"], {"prompt_tokens": 1, "completion_tokens": 1}
        return Client()


class Library:
    """pondie's passes and the medical linking, and what they were given."""

    def __init__(self):
        self.version = "design-0123456789ab"
        self.order, self.linked, self.finished = [], [], []

    def describe(self, model):
        return {"version": self.version, "model": model, "vocabulary": {"mondo": "2026-09-01"},
                "assigner": {"model": model, "prefix_sha256": design.MODELS[model][1]}}

    def repair(self, record, parse, text):
        self.order.append("repaired")
        self.repaired = {"parse": parse, "text": text}
        return []

    def apply_derived(self, record):
        self.order.append("derived")
        for group in record.get("groups") or []:
            group["is_healthy"] = {"value": True}

    def apply_normalized(self, record):
        self.order.append("normalized")
        for group in record.get("groups") or []:
            group.setdefault("age_unit", {}).setdefault("normalized", [{"term": "years"}])

    def finish(self, record, answers, parse, model, version):
        self.finished.append({"answers": dict(answers), "parse": parse, "model": model, "version": version})
        return {}

    def link(self, record, text, article_id, version, **kw):
        self.linked.append({"article_id": article_id, "text": text, "version": version,
                            "record": copy.deepcopy(record)})
        if getattr(self, "broken", None):
            raise RuntimeError(self.broken)
        return {}


@pytest.fixture()
def library(monkeypatch):
    held = Library()
    monkeypatch.setattr(normalize_assign, "describe", held.describe)
    monkeypatch.setattr(normalize_assign, "repair", held.repair)
    monkeypatch.setattr(normalize_assign, "apply_derived", held.apply_derived)
    monkeypatch.setattr(normalize_assign, "apply_normalized", held.apply_normalized)
    monkeypatch.setattr(api, "link", held.link)
    monkeypatch.setattr(api, "finish", held.finish)
    return held


@pytest.fixture()
def server(monkeypatch):
    held = Server()
    monkeypatch.setattr(normalize_assign, "Client", held.client)
    return held


@pytest.fixture()
def ctx(tmp_path):
    settings = Settings(data_root=tmp_path / "data", cache_root=tmp_path / "cache",
                        catalog_root=tmp_path / "cat", ns_pond_root=tmp_path / "pond",
                        normalize_model=TaskDesignModel(name=MODEL, url="http://localhost:8410/v1",
                                                        concurrency=2))
    paper = tmp_path / "pond" / BASE
    shutil.copytree(PAPER, paper)
    with Catalog.open(settings.catalog_root) as catalog:
        catalog._conn.execute("INSERT INTO articles(id, created_at) VALUES(?, '2026-01-01')",
                              (ARTICLE,))
        catalog.record([Outcome(ARTICLE, "sync", "", Status.OK, "sync-fp",
                                summary={"path": str(paper), "parse_id": PARSE["parse_id"],
                                         "analyses": 1, "base_study_id": BASE})])
        yield Context(settings, catalog, retry_after=timedelta(hours=24))


def _with(ctx, **model):
    held = ctx.settings.normalize_model.model_copy(update=model)
    return ctx.settings.model_copy(update={"normalize_model": held})


def _recorded(ctx, evidence="ok"):
    import_run(ctx.catalog, _run("record"), [{"article_id": ARTICLE, "status": "ok",
                                              "text_sha256": PARSE["text_sha256"],
                                              "record": RECORD}])
    digest = ctx.catalog.artifact(ARTICLE, "record", "ns-27b").summary["record_digest"]
    row = ({"status": "ok", "record_digest": digest, "record": EVIDENCED} if evidence == "ok"
           else {"status": "skipped", "reason": "over the input cap", "record": None})
    import_run(ctx.catalog, _run("evidence", model="numind/NuExtract3"),
               [{"article_id": ARTICLE, "text_sha256": PARSE["text_sha256"], **row}])


def _renormalized(record):
    """`record` as the stage asks about it: its old normalizations made again (by the stand-ins)."""
    record = copy.deepcopy(record)
    normalize_assign.drop_siblings(record)
    for group in record.get("groups") or []:
        group["is_healthy"] = {"value": True}
        group.setdefault("age_unit", {}).setdefault("normalized", [{"term": "years"}])
    return record


def _plan(ctx, stage):
    return stage.plan(ctx, [ctx.catalog.ref(ARTICLE)],
                      ctx.catalog.artifacts([ARTICLE], stage.name),
                      ctx.catalog.artifacts([ARTICLE], stage.requires))


def _normalize(ctx, settings=None):
    stage = NormalizeStage(settings or ctx.settings)
    plan = _plan(ctx, stage)
    outcomes = list(stage.execute(ctx, plan.pending))
    ctx.catalog.record(outcomes)
    return plan, outcomes


def _entries(wrapper):
    return {e["vocabulary"]: e for e in wrapper.get("normalized") or []}


def test_without_a_model_normalize_only_holds_imports(ctx, library, server):
    _recorded(ctx)
    plan = _plan(ctx, NormalizeStage(ctx.settings.model_copy(update={"normalize_model": None})))
    assert plan.pending == [] and plan.reasons == {AWAITS_IMPORT: 1}


def test_each_task_of_the_evidence_record_is_asked_its_training_prompt(ctx, library, server):
    _recorded(ctx)
    plan, (outcome,) = _normalize(ctx)
    evidence = ctx.catalog.artifact(ARTICLE, "evidence", "ns-27b")
    assert plan.pending[0].upstream.stage == "evidence"
    expected = api.prompts(_renormalized(ctx.catalog.payload(evidence)), TEXT, MODEL)
    assert sorted(server.asked) == sorted(expected.values()) and len(expected) == len(ANSWERS["answers"]) == 2
    (done,) = library.finished
    assert done["answers"] == {t: a["content"] for t, a in ANSWERS["answers"].items()}
    assert (done["model"], done["version"]) == (MODEL, library.version)
    assert done["parse"]["parse_id"] == PARSE["parse_id"]
    (linked,) = library.linked
    assert (linked["article_id"], linked["text"], linked["version"]) == (ARTICLE, TEXT, library.version)
    assert outcome.status is Status.OK, outcome.error
    kept = ctx.catalog.artifact(ARTICLE, "normalize", "ns-27b")
    assert (kept.summary["read"], kept.summary["model"], kept.summary["version"]) == (
        "evidence", MODEL, library.version)
    assert kept.summary["record_digest"] == evidence.summary["record_digest"]
    run = ctx.catalog.run(kept.run_id)
    assert (run.stage, run.model, run.version, run.pipeline, run.fit) == (
        "normalize", MODEL, library.version, "ns-pond normalize", None)
    assert run.prompt_version == design.MODELS[MODEL][1] and run.run_id == run.minted_id
    assert _plan(ctx, NormalizeStage(ctx.settings)).fresh == 1


def test_the_answers_are_written_as_sidecars(ctx, library, server, monkeypatch):
    pytest.importorskip("pondie")
    monkeypatch.setattr(api, "finish", REAL_FINISH)
    _recorded(ctx)
    _normalize(ctx)
    record = ctx.catalog.payload(ctx.catalog.artifact(ARTICLE, "normalize", "ns-27b"))
    for task in design.tasks_of(record):
        answer = design.read(ANSWERS["answers"][task["local_id"]]["content"])
        got = _entries(task["name"])
        assert got["astra canonical task"].get("value") == answer["task"]
        assert got["astra task family"].get("value") == answer["family"]
        assert got["ns-pond task confidence"]["value"] == answer["task_confidence"]
        assert got["astra canonical task"]["mapping_tool"] == \
            f"ingestion_workflow.normalization.design {library.version}"
        for cond in task["conditions"]:
            mine = next(c for c in answer["conditions"] if c["id"] == cond["local_id"])
            atoms = {(e["vocabulary"].split(": ", 1)[1], e["value"]) for e in cond["name"]["normalized"]
                     if e["vocabulary"].startswith("condition atom: ")}
            assert atoms == {(a["axis"], a["value"]) for a in mine["atoms"]}
    keys = [_entries(a["name"]).get("ns-pond contrast key v2") for a in record["analyses"]]
    assert all(k and k["value"].startswith("v2|") and k["label"] for k in keys)


def test_a_skipped_evidence_run_s_article_is_normalized_from_its_record(ctx, library, server):
    _recorded(ctx, evidence="skipped")
    _normalize(ctx)
    (linked,) = library.linked
    assert record_digest(linked["record"]) == record_digest(_renormalized(RECORD))
    assert ctx.catalog.artifact(ARTICLE, "normalize", "ns-27b").summary["read"] == "record"


def test_new_code_makes_every_normalization_stale_and_asks_the_model_nothing_again(ctx, library, server):
    _recorded(ctx)
    _normalize(ctx)
    first = ctx.catalog.artifact(ARTICLE, "normalize", "ns-27b").run_id
    asked = len(server.asked)
    library.version = "design-ba9876543210"
    plan, (outcome,) = _normalize(ctx)
    assert len(plan.pending) == 1 and outcome.status is Status.OK
    kept = ctx.catalog.artifact(ARTICLE, "normalize", "ns-27b")
    assert kept.run_id != first and ctx.catalog.run(kept.run_id).version == "design-ba9876543210"
    assert len(server.asked) == asked and kept.summary["asked"] == 0


def test_a_rerun_under_the_same_code_is_the_run_first_kept(ctx, library, server):
    _recorded(ctx)
    _normalize(ctx)
    kept = ctx.catalog.run(ctx.catalog.artifact(ARTICLE, "normalize", "ns-27b").run_id)
    ctx.catalog._conn.execute("UPDATE artifacts SET status='failed' WHERE stage='normalize'")
    ctx.catalog._conn.execute("DELETE FROM attempts WHERE stage='normalize'")
    _normalize(ctx)
    again = ctx.catalog.run(ctx.catalog.artifact(ARTICLE, "normalize", "ns-27b").run_id)
    assert again == kept and len(ctx.catalog.runs()) == 3


def test_stale_evidence_is_not_normalized(ctx, library, server):
    _recorded(ctx)
    newer = copy.deepcopy(RECORD)
    newer["analyses"][0]["name"]["value"] = "a later run's name"
    import_run(ctx.catalog, _run("record", prompt_version="p2"),
               [{"article_id": ARTICLE, "status": "ok", "text_sha256": PARSE["text_sha256"],
                 "record": newer}])
    plan = _plan(ctx, NormalizeStage(ctx.settings))
    assert plan.pending == [] and plan.reasons == {NO_NORMALIZE_INPUT: 1}


def test_a_link_that_raises_is_this_article_s_failure(ctx, library, server):
    _recorded(ctx)
    library.broken = "pondie's negation parser is missing"
    _, (outcome,) = _normalize(ctx)
    assert outcome.status is Status.FAILED and "negation parser" in outcome.error
    assert outcome.run_id and ctx.catalog.run(outcome.run_id).version == library.version
    assert server.asked == []


def test_a_failed_request_fails_the_article_and_is_asked_again(ctx, library, server):
    _recorded(ctx)
    server.fail = "http 500: engine dead"
    _, (outcome,) = _normalize(ctx)
    assert outcome.status is Status.FAILED and "engine dead" in outcome.error
    store = NormalizeStage(ctx.settings).answer_store(ctx)
    assert len(store) == 0
    store.close()
    server.fail = None
    ctx.catalog._conn.execute("DELETE FROM attempts WHERE stage='normalize'")
    _, (outcome,) = _normalize(ctx)
    assert outcome.status is Status.OK


def test_an_answer_cut_at_the_limit_is_kept_and_read_as_no_answer(ctx, library, server):
    _recorded(ctx)
    server.reply = ("length", '{"factors": [', {})
    _, (outcome,) = _normalize(ctx)
    assert outcome.status is Status.OK
    (done,) = library.finished
    assert set(done["answers"]) == set(ANSWERS["answers"])
    assert all(isinstance(a, api.NoAnswer) and a == "no answer: cut at the length limit"
               for a in done["answers"].values())
    asked = len(server.asked)
    library.version = "design-ba9876543210"
    _normalize(ctx)
    assert len(server.asked) == asked


def test_without_a_url_only_stored_answers_are_read(ctx, library, server):
    _recorded(ctx)
    _, (outcome,) = _normalize(ctx, _with(ctx, url=None))
    assert outcome.status is Status.FAILED
    assert outcome.error == NO_SERVER.format(len(ANSWERS["answers"]))
    ctx.catalog._conn.execute("DELETE FROM attempts WHERE stage='normalize'")
    _normalize(ctx)                       # asked once, by a server
    library.version = "design-ba9876543210"
    _, (outcome,) = _normalize(ctx, _with(ctx, url=None))
    assert outcome.status is Status.OK


def test_a_server_serving_another_model_stops_the_stage(ctx, library, server):
    from ingestion_workflow.clients.model_server import ModelUnavailable

    _recorded(ctx)
    server.served_names = ["task-design-v3"]
    with pytest.raises(ModelUnavailable, match="task-design-v3"):
        _normalize(ctx)
    assert server.asked == [] and library.linked == []


def test_workers_answer_in_order_and_the_first_job_runs_here(library):
    jobs = [(k, None if k == "b" else ({"local_id": k}, "text", k, None, MODEL, "v"))
            for k in ("a", "b", "c", "d", "e")]
    out = list(normalize_assign.prepare_all(jobs, 2, "no parse"))
    assert [k for k, _ in out] == ["a", "b", "c", "d", "e"]
    assert out[1][1] == (None, "no parse")
    assert [rec["local_id"] for _, (got, err) in out if got for rec in [got[0]]] == ["a", "c", "d", "e"]
    # the first job ran here, before the fork; the rest in the workers
    assert [c["article_id"] for c in library.linked] == ["a"]


def test_the_old_normalized_siblings_are_dropped():
    (held,) = OLD_EVIDENCED["groups"]
    assert "age_unit_normalized" in held  # the 27B's records carry them; the fixture is one
    record = copy.deepcopy(OLD_EVIDENCED)
    assert normalize_assign.drop_siblings(record) == 3
    assert "age_unit_normalized" not in json.dumps(record) and "category_normalized" not in json.dumps(record)


def test_derived_slots_replace_the_model_s_answer_before_linking(ctx, library, server):
    _recorded(ctx)
    _normalize(ctx)
    assert library.order == ["repaired", "derived", "normalized"]
    assert library.linked[0]["record"]["groups"][0]["is_healthy"] == {"value": True}


def test_the_fixes_read_the_paper_s_own_parse_and_text(ctx, library, server):
    _recorded(ctx)
    _normalize(ctx)
    paper = ctx.settings.ns_pond_root / BASE
    assert library.repaired["parse"] == paper / "parse" / "coordinate_parse.json"
    assert library.repaired["text"] == TEXT


def test_pondie_s_fixes_run_on_the_record_but_the_two_that_change_what_it_says():
    """`unreachable` would delete a scanner no analysis points at; the stage keeps it."""
    pytest.importorskip("pondie.extraction.record.fix")
    record = copy.deepcopy(OLD_EVIDENCED)
    normalize_assign.drop_siblings(record)
    record.setdefault("devices", []).append({"local_id": "dev_unused"})
    fired = normalize_assign.repair(
        record, OLD / "parse/coordinate_parse.json",
        (OLD / "processed/ace/text.txt").read_bytes().decode("utf-8"))
    assert "derived_ids" in fired
    assert not set(fired) & normalize_assign.SKIPPED_REPAIRS
    assert "dev_unused" in {d.get("local_id") for d in record["devices"]}
