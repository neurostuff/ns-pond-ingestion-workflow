"""The normalize stage's model calls offloaded: a bundle of task prompts, the job's request, and the answers pulled
into the answer store, which the stage then reads with no server.

The paper is article ggrxjtagxq4g with its ev1 evidence, and task-design v4's answers for its two tasks
(`ggrxjtagxq4g.task-design-v4.json`). SSH, rsync and Slurm run against local directories (`Cluster`, as in
test_offload_cluster.py). pondie's passes and the medical linking are stood in for.
"""
from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from ingestion_workflow.catalog import Catalog, Outcome, Run, RunKind, Status
from ingestion_workflow.config import Machine, Settings, TaskDesignModel
from ingestion_workflow.normalization import api, design
from ingestion_workflow.pipeline import Context
from ingestion_workflow.pipeline.stages.offloaded import NormalizeStage
from ingestion_workflow.services import normalize_assign
from ingestion_workflow.services.offload import normalize as N
from ingestion_workflow.services.offload import remote as R
from ingestion_workflow.services.offload.contract import BundleDir, RunSpec, load_spec
from ingestion_workflow.services.offload.importer import import_run
from ingestion_workflow.services.offload.pull import pull

DATA = Path(__file__).resolve().parents[1] / "data" / "offload"
SPEC_FILE = Path(__file__).resolve().parents[3] / "configs" / "offload" / "td-v4.json"
ARTICLE, BASE = "ggrxjtagxq4g", "HHQLir9yUauj"
PAPER = DATA / f"paper-{ARTICLE}"
PARSE = json.loads((PAPER / "parse/coordinate_parse.json").read_text())
RECORD = json.loads((DATA / f"{ARTICLE}.extraction.json").read_text())
EVIDENCED = json.loads((DATA / f"{ARTICLE}.ev1.extraction.json").read_text())
ANSWERS = json.loads((DATA / f"{ARTICLE}.task-design-v4.json").read_text())["answers"]
NOW = datetime(2026, 11, 27, 9, 0, tzinfo=timezone.utc)
INDEX = b'{"weight_map": {}}'


class Cluster:
    """The SSH and rsync a Remote runs, against local directories; Slurm is recorded."""

    def __init__(self, alias):
        self.alias, self.next_job, self.states = alias, 3700000, {}

    def __call__(self, argv, **kwargs):
        if argv[0] == "ssh":
            command = argv[-1]
            if command.startswith("squeue"):
                return subprocess.CompletedProcess([], 0, stdout="", stderr="")
            if command.startswith("sbatch"):
                self.next_job += 1
                return subprocess.CompletedProcess([], 0, stdout=f"{self.next_job}\n", stderr="")
            if command.startswith("sacct"):
                jobs = command.rsplit("-j ", 1)[1].split(",")
                out = "\n".join(f"{j}|{self.states.get(j, 'PENDING')}|00:20:00|0:0" for j in jobs)
                return subprocess.CompletedProcess([], 0, stdout=out, stderr="")
            return subprocess.run(["bash", "-c", command], **kwargs)
        args = [a.replace(f"{self.alias}:", "") for a in argv if a not in ("-e", "ssh -o BatchMode=yes")]
        return subprocess.run(args, **kwargs)


def _run(stage, **kwargs) -> Run:
    held = dict(version=f"v-{stage}", model="m", prompt_version="p", schema_version="s",
                created_at="2026-10-31T13:43:00+00:00")
    run = Run("", RunKind.MODEL, stage, "ns-27b", **{**held, **kwargs})
    return Run(**{**run.__dict__, "run_id": run.minted_id})


@pytest.fixture()
def world(tmp_path, monkeypatch):
    monkeypatch.setattr(normalize_assign, "describe", lambda model: {
        "version": "design-0123456789ab", "model": model, "vocabulary": {"mondo": "2026-09-01"},
        "assigner": {"model": model, "prefix_sha256": design.MODELS[model][1]}})
    for name in ("repair", "apply_derived", "apply_normalized"):
        monkeypatch.setattr(normalize_assign, name, lambda *a, **k: [])
    monkeypatch.setattr(api, "link", lambda *a, **k: {})
    finished = []
    monkeypatch.setattr(api, "finish", lambda record, answers, *a: finished.append(dict(answers)) or {})
    machine = Machine(ssh="ls6", work=str(tmp_path / "m/work"), scratch=str(tmp_path / "m/scratch"),
                      partition="gpu-a100", account="IBN26027", serve_python="/bin/python3",
                      train_python="/bin/python3", hf_home=str(tmp_path / "m/hf"), max_queued=30, su_cap=50)
    weights = tmp_path / "m/scratch/task-design/merged/td-v4-final"
    weights.mkdir(parents=True)
    (weights / "model.safetensors.index.json").write_bytes(INDEX)
    spec = load_spec(SPEC_FILE).model_copy(update={"weights": str(weights),
                                                   "weights_sha256": hashlib.sha256(INDEX).hexdigest()})
    base = Path(machine.hf_home) / "hub/models--numind--NuExtract3/snapshots" / spec.model_revision
    base.mkdir(parents=True)
    (base / "config.json").write_text("{}")
    settings = Settings(data_root=tmp_path / "data", cache_root=tmp_path / "cache", catalog_root=tmp_path / "cat",
                        ns_pond_root=tmp_path / "pond", normalize_model=TaskDesignModel(name="task-design-v4"))
    paper = tmp_path / "pond" / BASE
    shutil.copytree(PAPER, paper)
    with Catalog.open(settings.catalog_root) as catalog:
        catalog._conn.execute("INSERT INTO articles(id, created_at) VALUES(?, '2026-01-01')", (ARTICLE,))
        catalog.record([Outcome(ARTICLE, "sync", "", Status.OK, "sync-fp",
                                summary={"path": str(paper), "parse_id": PARSE["parse_id"], "analyses": 1,
                                         "base_study_id": BASE})])
        import_run(catalog, _run("record"), [{"article_id": ARTICLE, "status": "ok",
                                              "text_sha256": PARSE["text_sha256"], "record": RECORD}])
        digest = catalog.artifact(ARTICLE, "record", "ns-27b").summary["record_digest"]
        import_run(catalog, _run("evidence", model="numind/NuExtract3"),
                   [{"article_id": ARTICLE, "text_sha256": PARSE["text_sha256"], "status": "ok",
                     "record_digest": digest, "record": EVIDENCED}])
        ctx = Context(settings, catalog, retry_after=timedelta(hours=24))
        yield dict(ctx=ctx, catalog=catalog, spec=spec, machine=machine, cluster=Cluster("ls6"),
                   finished=finished, weights=weights)


def _export(w, spec=None):
    return N.export(w["ctx"], spec or w["spec"], "ls6", now=NOW)


def _rows(bundle):
    held = BundleDir(bundle)
    return [json.loads(line) for s in held.bundle().shards for line in held.inputs(s.name).read_text().splitlines()]


def _store(w):
    return NormalizeStage(w["ctx"].settings).answer_store(w["ctx"])


def test_a_spec_names_an_adapter_or_merged_weights():
    held = json.loads(SPEC_FILE.read_text())
    assert load_spec(SPEC_FILE).weights and load_spec(SPEC_FILE).adapter is None
    with pytest.raises(ValueError, match="not both or neither"):
        RunSpec(**{**held, "adapter": "jdkent/x@" + "0" * 40, "adapter_sha256": "a" * 64})
    with pytest.raises(ValueError, match="needs its sha256"):
        RunSpec(**{**held, "weights_sha256": None})


def test_export_writes_one_row_per_task_prompt(world):
    report = _export(world)
    rows = _rows(report.bundle)
    assert len(rows) == len(ANSWERS) == 2 and report.articles == 1
    assert {hashlib.sha256(r["content"].encode()).hexdigest() for r in rows} == \
        {a["prompt_sha256"] for a in ANSWERS.values()}
    for r in rows:
        assert r["key"] == design.key_of(r["content"], "task-design-v4")
        assert r["chat_template_kwargs"] == {"template": design.template(), "enable_thinking": False}
        assert r["max_tokens"] == 4096 and r["text_sha256"] == PARSE["text_sha256"]


def test_export_leaves_out_what_the_store_already_answers(world):
    first = _rows(_export(world).bundle)[0]
    store = _store(world)
    store.put(first["key"], "task-design-v4", "stop", "{}")
    store.close()
    rows = _rows(N.export(world["ctx"], world["spec"], "ls6", now=NOW + timedelta(minutes=1)).bundle)
    assert len(rows) == 1 and rows[0]["key"] != first["key"]


def test_export_refuses_a_spec_of_another_model(world):
    with pytest.raises(ValueError, match="normalize_model names"):
        _export(world, world["spec"].model_copy(update={"version": "task-design-v5"}))


def test_the_job_asks_as_the_model_was_trained(world):
    from ingestion_workflow.services.offload.cluster.job import request_body

    row = _rows(_export(world).bundle)[0]
    body = request_body(row, world["spec"].serve.model_dump())
    assert "response_format" not in body and body["temperature"] == 0.0 and body["max_tokens"] == 4096
    assert body["messages"] == [{"role": "user", "content": row["content"]}]
    assert body["chat_template_kwargs"] == design.request(row["content"], "x")["chat_template_kwargs"]


def test_push_checks_the_merged_weights_and_fetches_no_adapter(world, monkeypatch):
    monkeypatch.setattr(R, "fetch_adapter", lambda *a: pytest.fail("no adapter to fetch"))
    bundle = _export(world).bundle
    remote = R.Remote("ls6", world["machine"], runner=world["cluster"])
    R.push(remote, bundle, hf_cache=Path("/nonexistent"))
    there = Path(world["machine"].work) / "bundles" / BundleDir(bundle).bundle().bundle_id
    cluster = json.loads((there / "cluster.json").read_text())
    assert cluster["merged_dir"] == str(world["weights"])
    (world["weights"] / "model.safetensors.index.json").write_bytes(b"other")
    with pytest.raises(R.RemoteError, match="no merged weights"):
        R.push(remote, bundle, hf_cache=Path("/nonexistent"))


def _pulled(world, results):
    bundle = _export(world).bundle
    remote = R.Remote("ls6", world["machine"], runner=world["cluster"])
    R.push(remote, bundle, hf_cache=Path("/nonexistent"))
    R.submit(remote, bundle, yes=True)
    (job,) = BundleDir(bundle).state().jobs.values()
    world["cluster"].states[job] = "COMPLETED"
    there = Path(world["machine"].work) / "bundles" / BundleDir(bundle).bundle().bundle_id
    (there / "results").mkdir(exist_ok=True)
    rows = _rows(bundle)
    (there / "results/s000.jsonl").write_text("".join(json.dumps(results(r)) + "\n" for r in rows))
    store = _store(world)
    try:
        return pull(remote, bundle, world["catalog"], store), store, rows
    finally:
        store.close()


def _answered(row):
    held = next(a for a in ANSWERS.values() if a["prompt_sha256"] == hashlib.sha256(row["content"].encode()).hexdigest())
    return {"article_id": row["article_id"], "key": row["key"], "text_sha256": row["text_sha256"],
            "prediction": json.loads(held["content"]), "content": held["content"], "finish_reason": "stop",
            "error": None, "prompt_tokens": 25000, "completion_tokens": 300}


def test_pull_stores_the_answers_and_the_stage_reads_them_with_no_server(world):
    said, _, rows = _pulled(world, _answered)
    assert "s000: 2 answers stored, 0 failed" in said
    stage = NormalizeStage(world["ctx"].settings)
    plan = stage.plan(world["ctx"], [world["catalog"].ref(ARTICLE)], world["catalog"].artifacts([ARTICLE], "normalize"),
                      world["catalog"].artifacts([ARTICLE], "evidence"))
    (outcome,) = list(stage.execute(world["ctx"], plan.pending))
    assert outcome.status is Status.OK and outcome.summary["asked"] == 0
    (answers,) = world["finished"]
    assert answers == {t: a["content"] for t, a in ANSWERS.items()}
    assert world["catalog"].artifact(ARTICLE, "normalize", "ns-27b") is None   # pull wrote no artifact


def test_an_over_long_prompt_is_kept_as_such_and_a_failed_request_is_asked_again(world):
    def results(row):
        if "\n- cond_emofh |" in row["content"]:
            return {**_answered(row), "finish_reason": None, "content": None, "prediction": None,
                    "error": "http 400: This model's maximum context length is 32768 tokens"}
        return {**_answered(row), "finish_reason": None, "content": None, "prediction": None,
                "error": "http 500: engine dead"}
    said, _, rows = _pulled(world, results)
    assert "s000: 1 answers stored, 1 failed; `submit --again s000` asks them again" in said
    store = _store(world)
    kept = [store.get(r["key"]) for r in rows]
    store.close()
    assert sorted(kept, key=str) == sorted([(normalize_assign.OVER, None), None], key=str)


def test_a_normalize_bundle_needs_an_answer_store(world):
    bundle = _export(world).bundle
    remote = R.Remote("ls6", world["machine"], runner=world["cluster"])
    with pytest.raises(ValueError, match="answer store"):
        pull(remote, bundle, world["catalog"])
