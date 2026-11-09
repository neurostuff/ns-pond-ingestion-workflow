"""`ingest offload push`, `submit`, `status` and `pull` against a machine that is a directory.

Shell commands sent over SSH run here, against that directory, and rsync copies into it;
only Slurm answers from what Vista said (sacct of the 10-30 and 10-31 shard jobs, its
"CANCELLED by <uid>" spelling). The paper and the model's answer are real: article
26klegjq7mhi and the prediction its 10-31 shard (s114) made.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from ingestion_workflow.catalog import Catalog, Outcome, Status
from ingestion_workflow.config import Machine, Settings
from ingestion_workflow.pipeline import Context
from ingestion_workflow.services.offload import remote as R
from ingestion_workflow.services.offload.contract import (
    BundleDir,
    load_spec,
    read_rows,
    write_rows,
)
from ingestion_workflow.services.offload.export import export
from ingestion_workflow.services.offload.pull import NO_ANSWER, REQUEST_FAILED, pull
from pondie_astra import prompt

DATA = Path(__file__).resolve().parents[1] / "data" / "offload"
SPEC_FILE = Path(__file__).resolve().parents[3] / "configs" / "offload" / "ns-27b-e.json"
PRED = json.loads((DATA / "26klegjq7mhi.pred.json").read_text())
RECORD = json.loads((DATA / "26klegjq7mhi.extraction.json").read_text())

def as_kept(record):
    """`record` as import keeps it: without the legacy siblings the schema refuses."""
    from ingestion_workflow.pipeline.stages.offloaded import drop_siblings

    record = json.loads(json.dumps(record))
    drop_siblings(record)
    return record

ARTICLE = "26klegjq7mhi"
NOW = datetime(2026, 11, 4, 15, 0, tzinfo=timezone.utc)
#: What Vista's sbatch --parsable printed for the 2026-11-04 smoke job: TACC's banner first.
SBATCH = ("-----------------------------------------------------------------\n"
          "          Welcome to the Vista Supercomputer                       \n"
          "-----------------------------------------------------------------\n\n"
          "No reservation for this job\n--> Verifying valid submit host (login1)...OK\n"
          "--> Verifying valid jobname...OK\n--> Verifying valid ssh keys...OK\n"
          "--> Verifying access to desired queue (gh-dev)...OK\n"
          "--> Checking available allocation (IBN26027)...OK\n"
          "--> Quotas are not currently enabled for filesystem /home1/07723/jdk3232...OK\n"
          "{job}\n")
#: Vista's sacct, -X -n -P -o JobID,State,Elapsed,ExitCode.
SACCT = {"COMPLETED": "{job}|COMPLETED|00:32:59|0:0",
         "TIMEOUT": "{job}|TIMEOUT|01:15:12|0:15",
         "CANCELLED": "{job}|CANCELLED by 870221|00:00:00|0:0",
         "RUNNING": "{job}|RUNNING|00:10:02|0:0"}


class Cluster:
    """The SSH and rsync a Remote runs, against local directories; Slurm is recorded."""

    def __init__(self, alias: str) -> None:
        self.alias = alias
        self.queued = 0
        self.next_job = 1112100
        self.states: dict = {}
        self.sent: list = []

    def __call__(self, argv, **kwargs):
        if argv[0] == "ssh":
            command = argv[-1]
            self.sent.append(command)
            if command.startswith("squeue"):
                return self._done("\n".join(str(i) for i in range(self.queued)))
            if command.startswith("sbatch"):
                self.next_job += 1
                self.states[str(self.next_job)] = "PENDING"
                return self._done(SBATCH.format(job=self.next_job))
            if command.startswith("sacct"):
                jobs = command.rsplit("-j ", 1)[1].split(",")
                return self._done("\n".join(SACCT.get(self.states.get(j), "{job}|PENDING|00:00:00|0:0")
                                            .format(job=j) for j in jobs))
            return subprocess.run(["bash", "-c", command], **kwargs)
        assert argv[0] == "rsync"
        args = [a.replace(f"{self.alias}:", "") for a in argv if a not in ("-e", "ssh -o BatchMode=yes")]
        return subprocess.run(args, **kwargs)

    @staticmethod
    def _done(out: str):
        return subprocess.CompletedProcess([], 0, stdout=out, stderr="")


@pytest.fixture()
def world(tmp_path, monkeypatch):
    monkeypatch.setattr(prompt, "ntok", lambda s: len(s) // 4)
    machine = Machine(ssh="vista", work=str(tmp_path / "m/work"), scratch=str(tmp_path / "m/scratch"),
                      partition="gh", account="IBN26027", serve_python="/bin/python3",
                      train_python="/bin/python3", hf_home=str(tmp_path / "m/hf"),
                      environment={"CC": "gcc"}, max_queued=30, su_cap=50)
    spec = load_spec(SPEC_FILE)
    base = Path(machine.hf_home) / "hub/models--Qwen--Qwen3.8-27B/snapshots" / spec.model_revision
    base.mkdir(parents=True)
    (base / "config.json").write_text("{}")
    adapter = tmp_path / "hub-adapter"
    adapter.mkdir()
    (adapter / "adapter_config.json").write_text("{}")
    (adapter / "adapter_model.safetensors").write_bytes(b"weights")
    spec = spec.model_copy(update={"adapter_sha256": hashlib.sha256(b"weights").hexdigest()})
    fetched = []

    def fetch(adapter_ref, sha, cache):
        fetched.append(adapter_ref)
        return adapter
    monkeypatch.setattr(R, "fetch_adapter", fetch)
    settings = Settings(data_root=tmp_path / "data", cache_root=tmp_path / "cache",
                        catalog_root=tmp_path / "cat", ns_pond_root=tmp_path / "pond")
    paper = tmp_path / "pond" / ARTICLE
    shutil.copytree(DATA / "paper-26klegjq7mhi", paper)
    cluster = Cluster("vista")
    with Catalog.open(settings.catalog_root) as catalog:
        catalog._conn.execute("INSERT INTO articles(id, created_at) VALUES(?, '2026-01-01')",
                              (ARTICLE,))
        catalog.record([Outcome(ARTICLE, "sync", "", Status.OK, "sync-fp",
                                summary={"path": str(paper), "parse_id": "p1", "analyses": 1})])
        ctx = Context(settings, catalog, retry_after=timedelta(hours=24))
        bundle = export(ctx, spec, "vista", workers=1, now=NOW).bundle
        yield dict(ctx=ctx, catalog=catalog, bundle=bundle, cluster=cluster,
                   remote=R.Remote("vista", machine, runner=cluster), fetched=fetched,
                   there=Path(machine.work) / "bundles" / BundleDir(bundle).bundle().bundle_id)


def _push(w):
    return R.push(w["remote"], w["bundle"], hf_cache=Path("/nonexistent"))


def test_push_copies_the_bundle_its_scripts_and_the_adapter_once(world):
    said = _push(world)
    assert world["fetched"] == [load_spec(SPEC_FILE).adapter]
    there = world["there"]
    assert (there / "bundle.json").exists() and (there / "inputs/s000.jsonl").exists()
    assert (there / "job.py").exists() and (there / "merge.py").exists()
    cluster = json.loads((there / "cluster.json").read_text())
    assert cluster["merged_dir"].endswith(
        "merged/jdkent__qwen3.8-27b-ns-record@dbfd846bd3e1259039eb2f3117dc12bd733c640a")
    assert (Path(cluster["adapter_dir"]) / "adapter_model.safetensors").read_bytes() == b"weights"
    assert not (there / "state.json").exists()
    assert any(line.startswith("adapter ") for line in said)
    _push(world)   # the machine already holds that commit's adapter
    assert len(world["fetched"]) == 1


def test_push_stops_when_the_machine_lacks_the_base_model(world):
    shutil.rmtree(Path(world["remote"].machine.hf_home) / "hub")
    with pytest.raises(R.RemoteError, match="no base model"):
        _push(world)


def test_submit_names_the_job_after_the_bundle_and_never_submits_a_shard_twice(world):
    with pytest.raises(R.RemoteError, match="push the bundle first"):
        R.submit(world["remote"], world["bundle"], yes=False)
    _push(world)
    said = R.submit(world["remote"], world["bundle"], yes=False)
    assert "1 shards" in said[0] and said[1] == "s000: job 1112101"
    script = (world["there"] / "logs/s000.sbatch").read_text()
    bundle_id = BundleDir(world["bundle"]).bundle().bundle_id
    assert f"#SBATCH -J {bundle_id}-s000" in script and "#SBATCH -t 01:15:00" in script
    assert 'export HF_HUB_OFFLINE="1"' in script and 'export CC="gcc"' in script
    assert f"/bin/python3 -I {world['there']}/job.py {world['there']} s000" in script
    assert R.submit(world["remote"], world["bundle"], yes=False) == ["every shard is submitted"]
    again = R.submit(world["remote"], world["bundle"], yes=False, again=["s000"])
    assert again[1] == "s000: job 1112102"


def test_submit_over_the_cap_needs_yes(world):
    _push(world)
    world["remote"].machine.su_cap = 0.5   # one shard at its 1:15 limit is 1.25 SU
    with pytest.raises(R.RemoteError, match="over vista's cap of 0.5 SU; pass --yes"):
        R.submit(world["remote"], world["bundle"], yes=False)
    assert not any(c.startswith("sbatch") for c in world["cluster"].sent)
    assert R.submit(world["remote"], world["bundle"], yes=True)[1].startswith("s000: job")


def test_submit_leaves_shards_for_later_when_the_queue_is_full(world):
    _push(world)
    world["cluster"].queued = 30
    said = R.submit(world["remote"], world["bundle"], yes=False)
    assert "1 shards wait for room in the queue (30 of 30 queued)" in said[-1]
    assert BundleDir(world["bundle"]).state().jobs == {}


def _job_ran(world, rows):
    """What job.py would have written, and the state sacct then reports."""
    (job,) = BundleDir(world["bundle"]).state().jobs.values()
    (world["there"] / "results").mkdir(exist_ok=True)
    (world["there"] / "results/s000.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
    return job


def _result(**kwargs):
    sent = json.loads((DATA / "paper-26klegjq7mhi/parse/coordinate_parse.json").read_text())
    row = {"article_id": ARTICLE, "text_sha256": sent["text_sha256"], "prediction": None,
           "finish_reason": "stop", "error": None, "prompt_tokens": 15089,
           "completion_tokens": PRED["tokens"], "wall_s": PRED["wall_s"]}
    row.update(kwargs)
    return row


def test_pull_imports_a_completed_shard_as_the_record_the_collected_set_holds(world):
    _push(world)
    R.submit(world["remote"], world["bundle"], yes=False)
    job = _job_ran(world, [_result(prediction=PRED["prediction"])])
    world["cluster"].states[job] = "RUNNING"
    said = pull(world["remote"], world["bundle"], world["catalog"])
    assert "s000: RUNNING" in said and world["catalog"].artifact(ARTICLE, "record", "ns-27b") is None
    world["cluster"].states[job] = "COMPLETED"
    pull(world["remote"], world["bundle"], world["catalog"])
    kept = world["catalog"].artifact(ARTICLE, "record", "ns-27b")
    bundle = BundleDir(world["bundle"]).bundle()
    assert kept.ok and kept.run_id == bundle.run["run_id"]
    record = world["catalog"].payload(kept)
    assert record["extraction_metadata"]["extractor_version"] == bundle.run["run_id"]
    strip = ("local_id", "extraction_metadata")
    assert {k: v for k, v in record.items() if k not in strip} == \
        {k: v for k, v in as_kept(RECORD).items() if k not in strip}
    state = BundleDir(world["bundle"]).state()
    assert state.imported["s000"] == {"ok": 1, "elapsed_s": 1979}
    assert world["catalog"].run(bundle.run["run_id"]).adapter == bundle.spec.adapter
    pull(world["remote"], world["bundle"], world["catalog"])   # nothing left to do
    assert BundleDir(world["bundle"]).state().imported == state.imported


def test_a_shard_whose_text_holds_a_line_separator_is_imported(world):
    """Export writes the paper as it is, so a U+2028 in it stays inside its row."""
    _push(world)
    R.submit(world["remote"], world["bundle"], yes=False)
    inputs = BundleDir(world["bundle"]).inputs("s000")
    (row,) = [json.loads(line) for line in inputs.read_text("utf-8").split("\n") if line]
    row["content"] += " "
    inputs.write_text(json.dumps(row, ensure_ascii=False) + "\n", "utf-8")
    job = _job_ran(world, [_result(prediction=PRED["prediction"])])
    world["cluster"].states[job] = "COMPLETED"
    pull(world["remote"], world["bundle"], world["catalog"])
    assert world["catalog"].artifact(ARTICLE, "record", "ns-27b").ok


def test_a_bundle_file_keeps_a_line_separator_inside_its_row(tmp_path):
    rows = [{"article_id": "a", "content": "one\u2028two"}, {"article_id": "b", "content": "é"}]
    write_rows(tmp_path / "s000.jsonl", rows)
    raw = (tmp_path / "s000.jsonl").read_text("utf-8")
    assert "\u2028" in raw and "\\u" not in raw
    assert list(read_rows([tmp_path / "s000.jsonl"])) == rows


@pytest.mark.parametrize("row, status, reason", [
    (dict(finish_reason="length"), "skipped", NO_ANSWER.format("length")),
    (dict(error="invalid_json: JSONDecodeError('Expecting value')"), "skipped",
     NO_ANSWER.format("invalid_json: JSONDecodeError('Expecting value')")),
    (dict(finish_reason=None, error="http 500: engine dead"), "failed",
     REQUEST_FAILED.format("http 500: engine dead")),
])
def test_a_row_without_an_answer_is_skipped_or_failed(world, row, status, reason):
    _push(world)
    R.submit(world["remote"], world["bundle"], yes=False)
    world["cluster"].states[_job_ran(world, [_result(**row)])] = "COMPLETED"
    pull(world["remote"], world["bundle"], world["catalog"])
    kept = world["catalog"].artifact(ARTICLE, "record", "ns-27b")
    assert kept.status.value == status and (kept.error or kept.summary.get("reason")) == reason


@pytest.mark.parametrize("state", ["TIMEOUT", "CANCELLED"])
def test_a_shard_that_ended_otherwise_is_left_for_a_resubmit(world, state):
    _push(world)
    R.submit(world["remote"], world["bundle"], yes=False)
    world["cluster"].states[_job_ran(world, [])] = state
    said = pull(world["remote"], world["bundle"], world["catalog"])
    assert f"s000: {state}" in " ".join(said) and "submit --again s000" in " ".join(said)
    assert BundleDir(world["bundle"]).state().imported == {}


def test_a_shard_missing_rows_is_not_imported(world):
    _push(world)
    R.submit(world["remote"], world["bundle"], yes=False)
    world["cluster"].states[_job_ran(world, [])] = "COMPLETED"
    said = pull(world["remote"], world["bundle"], world["catalog"])
    assert "s000: 1 of 1 rows have no result" in " ".join(said)
    assert world["catalog"].artifact(ARTICLE, "record", "ns-27b") is None


def test_the_estimate_uses_the_last_measured_run_of_the_version(world, tmp_path):
    _push(world)
    held = BundleDir(world["bundle"])
    tokens = held.bundle().shards[0].input_tokens
    earlier = held.root.parent / "ns-27b-e-20261101-0900"
    shutil.copytree(held.root, earlier)
    manifest = json.loads((earlier / "bundle.json").read_text())
    manifest.update(bundle_id=earlier.name, created_at="2026-11-01T09:00:00+00:00")
    (earlier / "bundle.json").write_text(json.dumps(manifest))
    (earlier / "state.json").write_text(json.dumps({"imported": {"s000": {"ok": 1, "elapsed_s": 1800}}}))
    guess = R.estimate(world["remote"], held.root, ["s000"], "01:15:00")
    assert guess.node_hours == pytest.approx(0.5) and "measured" in guess.basis
    assert R.measured_rate(held.root.parent, held.bundle()) == pytest.approx(tokens * 2)


def test_setup_checks_each_environment_against_its_group(world):
    assert R.group_pins("infer-vista") == {"vllm": "0.28.0", "torch": "2.13.0",
                                           "transformers": "5.16.1"}
    with pytest.raises(ValueError, match="no optional-dependency group"):
        R.group_pins("infer-nowhere")


def test_the_job_id_is_read_past_tacc_s_banner():
    assert R.sbatch_job_id(SBATCH.format(job=1122428)) == "1122428"
    assert R.sbatch_job_id("1122428;vista\n") == "1122428"
    assert R.sbatch_job_id("sbatch: error: invalid partition\n") is None


EV1_PRED = json.loads((DATA / "26klegjq7mhi.ev1.pred.json").read_text())
EV1_RECORD = json.loads((DATA / "26klegjq7mhi.ev1.extraction.json").read_text())


def _evidence_bundle(world):
    """The record imported, then an evidence bundle of it pushed and submitted."""
    from ingestion_workflow.catalog import Run, RunKind
    from ingestion_workflow.services.offload.importer import import_run

    sent = json.loads((DATA / "paper-26klegjq7mhi/parse/coordinate_parse.json").read_text())
    import_run(world["catalog"], Run("ns-27b-e-20261031", RunKind.MODEL, "record", "ns-27b",
                                     model="Qwen/Qwen3.8-27B", created_at="2026-10-31"),
               [{"article_id": ARTICLE, "status": "ok", "text_sha256": sent["text_sha256"],
                 "record": RECORD}])
    spec = load_spec(SPEC_FILE.with_name("ns-ev1.json")).model_copy(
        update={"adapter_sha256": hashlib.sha256(b"weights").hexdigest(),
                "model_revision": load_spec(SPEC_FILE).model_revision, "model": "Qwen/Qwen3.8-27B"})
    bundle = export(world["ctx"], spec, "vista", workers=1, now=NOW + timedelta(minutes=1)).bundle
    R.push(world["remote"], bundle, hf_cache=Path("/nonexistent"))
    R.submit(world["remote"], bundle, yes=False)
    (job,) = BundleDir(bundle).state().jobs.values()
    there = Path(world["remote"].bundle_dir(BundleDir(bundle).bundle()))
    (there / "results").mkdir(exist_ok=True)
    (there / "results/s000.jsonl").write_text(json.dumps(_result(
        prediction=EV1_PRED["prediction"], prompt_tokens=EV1_PRED["prompt_tokens"])) + "\n")
    world["cluster"].states[job] = "COMPLETED"
    return bundle


def test_pull_merges_ev1_s_answer_into_the_record_as_the_ev1_set_did(world):
    bundle = _evidence_bundle(world)
    pull(world["remote"], bundle, world["catalog"])
    kept = world["catalog"].artifact(ARTICLE, "evidence", "ns-27b")
    assert kept.ok, kept.error
    merged = world["catalog"].payload(kept)
    strip = ("local_id", "extraction_metadata")
    assert {k: v for k, v in merged.items() if k not in strip} == \
        {k: v for k, v in as_kept(EV1_RECORD).items() if k not in strip}
    # the record's maker stays its provenance
    assert merged["extraction_metadata"]["extractor_version"] == "ns-27b-e-20261031"


def test_evidence_of_a_record_replaced_since_export_is_refused(world):
    from ingestion_workflow.catalog import Run, RunKind
    from ingestion_workflow.services.offload.importer import RECORD_MOVED, import_run

    bundle = _evidence_bundle(world)
    newer = json.loads(json.dumps(RECORD))
    newer["analyses"][0]["name"]["value"] = "a later run's name"
    sent = json.loads((DATA / "paper-26klegjq7mhi/parse/coordinate_parse.json").read_text())
    import_run(world["catalog"], Run("ns-27b-e-20261105", RunKind.MODEL, "record", "ns-27b",
                                     model="Qwen/Qwen3.8-27B", prompt_version="v6",
                                     created_at="2026-11-05"),
               [{"article_id": ARTICLE, "status": "ok", "text_sha256": sent["text_sha256"],
                 "record": newer}])
    pull(world["remote"], bundle, world["catalog"])
    assert world["catalog"].artifact(ARTICLE, "evidence", "ns-27b").error == RECORD_MOVED
