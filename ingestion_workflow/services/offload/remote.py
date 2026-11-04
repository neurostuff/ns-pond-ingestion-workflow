"""A bundle on a cluster: `ingest offload push`, `submit`, `status` and `setup`.

Every command goes over the machine's SSH alias with BatchMode, so it reuses the
connection you opened and never prompts: when it is closed the command stops, and the
bundle stays where it was. On the machine:

    <work>/bundles/<bundle_id>/            the bundle, job.py, merge.py, cluster.json, logs/
    <work>/adapters/<repo>/<commit>/       the adapter, as the Hub holds it at that commit
    <scratch>/merged/<repo>@<commit>/      the merged model, made by the first shard to need it
    <hf_home>/hub/models--<org>--<name>/snapshots/<revision>/   the base model
"""

from __future__ import annotations

import hashlib
import json
import re
import shlex
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence

from ingestion_workflow.config import Machine

from .contract import Bundle, BundleDir

CLUSTER = Path(__file__).resolve().parent / "cluster"
#: Slurm states after which a job has nothing more to write.
ENDED = ("COMPLETED", "FAILED", "TIMEOUT", "CANCELLED", "OUT_OF_MEMORY", "NODE_FAIL",
         "PREEMPTED", "BOOT_FAIL", "DEADLINE")
ADAPTER_FILES = ("adapter_config.json", "adapter_model.safetensors")


class RemoteError(RuntimeError):
    pass


Runner = Callable[..., subprocess.CompletedProcess]


class Remote:
    """Commands on one machine, through its SSH alias."""

    def __init__(self, name: str, machine: Machine, runner: Runner = subprocess.run) -> None:
        self.name, self.machine, self._run = name, machine, runner

    def _ssh(self) -> List[str]:
        return ["ssh", "-o", "BatchMode=yes", self.machine.ssh]

    def run(self, command: str, timeout: int = 120) -> str:
        done = self._run(self._ssh() + [command], capture_output=True, text=True,
                         timeout=timeout)
        if done.returncode:
            raise RemoteError(f"{self.machine.ssh}: `{command[:80]}` exited {done.returncode}: "
                              f"{(done.stderr or done.stdout).strip()[-400:]}")
        return done.stdout

    def _rsync(self, args: Sequence[str], timeout: int) -> None:
        done = self._run(["rsync", "-a", "-e", "ssh -o BatchMode=yes", *args],
                         capture_output=True, text=True, timeout=timeout)
        if done.returncode:
            raise RemoteError(f"rsync {' '.join(args[-2:])} exited {done.returncode}: "
                              f"{done.stderr.strip()[-400:]}")

    def put(self, local: Path, remote: str, *, exclude: Sequence[str] = (),
            timeout: int = 3600) -> None:
        excluded = [f"--exclude={e}" for e in exclude]
        self._rsync([*excluded, f"{local}/", f"{self.machine.ssh}:{remote}/"], timeout)

    def get(self, remote: str, local: Path, timeout: int = 3600) -> None:
        self._rsync([f"{self.machine.ssh}:{remote}", str(local)], timeout)

    # -- where things are ----------------------------------------------------

    def bundle_dir(self, bundle: Bundle) -> str:
        return f"{self.machine.work}/bundles/{bundle.bundle_id}"

    def adapter_dir(self, adapter: str) -> str:
        repo, commit = split_ref(adapter)
        return f"{self.machine.work}/adapters/{repo.replace('/', '__')}/{commit}"

    def merged_dir(self, adapter: str) -> str:
        repo, commit = split_ref(adapter)
        return f"{self.machine.scratch}/merged/{repo.replace('/', '__')}@{commit}"

    def base_dir(self, bundle: Bundle) -> str:
        org, name = bundle.spec.model.split("/", 1)
        return (f"{self.machine.hf_home}/hub/models--{org}--{name}/snapshots/"
                f"{bundle.spec.model_revision}")


def split_ref(adapter: str):
    repo, sep, commit = adapter.partition("@")
    if not sep or not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise ValueError(f"adapter {adapter!r} is not repo@<40-hex commit>")
    return repo, commit


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 24), b""):
            digest.update(block)
    return digest.hexdigest()


def fetch_adapter(adapter: str, expected_sha256: str, cache: Path,
                  token: Optional[str]) -> Path:
    """The adapter's files at its commit, downloaded here and checked against the bundle."""
    from huggingface_hub import snapshot_download

    repo, commit = split_ref(adapter)
    local = Path(snapshot_download(repo, revision=commit, allow_patterns=list(ADAPTER_FILES),
                                   cache_dir=str(cache), token=token))
    found = _sha256(local / "adapter_model.safetensors")
    if found != expected_sha256:
        raise RemoteError(f"{adapter}: sha256 {found}, the spec names {expected_sha256}")
    return local


def push(remote: Remote, root: Path, *, hf_cache: Path, hf_token: Optional[str]) -> List[str]:
    """Copy the bundle, its job scripts and (once per commit) its adapter to the machine."""
    held = BundleDir(root)
    bundle = held.bundle()
    spec, said = bundle.spec, []
    target = remote.adapter_dir(spec.adapter)
    there = remote.run(f"sha256sum {shlex.quote(target)}/adapter_model.safetensors "
                       f"2>/dev/null || true").split()
    if there[:1] != [spec.adapter_sha256]:
        local = fetch_adapter(spec.adapter, spec.adapter_sha256, hf_cache, hf_token)
        staged = root / ".adapter"
        staged.mkdir(exist_ok=True)
        for name in ADAPTER_FILES:   # the Hub cache holds symlinks; rsync copies files
            shutil.copyfile(local / name, staged / name)
        remote.run(f"mkdir -p {shlex.quote(target)}")
        remote.put(staged, target)
        shutil.rmtree(staged)
        said.append(f"adapter {spec.adapter} -> {target}")
    base = remote.base_dir(bundle)
    remote.run(f"test -f {shlex.quote(base)}/config.json || "
               f"{{ echo 'no base model at {base}' >&2; exit 1; }}")
    for script in ("job.py", "merge.py"):
        shutil.copyfile(CLUSTER / script, root / script)
    cluster = {"adapter_dir": target, "merged_dir": remote.merged_dir(spec.adapter),
               "base_dir": base, "train_python": remote.machine.train_python,
               "merge_script": f"{remote.bundle_dir(bundle)}/merge.py"}
    (root / "cluster.json").write_text(json.dumps(cluster, indent=1), "utf-8")
    remote.run(f"mkdir -p {shlex.quote(remote.bundle_dir(bundle))}/logs")
    remote.put(root, remote.bundle_dir(bundle), exclude=("results/", "state.json", ".adapter"))
    state = held.state()
    state.pushed_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    held.save(state)
    said.append(f"bundle -> {remote.machine.ssh}:{remote.bundle_dir(bundle)}")
    return said


def sbatch_script(remote: Remote, bundle: Bundle, shard: str, *, partition: str,
                  time: str) -> str:
    """The job script of one shard."""
    m, where = remote.machine, remote.bundle_dir(bundle)
    env = {"HF_HOME": m.hf_home, "HF_HUB_OFFLINE": "1", "TOKENIZERS_PARALLELISM": "false",
           "PYTHONDONTWRITEBYTECODE": "1", **m.environment}
    exports = "\n".join(f'export {k}="{v}"' for k, v in env.items())
    return (f"#!/bin/bash\n#SBATCH -J {bundle.bundle_id}-{shard}\n#SBATCH -p {partition}\n"
            f"#SBATCH -A {m.account}\n#SBATCH -N 1\n#SBATCH -n 1\n#SBATCH -t {time}\n"
            f"#SBATCH -o {where}/logs/%x-%j.out\nset -u\n{exports}\n"
            'mkdir -p "${TRITON_CACHE_DIR:-/tmp}" "${TORCHINDUCTOR_CACHE_DIR:-/tmp}"\n'
            'echo "node $(hostname) job $SLURM_JOB_ID $(date)"\n'
            f"{m.serve_python} -I {where}/job.py {where} {shard}\n"
            'rc=$?; echo "job exit $rc $(date)"; exit $rc\n')


@dataclass
class Estimate:
    shards: int
    node_hours: float
    su: float
    basis: str

    def line(self) -> str:
        return (f"{self.shards} shards, about {self.node_hours:.1f} node-hours, "
                f"{self.su:.1f} SU ({self.basis})")


def _hours(limit: str) -> float:
    days, _, clock = limit.rpartition("-")
    h, m, s = (int(x) for x in clock.split(":"))
    return int(days or 0) * 24 + h + m / 60 + s / 3600


def measured_rate(offload_root: Path, bundle: Bundle) -> Optional[float]:
    """Input tokens per node-hour of the latest other bundle of this version on this machine
    whose shards were imported with their elapsed time."""
    found = []
    for other in sorted(offload_root.glob("*/bundle.json")):
        held = BundleDir(other.parent)
        try:
            them = held.bundle()
        except ValueError:
            continue
        if (them.bundle_id == bundle.bundle_id or them.machine != bundle.machine
                or them.spec.version != bundle.spec.version):
            continue
        tokens = {s.name: s.input_tokens for s in them.shards}
        timed = [(tokens[s], done["elapsed_s"]) for s, done in held.state().imported.items()
                 if done.get("elapsed_s") and s in tokens]
        if timed:
            found.append((them.created_at, sum(t for t, _ in timed)
                          / (sum(e for _, e in timed) / 3600)))
    return max(found)[1] if found else None


def estimate(remote: Remote, root: Path, shards: Sequence[str], limit: str) -> Estimate:
    held = BundleDir(root)
    bundle = held.bundle()
    rate = measured_rate(root.parent, bundle)
    tokens = {s.name: s.input_tokens for s in bundle.shards}
    if rate:
        hours = sum(tokens[s] for s in shards) / rate
        basis = f"{rate:,.0f} input tokens per node-hour measured"
    else:
        hours = len(shards) * _hours(limit)
        basis = f"no run measured yet: each shard's {limit} limit"
    return Estimate(len(shards), hours, hours * remote.machine.su_per_node_hour, basis)


def submit(remote: Remote, root: Path, *, yes: bool, again: Sequence[str] = (),
           partition: Optional[str] = None, time: Optional[str] = None,
           limit_shards: int = 0) -> List[str]:
    """Submit the shards not yet submitted (and `again`), as many as the queue has room for.

    Refused without `yes` when the estimate is over the machine's SU cap."""
    held = BundleDir(root)
    bundle, state = held.bundle(), held.state()
    if state.pushed_at is None:
        raise RemoteError("push the bundle first")
    time = time or bundle.spec.time
    names = [s.name for s in bundle.shards]
    unknown = set(again) - set(names)
    if unknown:
        raise RemoteError(f"no such shards: {', '.join(sorted(unknown))}")
    todo = [s for s in names if s not in state.jobs or s in again]
    todo = todo[:limit_shards] if limit_shards else todo
    if not todo:
        return ["every shard is submitted"]
    guess = estimate(remote, root, todo, time)
    said = [guess.line()]
    if guess.su > remote.machine.su_cap and not yes:
        raise RemoteError(f"{guess.line()}: over {remote.name}'s cap of "
                          f"{remote.machine.su_cap:g} SU; pass --yes to submit")
    queued = len(remote.run("squeue -h -u $USER -o %i").split())
    room = max(0, remote.machine.max_queued - queued)
    where = remote.bundle_dir(bundle)
    for shard in todo[:room]:
        script = sbatch_script(remote, bundle, shard, partition=partition or
                               remote.machine.partition, time=time)
        path = f"{where}/logs/{shard}.sbatch"
        remote.run(f"cat > {shlex.quote(path)} <<'EOF'\n{script}EOF")
        job = remote.run(f"sbatch --parsable {shlex.quote(path)}").strip().split(";")[0]
        if not job.isdigit():
            raise RemoteError(f"{shard}: sbatch said {job!r}")
        state.jobs[shard] = job
        held.save(state)
        said.append(f"{shard}: job {job}")
    if len(todo) > room:
        said.append(f"{len(todo) - room} shards wait for room in the queue "
                    f"({queued} of {remote.machine.max_queued} queued); submit again later")
    return said


def job_states(remote: Remote, jobs: Dict[str, str]) -> Dict[str, Dict[str, str]]:
    """{shard: {"state", "elapsed", "exit"}} of the bundle's jobs, from sacct."""
    if not jobs:
        return {}
    out = remote.run("sacct -X -n -P -o JobID,State,Elapsed,ExitCode -j "
                     + ",".join(jobs.values()))
    by_job = {}
    for line in out.splitlines():
        parts = line.split("|")
        if len(parts) == 4:
            by_job[parts[0]] = {"state": parts[1].split()[0], "elapsed": parts[2],
                                "exit": parts[3]}
    return {shard: by_job.get(job, {"state": "UNKNOWN", "elapsed": "", "exit": ""})
            for shard, job in jobs.items()}


def elapsed_s(elapsed: str) -> int:
    return round(_hours(elapsed) * 3600) if elapsed else 0


def group_pins(group: str) -> Dict[str, str]:
    """{package: version} an optional-dependency group of this checkout's pyproject.toml
    pins. Read from the file, since an editable install's metadata keeps the groups it was
    installed with."""
    import tomllib

    from packaging.requirements import Requirement

    pyproject = Path(__file__).resolve().parents[3] / "pyproject.toml"
    groups = tomllib.loads(pyproject.read_text("utf-8"))["project"]["optional-dependencies"]
    if group not in groups:
        raise ValueError(f"no optional-dependency group {group!r} in {pyproject}")
    return {Requirement(spec).name: str(Requirement(spec).specifier).lstrip("=")
            for spec in groups[group]}


_VERSIONS = ("import importlib.metadata as m, json, sys; "
             "print(json.dumps({p: m.version(p) for p in sys.argv[1:]}))")


def _env_problem(remote: Remote, python: str, group: Optional[str], probe: str) -> str:
    """What the environment holds, or why it does not match its group's pins."""
    pins = group_pins(group) if group else {}
    found = json.loads(remote.run(f"{python} -c {shlex.quote(_VERSIONS)} "
                                  + " ".join(pins or [probe]), timeout=300))
    wrong = {p: v for p, v in found.items() if p in pins and v.split("+")[0] != pins[p]}
    if wrong:
        raise RemoteError(", ".join(f"{p} {v}, {group} pins {pins[p]}" for p, v in wrong.items()))
    return " ".join(f"{p} {v}" for p, v in found.items())


def check(remote: Remote, spec_model: str, spec_revision: str) -> List[str]:
    """`ingest offload setup`: what a job will need on the machine is there."""
    m = remote.machine
    org, name = spec_model.split("/", 1)
    base = f"{m.hf_home}/hub/models--{org}--{name}/snapshots/{spec_revision}"
    probes = {
        "serve environment": lambda: _env_problem(remote, m.serve_python, m.infer_group, "vllm"),
        "train environment": lambda: _env_problem(remote, m.train_python, m.merge_group, "peft"),
        f"base model {spec_model}@{spec_revision[:12]}":
            lambda: remote.run(f"ls {base}/config.json"),
        "work and scratch":
            lambda: remote.run(f"mkdir -p {m.work}/bundles {m.scratch}/merged && echo ok"),
        f"partition {m.partition}": lambda: remote.run(f"sinfo -h -p {m.partition} -o %P"),
    }
    said = []
    for what, probe in probes.items():
        try:
            said.append(f"ok   {what}: {probe().strip()[-120:]}")
        except RemoteError as exc:
            said.append(f"FAIL {what}: {exc}")
    return said
