"""The bundle: what ns-pond hands a cluster for one run of an offloaded stage, and what it
takes back.

A bundle is a directory, the same on both sides:

    bundle.json              Bundle: the run, its spec, the machine, its shards
    inputs/<shard>.jsonl     InputRow per article, the request already built
    skipped.jsonl            articles left out (over the context), imported as skipped
    records/<shard>.jsonl    the record each evidence request was built from (ns-pond only)
    results/<shard>.jsonl    ResultRow per article, written by the job (cluster/job.py)
    state.json               BundleState: jobs submitted and shards imported (ns-pond only)

The cluster never sees the catalog: everything a job reads is in its shard's inputs, and
everything import checks (the text hash and parse keys) travels with each row.
"""

from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Optional

from pydantic import BaseModel, ConfigDict, Field

#: Bumped when a field changes meaning; a bundle of another version is refused.
CONTRACT_VERSION = 1


class Serve(BaseModel):
    """How the job serves the model and asks it (tacc/eval_record.py's config)."""

    model_config = ConfigDict(extra="forbid")

    max_len: int
    max_tokens: int
    workers: int
    gpu_mem: float
    tp: int = 1
    vllm_args: List[str] = Field(default_factory=list)
    #: What the job leaves of the context for the answer's template wrapping, per family.
    reserve: int


class RunSpec(BaseModel):
    """A run to make: the model and how it is run (configs/offload/<name>.json)."""

    model_config = ConfigDict(extra="forbid")

    stage: str
    source: str
    #: pondie_astra.prompt.user_content's model family: "q27" or "nu".
    family: str
    pipeline: str
    version: str
    model: str
    #: The base model's Hub revision, as the machine's HF_HOME holds it.
    model_revision: str
    #: `repo@commit` on the Hub.
    adapter: str
    adapter_sha256: str
    prompt_version: str
    schema_version: str
    serve: Serve
    shard_size: int = 500
    #: Papers whose request is longer are left out, as skipped.
    max_input_tokens: int
    #: Each shard's job time limit.
    time: str


class Shard(BaseModel):
    name: str
    articles: int
    input_tokens: int


class Bundle(BaseModel):
    model_config = ConfigDict(extra="forbid")

    contract: int = CONTRACT_VERSION
    bundle_id: str
    machine: str
    ns_pond_commit: str
    created_at: str
    #: The `runs` row its results are imported under: the kept run it repeats, or a new one
    #: under its minted_id. Many bundles can make one run.
    run: Dict[str, Any]
    spec: RunSpec
    shards: List[Shard]
    skipped: int


class InputRow(BaseModel):
    article_id: str
    base_study_id: Optional[str] = None
    text_sha256: str
    #: The parse keys the prompt listed (record), or the record names (evidence).
    parse_keys: List[str]
    #: `record_digest` of the record an evidence request was built from.
    record_digest: Optional[str] = None
    n_input_tokens: int
    #: The user message and chat template arguments, as the model is sent them.
    content: str
    chat_template_kwargs: Dict[str, Any]
    max_tokens: int


class ResultRow(BaseModel):
    article_id: str
    text_sha256: str
    #: The model's answer, parsed: a compact record. None when there is no answer.
    prediction: Optional[Any] = None
    finish_reason: Optional[str] = None
    error: Optional[str] = None
    prompt_tokens: Optional[int] = None
    completion_tokens: Optional[int] = None
    wall_s: Optional[float] = None


class BundleState(BaseModel):
    """What has been done with a bundle, kept beside it on the ns-pond side only."""

    pushed_at: Optional[str] = None
    #: {shard: Slurm job id}. A shard is submitted once; `submit --again` resubmits it.
    jobs: Dict[str, str] = Field(default_factory=dict)
    #: {shard: {status: articles recorded}}.
    imported: Dict[str, Dict[str, int]] = Field(default_factory=dict)
    skipped_imported: bool = False


class BundleDir:
    """A bundle's files."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    @property
    def manifest(self) -> Path:
        return self.root / "bundle.json"

    def inputs(self, shard: str) -> Path:
        return self.root / "inputs" / f"{shard}.jsonl"

    def records(self, shard: str) -> Path:
        return self.root / "records" / f"{shard}.jsonl"

    def results(self, shard: str) -> Path:
        return self.root / "results" / f"{shard}.jsonl"

    @property
    def skipped(self) -> Path:
        return self.root / "skipped.jsonl"

    @property
    def state_file(self) -> Path:
        return self.root / "state.json"

    def bundle(self) -> Bundle:
        held = json.loads(self.manifest.read_text("utf-8"))
        if held.get("contract") != CONTRACT_VERSION:
            raise ValueError(f"{self.manifest}: contract {held.get('contract')}, "
                             f"this code reads {CONTRACT_VERSION}")
        return Bundle(**held)

    def state(self) -> BundleState:
        if not self.state_file.exists():
            return BundleState()
        return BundleState(**json.loads(self.state_file.read_text("utf-8")))

    def save(self, state: BundleState) -> None:
        tmp = self.state_file.with_suffix(".tmp")
        tmp.write_text(state.model_dump_json(indent=1), "utf-8")
        tmp.replace(self.state_file)


def read_rows(paths: Iterable[Path | str]) -> Iterator[Dict[str, Any]]:
    """The rows of JSON-lines files, gzipped when named `.gz`. Only a newline ends a row:
    the text in one may hold U+2028, which `str.splitlines` would split on."""
    for path in paths:
        opener = gzip.open if str(path).endswith(".gz") else open
        with opener(path, "rt", encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    yield json.loads(line)


def write_rows(path: Path, rows: Iterable[dict]) -> None:
    """A bundle file, one row per line, its text as it is."""
    with Path(path).open("w", encoding="utf-8") as handle:
        handle.writelines(json.dumps(r, ensure_ascii=False) + "\n" for r in rows)


def load_spec(path: Path) -> RunSpec:
    return RunSpec(**json.loads(Path(path).read_text("utf-8")))
