"""`ingest offload export`: write a bundle of the articles a stage's source is not fresh for.

The articles are those whose sync holds at least one analysis (or `--articles`), less the
ones whose artifact of the spec's stage and source is fresh; for evidence, also those
without a current record of the source (a skipped record makes skipped evidence). Each
request is built as the model was trained and run (pondie_astra): the record request from
the paper's own parse (`prompt`), the evidence request from its text in units and its
current record (`evidence`), whose full record the bundle keeps in records/ for pull to
merge into. The text is the parse's own text file, read with universal newlines; an
article whose text no longer hashes to the parse's `text_sha256` is left out, since import
would refuse it. Requests longer
than the spec's `max_input_tokens` go to skipped.jsonl. The rest are sharded as
`shard_ns.py` did: longest first, each to the shard with the least input that has room.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple

from ingestion_workflow.catalog import Catalog, Run, RunKind, Status
from ingestion_workflow.pipeline.stage import Context, recorded_path
from ingestion_workflow.pipeline.stages.offloaded import (
    EvidenceStage,
    RecordStage,
    parse_keys,
    record_digest,
)

from .contract import Bundle, BundleDir, InputRow, RunSpec, Shard

#: Why an article is left out of a bundle; counted, never written.
NO_SYNC = "no sync"
NO_PARSE = "no readable parse"
NO_ANALYSES = "parse has no analyses"
TEXT_MOVED = "text no longer hashes to the parse's text_sha256"
NO_CURRENT_RECORD = "no current record of the source"
#: The reason evidence is skipped for a record that was.
RECORD_SKIPPED = "the record was skipped"
#: The reason an over-long request is imported with, as skipped.
OVER = "over {} input tokens"


@dataclass
class ExportReport:
    bundle: Optional[Path] = None
    articles: int = 0
    shards: List[Shard] = field(default_factory=list)
    skipped: int = 0
    fresh: int = 0
    left_out: Counter = field(default_factory=Counter)

    def lines(self) -> List[str]:
        sent = sum(s.articles for s in self.shards)
        out = [f"bundle {self.bundle}" if self.bundle else "nothing to export",
               f"  {self.articles:,} articles: {sent:,} in {len(self.shards)} shards, "
               f"{self.skipped:,} skipped as too long, {self.fresh:,} already fresh"]
        out += [f"  left out {n:,}: {why}" for why, n in self.left_out.most_common()]
        if self.shards:
            tokens = [s.input_tokens for s in self.shards]
            out.append(f"  input tokens per shard {min(tokens):,}-{max(tokens):,}")
        return out


def ns_pond_commit() -> str:
    """This checkout's commit, marked when it has uncommitted changes."""
    here = Path(__file__).resolve().parent
    try:
        commit = subprocess.run(["git", "rev-parse", "--short=12", "HEAD"], cwd=here,
                                capture_output=True, text=True, check=True).stdout.strip()
        dirty = subprocess.run(["git", "status", "--porcelain", "--untracked-files=no"],
                               cwd=here, capture_output=True, text=True, check=True).stdout
    except (OSError, subprocess.CalledProcessError):
        return "unknown"
    return f"ns-pond@{commit}" + ("+dirty" if dirty.strip() else "")


def _text(root: Path):
    """(parse, parsed paper, text, text_sha256) of a paper, or the reason it has none to read."""
    try:
        parse = json.loads((root / "parse" / "coordinate_parse.json").read_text("utf-8"))
        parsed = json.loads((root / "parse" / "parsed_paper.json").read_text("utf-8"))
        raw = (root / parsed["text_path"]).read_bytes()
    except (OSError, ValueError, KeyError):
        return NO_PARSE
    text_sha256 = hashlib.sha256(raw).hexdigest()
    if text_sha256 != parse.get("text_sha256"):
        return TEXT_MOVED
    text = raw.decode("utf-8").replace("\r\n", "\n").replace("\r", "\n")
    return parse, parsed, text, text_sha256


def _row(job, text_sha256, parse_keys, n_in, content, kwargs, record_digest=None) -> dict:
    article_id, _, base_study_id, _, serve, _ = job
    room = serve["max_len"] - n_in - serve["reserve"]
    return InputRow(article_id=article_id, base_study_id=base_study_id, text_sha256=text_sha256,
                    parse_keys=parse_keys, record_digest=record_digest, n_input_tokens=n_in,
                    content=content, chat_template_kwargs=kwargs,
                    max_tokens=max(256, min(serve["max_tokens"], room))).model_dump()


def build_record(job) -> Tuple[Optional[dict], str]:
    """A record request: the paper prompt from the paper's own parse. Runs in a worker."""
    from pondie_astra import prompt

    read = _text(Path(job[1]))
    if isinstance(read, str):
        return None, read
    parse, parsed, text, text_sha256 = read
    entries = [e for e in parse.get("analyses") or [] if e.get("key")]
    if not entries:
        return None, NO_ANALYSES
    caps = prompt.captions(parsed.get("tables"), entries)
    listed = {e["key"]: dict(e, **caps.get(e["key"], {})) for e in entries}
    paper_prompt = prompt.prompt_of(text, listed)
    template = prompt.template()
    content, kwargs = prompt.user_content(paper_prompt, template, job[3])
    n_in = prompt.ntok(prompt.user_turn(paper_prompt, template))
    return _row(job, text_sha256, list(listed), n_in, content, kwargs), ""


def build_evidence(job) -> Tuple[Optional[dict], str]:
    """An evidence request: the paper's text in numbered units and its current record."""
    from pondie_astra import evidence, prompt

    read = _text(Path(job[1]))
    if isinstance(read, str):
        return None, read
    _, _, text, text_sha256 = read
    record = job[5]
    request, _ = evidence.prompt_of(record, text)
    content, kwargs = prompt.user_content(request, evidence.TEMPLATE, job[3])
    return _row(job, text_sha256, sorted(parse_keys(record)), evidence.n_input_tokens(request),
                content, kwargs, record_digest(record)), ""


BUILDERS = {"record": build_record, "evidence": build_evidence}


def shard(rows: Sequence[dict], size: int) -> List[List[dict]]:
    """`shard_ns.py`'s split: longest first, each to the least-loaded shard with room."""
    count = (len(rows) + size - 1) // size
    shards: List[List[dict]] = [[] for _ in range(count)]
    load = [0] * count
    for row in sorted(rows, key=lambda r: -r["n_input_tokens"]):
        k = min(range(count), key=lambda j: load[j] if len(shards[j]) < size else float("inf"))
        shards[k].append(row)
        load[k] += row["n_input_tokens"]
    return shards


def _map(fn, jobs: List[tuple], workers: int) -> Iterable:
    """`fn` over `jobs`, in worker processes when there are more than one."""
    if workers <= 1:
        return map(fn, jobs)
    with ProcessPoolExecutor(workers) as pool:
        return list(pool.map(fn, jobs, chunksize=8))


def candidates(catalog: Catalog, articles: Optional[Iterable[str]]) -> List[str]:
    if articles is not None:
        return sorted({a for a in articles if catalog.live(a)})
    return sorted(a for a in catalog.article_ids_where_summary("sync", "analyses")
                  if catalog.live(a))


def _due(ctx: Context, spec: RunSpec, ids: List[str], report: ExportReport):
    """{article: its record, for evidence} of the articles the spec's source is not fresh for,
    and the skipped rows of the evidence whose record was skipped."""
    catalog = ctx.catalog
    stage = (RecordStage if spec.stage == "record" else EvidenceStage)(ctx.settings)
    fresh = stage.fresh_sources(ctx, ids, catalog.artifacts(ids, stage.name),
                                catalog.artifacts(ids, stage.requires))
    todo = [a for a in ids if spec.source not in fresh.get(a, ())]
    report.fresh = len(ids) - len(todo)
    if spec.stage == "record":
        return {a: None for a in todo}, []
    records = catalog.artifacts(todo, "record")
    current = RecordStage(ctx.settings).fresh_sources(ctx, todo, records,
                                                      catalog.artifacts(todo, "sync"))
    due, skipped = {}, []
    for article_id in todo:
        mine = records.get(article_id, {}).get(spec.source)
        if mine is None or spec.source not in current.get(article_id, ()):
            report.left_out[NO_CURRENT_RECORD] += 1
        elif mine.status is Status.SKIPPED:
            skipped.append({"article_id": article_id, "status": "skipped",
                            "reason": RECORD_SKIPPED, "record": None})
        else:
            due[article_id] = ctx.payload(mine)
    return due, skipped


def export(ctx: Context, spec: RunSpec, machine: str, *, articles: Optional[Iterable[str]] = None,
           limit: int = 0, workers: int = 8, now: Optional[datetime] = None) -> ExportReport:
    if spec.stage not in BUILDERS:
        raise ValueError(f"the {spec.stage} stage does not export; {', '.join(BUILDERS)} do")
    catalog = ctx.catalog
    report = ExportReport()
    due, skipped = _due(ctx, spec, candidates(catalog, articles), report)
    todo = list(due)[:limit] if limit else list(due)
    report.articles = len(todo) + len(skipped)
    synced = catalog.artifacts(todo, "sync")
    jobs = []
    for article_id in todo:
        sync = synced.get(article_id, {}).get("")
        if sync is None or not sync.ok or not sync.summary.get("path"):
            report.left_out[NO_SYNC] += 1
            continue
        jobs.append((article_id, str(recorded_path(catalog.root, sync.summary["path"])),
                     sync.summary.get("base_study_id"), spec.family, spec.serve.model_dump(),
                     due[article_id]))
    built: List[dict] = []
    for row, why in _map(BUILDERS[spec.stage], jobs, workers):
        if row is None:
            report.left_out[why] += 1
        elif row["n_input_tokens"] > spec.max_input_tokens:
            skipped.append({"article_id": row["article_id"], "status": "skipped",
                            "reason": OVER.format(spec.max_input_tokens),
                            "text_sha256": row["text_sha256"], "record": None})
        else:
            built.append(row)
    report.skipped = len(skipped)
    if not built and not skipped:
        return report
    now = now or datetime.now(timezone.utc)
    bundle_id = f"{spec.version}-{now:%Y%m%d-%H%M}"
    out = BundleDir(catalog.root / "offload" / bundle_id)
    for part in ("inputs", "results", "records"):
        (out.root / part).mkdir(parents=True, exist_ok=part != "inputs")
    shards = []
    for k, rows in enumerate(shard(built, spec.shard_size)):
        name = f"s{k:03d}"
        with out.inputs(name).open("w", encoding="utf-8") as handle:
            handle.writelines(json.dumps(r, ensure_ascii=False) + "\n" for r in rows)
        if spec.stage == "evidence":
            with out.records(name).open("w", encoding="utf-8") as handle:
                handle.writelines(json.dumps({"article_id": r["article_id"],
                                              "record": due[r["article_id"]]}) + "\n"
                                  for r in rows)
        shards.append(Shard(name=name, articles=len(rows),
                            input_tokens=sum(r["n_input_tokens"] for r in rows)))
    with out.skipped.open("w", encoding="utf-8") as handle:
        handle.writelines(json.dumps(r) + "\n" for r in skipped)
    config = {"serve": spec.serve.model_dump(), "family": spec.family,
              "model_revision": spec.model_revision, "adapter_sha256": spec.adapter_sha256,
              "shard_size": spec.shard_size, "max_input_tokens": spec.max_input_tokens,
              "time": spec.time, "shards": len(shards)}
    commit = ns_pond_commit()
    made = Run("", RunKind.MODEL, spec.stage, spec.source, pipeline=spec.pipeline,
               version=spec.version, model=spec.model, adapter=spec.adapter,
               prompt_version=spec.prompt_version, schema_version=spec.schema_version,
               code_commit=commit, machine=machine, config=config,
               bundle_id=bundle_id, created_at=now.isoformat(timespec="seconds"))
    # A rerun of a kept run (its missing papers, say) is that run, as it was first kept.
    made = catalog.run_like(made) or replace(made, run_id=made.minted_id)
    run = {**asdict(made), "kind": made.kind.value, "imported_at": None}
    bundle = Bundle(bundle_id=bundle_id, machine=machine, ns_pond_commit=commit,
                    created_at=run["created_at"], run=run, spec=spec, shards=shards,
                    skipped=len(skipped))
    out.manifest.write_text(bundle.model_dump_json(indent=1), "utf-8")
    report.bundle, report.shards = out.root, shards
    return report
