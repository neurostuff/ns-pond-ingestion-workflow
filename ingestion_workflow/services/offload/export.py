"""`ingest offload export`: write a bundle of the articles a stage's source is not fresh for.

The articles are those whose sync holds at least one analysis (or `--articles`), less the
ones whose `record` artifact of this source is fresh. Each one's request is built from its
own parse as the 27B was trained and run (pondie_astra.prompt): the text is the parse's
own text file, read with universal newlines, and an article whose text no longer hashes
to the parse's `text_sha256` is left out, since import would refuse it. Requests longer
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
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from ingestion_workflow.catalog import Catalog, Run, RunKind
from ingestion_workflow.pipeline.stage import Context, recorded_path
from ingestion_workflow.pipeline.stages.offloaded import RecordStage

from .contract import Bundle, BundleDir, InputRow, RunSpec, Shard

#: Why an article is left out of a bundle; counted, never written.
NO_SYNC = "no sync"
NO_PARSE = "no readable parse"
NO_ANALYSES = "parse has no analyses"
TEXT_MOVED = "text no longer hashes to the parse's text_sha256"
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


def build(job: Tuple[str, str, Optional[str], str, Dict[str, Any]]) -> Tuple[Optional[dict], str]:
    """One article's request, or (None, why it is left out). Runs in a worker process."""
    from pondie_astra import prompt

    article_id, paper, base_study_id, family, serve = job
    root = Path(paper)
    try:
        parse = json.loads((root / "parse" / "coordinate_parse.json").read_text("utf-8"))
        parsed = json.loads((root / "parse" / "parsed_paper.json").read_text("utf-8"))
        raw = (root / parsed["text_path"]).read_bytes()
    except (OSError, ValueError, KeyError):
        return None, NO_PARSE
    entries = [e for e in parse.get("analyses") or [] if e.get("key")]
    if not entries:
        return None, NO_ANALYSES
    text_sha256 = hashlib.sha256(raw).hexdigest()
    if text_sha256 != parse.get("text_sha256"):
        return None, TEXT_MOVED
    text = raw.decode("utf-8").replace("\r\n", "\n").replace("\r", "\n")
    caps = prompt.captions(parsed.get("tables"), entries)
    listed = {e["key"]: dict(e, **caps.get(e["key"], {})) for e in entries}
    paper_prompt = prompt.prompt_of(text, listed)
    template = prompt.template()
    content, kwargs = prompt.user_content(paper_prompt, template, family)
    n_in = prompt.ntok(prompt.user_turn(paper_prompt, template))
    room = serve["max_len"] - n_in - serve["reserve"]
    row = InputRow(article_id=article_id, base_study_id=base_study_id, text_sha256=text_sha256,
                   parse_keys=list(listed), n_input_tokens=n_in, content=content,
                   chat_template_kwargs=kwargs,
                   max_tokens=max(256, min(serve["max_tokens"], room)))
    return row.model_dump(), ""


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


def export(ctx: Context, spec: RunSpec, machine: str, *, articles: Optional[Iterable[str]] = None,
           limit: int = 0, workers: int = 8, now: Optional[datetime] = None) -> ExportReport:
    if spec.stage != "record":
        raise ValueError(f"only the record stage exports so far, not {spec.stage}")
    catalog = ctx.catalog
    report = ExportReport()
    ids = candidates(catalog, articles)
    stage = RecordStage(ctx.settings)
    fresh = stage.fresh_sources(ctx, ids, catalog.artifacts(ids, stage.name),
                                catalog.artifacts(ids, stage.requires))
    todo = [a for a in ids if spec.source not in fresh.get(a, ())]
    report.fresh = len(ids) - len(todo)
    todo = todo[:limit] if limit else todo
    report.articles = len(todo)
    synced = catalog.artifacts(todo, "sync")
    jobs = []
    for article_id in todo:
        sync = synced.get(article_id, {}).get("")
        if sync is None or not sync.ok or not sync.summary.get("path"):
            report.left_out[NO_SYNC] += 1
            continue
        jobs.append((article_id, str(recorded_path(catalog.root, sync.summary["path"])),
                     sync.summary.get("base_study_id"), spec.family, spec.serve.model_dump()))
    built: List[dict] = []
    over: List[dict] = []
    for row, why in _map(build, jobs, workers):
        if row is None:
            report.left_out[why] += 1
        elif row["n_input_tokens"] > spec.max_input_tokens:
            over.append(row)
        else:
            built.append(row)
    report.skipped = len(over)
    if not built and not over:
        return report
    now = now or datetime.now(timezone.utc)
    bundle_id = f"{spec.version}-{now:%Y%m%d-%H%M}"
    out = BundleDir(catalog.root / "offload" / bundle_id)
    (out.root / "inputs").mkdir(parents=True, exist_ok=False)
    (out.root / "results").mkdir()
    shards = []
    for k, rows in enumerate(shard(built, spec.shard_size)):
        name = f"s{k:03d}"
        with out.inputs(name).open("w", encoding="utf-8") as handle:
            handle.writelines(json.dumps(r, ensure_ascii=False) + "\n" for r in rows)
        shards.append(Shard(name=name, articles=len(rows),
                            input_tokens=sum(r["n_input_tokens"] for r in rows)))
    with out.skipped.open("w", encoding="utf-8") as handle:
        for r in over:
            handle.write(json.dumps({"article_id": r["article_id"], "status": "skipped",
                                     "reason": OVER.format(spec.max_input_tokens),
                                     "text_sha256": r["text_sha256"], "record": None}) + "\n")
    config = {"serve": spec.serve.model_dump(), "family": spec.family,
              "model_revision": spec.model_revision, "adapter_sha256": spec.adapter_sha256,
              "shard_size": spec.shard_size, "max_input_tokens": spec.max_input_tokens,
              "time": spec.time, "shards": len(shards)}
    made = Run("", RunKind.MODEL, spec.stage, spec.source, pipeline=spec.pipeline,
               version=spec.version, model=spec.model, adapter=spec.adapter,
               prompt_version=spec.prompt_version, schema_version=spec.schema_version,
               code_commit=ns_pond_commit(), machine=machine, config=config,
               bundle_id=bundle_id, created_at=now.isoformat(timespec="seconds"))
    # A rerun of a kept run (its missing papers, say) is that run, as it was first kept.
    made = catalog.run_like(made) or replace(made, run_id=made.minted_id)
    run = {**asdict(made), "kind": made.kind.value, "imported_at": None}
    bundle = Bundle(bundle_id=bundle_id, machine=machine, ns_pond_commit=run["code_commit"],
                    created_at=run["created_at"], run=run, spec=spec, shards=shards,
                    skipped=len(over))
    out.manifest.write_text(bundle.model_dump_json(indent=1), "utf-8")
    report.bundle, report.shards = out.root, shards
    return report
