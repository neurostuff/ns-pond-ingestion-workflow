"""The normalize stage's model calls, offloaded: a bundle of task prompts, and their answers
pulled into the answer store.

`export` writes one input row per task prompt the normalize stage would send and the answer
store does not hold (`InputRow.key` is the prompt's `design.key_of`); `answers` reads a
shard's results back into the store. The stage then runs here with no server
(`normalize_model.url` unset) and reads every answer from the store. The catalog is written
only by that run, never by pull.

The spec's `version` is the served name the settings' `normalize_model` names, so the
answers are the ones that model would give."""

from __future__ import annotations

from dataclasses import asdict, replace
from datetime import datetime, timezone
from typing import Iterable, List, Optional

from ingestion_workflow.catalog import Run, RunKind
from ingestion_workflow.pipeline.stage import Context
from ingestion_workflow.pipeline.stages.offloaded import NO_PARSE, NormalizeStage

from .contract import Bundle, BundleDir, InputRow, ResultRow, RunSpec, write_rows
from .export import ExportReport, candidates, ns_pond_commit, shard

#: A prompt's tokens, estimated from its characters for sharding (prompts run about 3.9
#: characters a token).
CHARS_PER_TOKEN = 4


def _prompts_one(job):
    """((article, text_sha256, {task id: prompt}), None), or (None, why)."""
    from ingestion_workflow.normalization import api
    from ingestion_workflow.services import normalize_assign as na

    record, text, article_id, parse, model, _version = job
    try:
        held = na.load_parse(parse)
        record = na.renormalize(record, parse, text)
        prompts = api.prompts(record, text, model)
        return (article_id, (held or {}).get("text_sha256") or "", prompts), None
    except Exception as exc:  # noqa: BLE001 - this article's failure
        return None, f"{type(exc).__name__}: {exc}"


def _check(ctx: Context, spec: RunSpec):
    model = getattr(ctx.settings, "normalize_model", None)
    if spec.stage != "normalize":
        raise ValueError(f"a {spec.stage} spec is not a normalize spec")
    if model is None or model.name != spec.version:
        raise ValueError(f"the spec asks {spec.version}, but normalize_model names "
                         f"{model.name if model else 'no model'}")
    return model


def export(ctx: Context, spec: RunSpec, machine: str, *, articles: Optional[Iterable[str]] = None,
           limit: int = 0, now: Optional[datetime] = None) -> ExportReport:
    """A bundle of the prompts of every article normalize has due that the answer store has no
    answer for."""
    from ingestion_workflow.normalization import design
    from ingestion_workflow.services import normalize_assign as na

    model = _check(ctx, spec)
    catalog, report = ctx.catalog, ExportReport()
    stage = NormalizeStage(ctx.settings)
    ids = candidates(catalog, articles)
    plan = stage.plan(ctx, [catalog.ref(a) for a in ids], catalog.artifacts(ids, stage.name),
                      catalog.artifacts(ids, stage.requires))
    works = plan.pending[:limit] if limit else plan.pending
    report.fresh = plan.fresh
    report.articles = len(works)
    synced = catalog.artifacts(sorted({w.article_id for w in works}), "sync")
    jobs = []
    for work in works:
        sync = synced.get(work.article_id, {}).get("")
        if sync is None or not sync.ok:
            jobs.append((work.article_id, None))
            continue
        jobs.append((work.article_id, (catalog.payload(work.upstream),
                                       na.text_of(catalog.root, sync), work.article_id,
                                       na.parse_of(catalog.root, sync), model.name, None)))
    store = stage.answer_store(ctx)
    rows, seen = [], set()
    try:
        for article_id, job in jobs:
            got, why = (None, NO_PARSE) if job is None else _prompts_one(job)
            if got is None:
                report.left_out[why] += 1
                continue
            _, text_sha256, prompts = got
            for prompt in prompts.values():
                key = design.key_of(prompt, model.name)
                if key in seen or store.get(key) is not None:
                    continue
                seen.add(key)
                kwargs = design.request(prompt, model.name)["chat_template_kwargs"]
                rows.append(InputRow(article_id=article_id, key=key, text_sha256=text_sha256,
                                     parse_keys=[], n_input_tokens=len(prompt) // CHARS_PER_TOKEN,
                                     content=prompt, chat_template_kwargs=kwargs,
                                     max_tokens=design.MAX_COMPLETION_TOKENS).model_dump())
    finally:
        store.close()
    if not rows:
        return report
    now = now or datetime.now(timezone.utc)
    bundle_id = f"{spec.version}-{now:%Y%m%d-%H%M}"
    out = BundleDir(catalog.root / "offload" / bundle_id)
    (out.root / "inputs").mkdir(parents=True)
    (out.root / "results").mkdir()
    shards = []
    from .contract import Shard

    for k, part in enumerate(shard(rows, spec.shard_size)):
        name = f"s{k:03d}"
        write_rows(out.inputs(name), part)
        shards.append(Shard(name=name, articles=len(part),
                            input_tokens=sum(r["n_input_tokens"] for r in part)))
    write_rows(out.skipped, [])
    made = Run("", RunKind.MODEL, "normalize", spec.source, pipeline=spec.pipeline,
               version=spec.version, model=spec.model, prompt_version=spec.prompt_version,
               schema_version=spec.schema_version, code_commit=ns_pond_commit(), machine=machine,
               bundle_id=bundle_id,
               config={"serve": spec.serve.model_dump(), "weights": spec.weights,
                       "weights_sha256": spec.weights_sha256, "prompts": len(rows),
                       "shards": len(shards)},
               created_at=now.isoformat(timespec="seconds"))
    made = replace(made, run_id=made.minted_id)
    run = {**asdict(made), "kind": made.kind.value, "imported_at": None}
    bundle = Bundle(bundle_id=bundle_id, machine=machine, ns_pond_commit=made.code_commit,
                    created_at=run["created_at"], run=run, spec=spec, shards=shards, skipped=0)
    out.manifest.write_text(bundle.model_dump_json(indent=1), "utf-8")
    report.bundle, report.shards = out.root, shards
    return report


def finish_of(res: ResultRow):
    """(finish reason, content) of a result the store keeps, or None for a request that failed
    and is asked again."""
    from ingestion_workflow.services.normalize_assign import OVER

    if res.finish_reason:
        return res.finish_reason, res.content
    error = (res.error or "").lower()
    if error.startswith("http 400") and any(w in error
                                            for w in ("context length", "maximum context")):
        return OVER, None
    return None


def answers(results: List[ResultRow], store, model: str, bundle_id: str) -> dict:
    """A shard's results into the answer store; how many were kept and how many failed."""
    kept = failed = 0
    for res in results:
        got = finish_of(res)
        if got is None:
            failed += 1
            continue
        store.put(res.key, model, got[0], got[1], res.prompt_tokens, res.completion_tokens,
                  source=f"{bundle_id}:{res.article_id}")
        kept += 1
    return {"answers": kept, "failed": failed}


__all__ = ["CHARS_PER_TOKEN", "answers", "export", "finish_of"]
