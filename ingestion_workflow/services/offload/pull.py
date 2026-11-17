"""`ingest offload pull`: fetch each shard whose job completed, and import it.

A shard is imported once, whole: every input row must have a result. A record answer is
restored to an extraction record with the schema the model was trained on
(pondie_astra.compact.from_compact); an evidence answer is merged into the record its
request was built from (records/<shard>.jsonl), over the paper's text as the parse holds it
now, and the importer refuses it when that text or the article's record has moved since.
A row with no answer (cut at the length limit, or
not JSON) is skipped, which counts as done; a request that failed is recorded failed and
blocks upload until the shard is submitted again (`submit --again`, which resumes it). A
shard whose job ended any other way is reported and left for that resubmit.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any, Dict, Iterator, List, Mapping, Optional

from ingestion_workflow.catalog import Catalog, Run, RunKind
from ingestion_workflow.pipeline.stage import recorded_path
from ingestion_workflow.pipeline.stages.offloaded import NO_ANSWER

from .contract import BundleDir, ResultRow
from .importer import import_run
from .remote import Remote, elapsed_s, job_states

REQUEST_FAILED = "request failed: {}"
RESTORE_FAILED = "restore failed: {}"


def run_of(bundle) -> Run:
    return Run(**{**bundle.run, "kind": RunKind(bundle.run["kind"])})


def _no_answer(res: ResultRow) -> Optional[Dict[str, Any]]:
    """The status and reason of a row without a usable answer; None when it has one."""
    if res.prediction is not None and not res.error:
        return None
    if res.finish_reason == "length" or (res.error or "").startswith("invalid_json"):
        why = (res.error or res.finish_reason)[:60]
        return {"status": "skipped", "reason": NO_ANSWER.format(why)}
    return {"status": "failed", "reason": REQUEST_FAILED.format((res.error or "")[:200])}


def record_rows(results: List[ResultRow], inputs: List[dict], context) -> Iterator[dict]:
    """A record shard's results as import rows."""
    from pondie_astra import compact, prompt

    for res in results:
        row: Dict[str, Any] = {"article_id": res.article_id, "text_sha256": res.text_sha256,
                               "record": None, **(_no_answer(res) or {})}
        if "status" not in row:
            try:
                row.update(status="ok", record=compact.from_compact(
                    res.prediction, prompt.schema_index(), res.article_id))
            except Exception as exc:  # noqa: BLE001 - a malformed answer is the row's failure
                row.update(status="failed",
                           reason=RESTORE_FAILED.format(f"{type(exc).__name__}: {exc}"[:200]))
        yield row


def evidence_rows(results: List[ResultRow], inputs: List[dict], context) -> Iterator[dict]:
    """An evidence shard's results as import rows: each answer merged into its record."""
    from pondie_astra import evidence

    from .export import _text

    records, papers = context
    by_input = {r["article_id"]: r for r in inputs}
    for res in results:
        sent = by_input[res.article_id]
        row: Dict[str, Any] = {"article_id": res.article_id, "text_sha256": res.text_sha256,
                               "record_digest": sent["record_digest"],
                               "record": records[res.article_id], **(_no_answer(res) or {})}
        if "status" not in row:
            read = _text(papers[res.article_id]) if res.article_id in papers else "no sync"
            row["status"] = "ok"
            if not isinstance(read, str) and read[3] == res.text_sha256:
                row["record"] = copy.deepcopy(records[res.article_id])
                evidence.merge(row["record"], read[2], res.prediction)
            # else: the text moved since; the importer refuses the row for it
        yield row


ROWS = {"record": record_rows, "evidence": evidence_rows}


def _context(held: BundleDir, shard: str, stage: str, catalog: Catalog,
             inputs: List[dict]):
    """What a stage's rows need beyond the results: for evidence, the records the requests
    were built from and where each paper is now."""
    if stage != "evidence":
        return None
    records = {}
    for line in held.records(shard).read_text("utf-8").splitlines():
        row = json.loads(line)
        records[row["article_id"]] = row["record"]
    ids = [r["article_id"] for r in inputs]
    synced = catalog.artifacts(ids, "sync")
    papers: Mapping[str, Path] = {
        a: recorded_path(catalog.root, synced[a][""].summary["path"]) for a in ids
        if synced.get(a, {}).get("") is not None and synced[a][""].summary.get("path")}
    return records, papers


def pull(remote: Remote, root, catalog: Catalog) -> List[str]:
    held = BundleDir(root)
    bundle, state = held.bundle(), held.state()
    run = run_of(bundle)
    said: List[str] = []
    if not state.skipped_imported:
        rows = [json.loads(line) for line in held.skipped.read_text("utf-8").splitlines()]
        report = import_run(catalog, run, rows)
        state.skipped_imported = True
        held.save(state)
        said.append(f"skipped: {report.rows:,} imported")
    states = job_states(remote, {s: j for s, j in state.jobs.items() if s not in state.imported})
    for shard, job in sorted(states.items()):
        if job["state"] != "COMPLETED":
            said.append(f"{shard}: {job['state']}" + (
                "" if job["state"] in ("PENDING", "RUNNING") else
                f" (exit {job['exit']}); `submit --again {shard}` resumes it"))
            continue
        remote.get(f"{remote.bundle_dir(bundle)}/results/{shard}.jsonl", held.results(shard))
        inputs = [json.loads(line) for line in held.inputs(shard).read_text("utf-8").splitlines()]
        results = {}
        for line in held.results(shard).read_text("utf-8").splitlines():
            res = ResultRow(**json.loads(line))
            results[res.article_id] = res
        missing = [r["article_id"] for r in inputs if r["article_id"] not in results]
        if missing:
            said.append(f"{shard}: {len(missing)} of {len(inputs)} rows have no result; "
                        f"`submit --again {shard}` resumes it")
            continue
        rows = ROWS[bundle.spec.stage]([results[r["article_id"]] for r in inputs], inputs,
                                       _context(held, shard, bundle.spec.stage, catalog, inputs))
        report = import_run(catalog, run, rows)
        state.imported[shard] = {**dict(report.recorded), "elapsed_s": elapsed_s(job["elapsed"])}
        held.save(state)
        said += [f"{shard}: " + report.lines()[1].strip()] + report.lines()[2:]
    left = [s.name for s in bundle.shards if s.name not in state.imported]
    said.append(f"{len(state.imported)} of {len(bundle.shards)} shards imported"
                + (f"; waiting on {', '.join(left[:8])}" + ("..." if len(left) > 8 else "")
                   if left else ""))
    return said
