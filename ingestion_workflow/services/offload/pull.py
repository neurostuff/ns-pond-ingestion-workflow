"""`ingest offload pull`: fetch each shard whose job completed, and import it.

A shard is imported once, whole: every input row must have a result. Its answers are
restored to extraction records with the schema the model was trained on
(pondie_astra.compact.from_compact). A row with no answer (cut at the length limit, or
not JSON) is skipped, which counts as done; a request that failed is recorded failed and
blocks upload until the shard is submitted again (`submit --again`, which resumes it). A
shard whose job ended any other way is reported and left for that resubmit.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Iterator, List

from ingestion_workflow.catalog import Catalog, Run, RunKind

from .contract import BundleDir, ResultRow
from .importer import import_run
from .remote import Remote, elapsed_s, job_states

NO_ANSWER = "no prediction: {}"
REQUEST_FAILED = "request failed: {}"
RESTORE_FAILED = "restore failed: {}"


def run_of(bundle) -> Run:
    return Run(**{**bundle.run, "kind": RunKind(bundle.run["kind"])})


def import_rows(results: List[ResultRow]) -> Iterator[dict]:
    """A shard's results as import rows."""
    from pondie_astra import compact, prompt

    for res in results:
        row: Dict[str, Any] = {"article_id": res.article_id, "text_sha256": res.text_sha256,
                               "record": None}
        if res.prediction is not None and not res.error:
            try:
                record = compact.from_compact(res.prediction, prompt.schema_index(),
                                              res.article_id)
                row.update(status="ok", record=record)
            except Exception as exc:  # noqa: BLE001 - a malformed answer is the row's failure
                row.update(status="failed",
                           reason=RESTORE_FAILED.format(f"{type(exc).__name__}: {exc}"[:200]))
        elif res.finish_reason == "length" or (res.error or "").startswith("invalid_json"):
            row.update(status="skipped",
                       reason=NO_ANSWER.format((res.error or res.finish_reason)[:60]))
        else:
            row.update(status="failed", reason=REQUEST_FAILED.format((res.error or "")[:200]))
        yield row


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
        said.append(f"skipped as too long: {report.rows:,} imported")
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
        report = import_run(catalog, run,
                            import_rows([results[r["article_id"]] for r in inputs]))
        state.imported[shard] = {**dict(report.recorded), "elapsed_s": elapsed_s(job["elapsed"])}
        held.save(state)
        said += [f"{shard}: " + report.lines()[1].strip()] + report.lines()[2:]
    left = [s.name for s in bundle.shards if s.name not in state.imported]
    said.append(f"{len(state.imported)} of {len(bundle.shards)} shards imported"
                + (f"; waiting on {', '.join(left[:8])}" + ("..." if len(left) > 8 else "")
                   if left else ""))
    return said
