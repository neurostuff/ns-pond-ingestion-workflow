"""Post each article's schema-extraction record to neurostore, with the runs that made it."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence, Set, Tuple

from ingestion_workflow.catalog import ArticleRef, Artifact, Outcome, Run, Status, fingerprint

from ..plan import StagePlan, Work
from ..stage import Context, recorded_path
from ..upstream import short_reason
from .offloaded import EvidenceStage, NormalizeStage, RecordStage
from .upload import NO_API_URL, UploadStage

#: 1: the normalized record, sent alone after its parse, with its chain's runs.
RECORD_UPLOAD_VERSION = 1

#: The stages a record passes through before it is sent, in order. A chain is complete when
#: each is fresh, a skipped one counting as done; the last one with output is sent.
CHAIN = (RecordStage, EvidenceStage, NormalizeStage)
#: Why an article has nothing to send.
NO_CHAIN = "waits for a fresh record, evidence and normalization of one source"
NO_PARSE_UPLOADED = "waits for its current parse's upload"
#: The fields of a kept run neurostore does not take: how ns-pond ran it, not what made it.
NOT_SENT = ("config", "bundle_id", "imported_at")
#: The pipeline neurostore files the record's config under.
PIPELINE = "ns-pond"


def sent_run(run: Run) -> Dict[str, Any]:
    """A run as neurostore's `study-schema-run` takes it: as the catalog first kept it."""
    held = {k: v for k, v in asdict(run).items() if k not in NOT_SENT}
    held["kind"] = held["kind"].value if hasattr(held["kind"], "value") else held["kind"]
    return held


def run_id_of(record: Dict[str, Any]) -> str:
    """The upload's run_id: the record itself, so the same record is a replay neurostore
    leaves alone, and a newer one of the same runs replaces it."""
    raw = json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return "ns-pond-" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]


def build_request(record: Dict[str, Any], runs: Sequence[Run], parse_id: str,
                  base_study_id: str) -> Dict[str, Any]:
    """The body of one record upload."""
    return {
        "record": record,
        "parse_id": parse_id,
        "base_study_id": base_study_id,
        "run_id": run_id_of(record),
        "pipeline": {"name": PIPELINE,
                     "version": "+".join(run.version or run.stage for run in runs),
                     "runs": [sent_run(run) for run in runs]},
    }


class RecordUploadStage(UploadStage):
    """Post an article's record once one source's chain (record, evidence, normalization)
    is complete and the article's current parse is uploaded: neurostore attaches a record
    to a stored parse. One source per article, its artifact under that source.

    The record sent is the last one in the chain with output, the normalization's when
    none was skipped; `pipeline.runs` names the runs that made it as the catalog keeps
    them, so neurostore keys its config on them and a rerun of the same runs lands in the
    same config. Answers are treated as `upload`'s are,
    StopRun included; a dry run writes each body under the ingester's dry-run root."""

    name = "record_upload"
    requires = "normalize"

    def __init__(self, settings) -> None:
        super().__init__(settings)
        #: {(article, source): (its study directory, its request)} for the batch being sent.
        self._requests: Dict[Tuple[str, str], Tuple[Path, Dict[str, Any]]] = {}

    def fingerprint_for_chain(self, chain: Sequence[Artifact], parse_id: str) -> str:
        return fingerprint(
            "record_upload", RECORD_UPLOAD_VERSION,
            (self.settings.neurostore_api_url or "").rstrip("/"), parse_id,
            *((a.stage, a.run_id) for a in chain), upstream=chain[-1].fingerprint,
        )

    def _chains(self, ctx: Context, ids: List[str]) -> Dict[str, Tuple[str, List[Artifact]]]:
        """{article: (the source it sends, that source's chain artifacts with output)} for
        the articles with a complete chain. A chain with output at every stage beats one
        with a skipped stage; among equals the first source by name. Sources are never
        mixed."""
        held: Dict[str, Dict[str, Dict[str, Artifact]]] = {}
        done: Dict[str, Set[str]] = {}
        for k, stage_type in enumerate(CHAIN):
            stage = stage_type(self.settings)
            mine = held[stage.name] = ctx.catalog.artifacts(ids, stage.name)
            upstream = held.get(stage.requires) or ctx.catalog.artifacts(ids, stage.requires)
            found = stage.fresh_sources(ctx, ids, mine, upstream)
            for article_id in ids:
                fresh = found.get(article_id, set())
                if k == 0:  # a skipped record has nothing to send
                    fresh = {s for s in fresh if mine[article_id][s].status is Status.OK}
                done[article_id] = fresh if k == 0 else done.get(article_id, set()) & fresh
        chosen = {}
        for article_id in ids:
            chains = [(source, [a for a in (held[s.name][article_id][source] for s in CHAIN)
                                if a.status is Status.OK])
                      for source in sorted(done.get(article_id, ()))]
            if chains:
                chosen[article_id] = max(chains, key=lambda c: len(c[1]))
        return chosen

    def plan(
        self,
        ctx: Context,
        refs: Sequence[ArticleRef],
        artifacts: Dict[str, Dict[str, Artifact]],
        upstream: Dict[str, Dict[str, Artifact]],
    ) -> StagePlan:
        plan = StagePlan(stage=self.name)
        if not self.settings.neurostore_api_url:
            for ref in refs:
                plan.wait(NO_API_URL, ref.id)
            return plan
        ids = [ref.id for ref in refs]
        chosen = self._chains(ctx, ids)
        uploads = ctx.catalog.artifacts(ids, "upload")
        synced = ctx.catalog.artifacts(ids, "sync")
        attempts = {}
        for ref in refs:
            if ref.id not in chosen:
                plan.wait(NO_CHAIN, ref.id)
                continue
            parse_id = _uploaded_parse(synced.get(ref.id, {}).get(""),
                                       uploads.get(ref.id, {}).get(""))
            if parse_id is None:
                plan.wait(NO_PARSE_UPLOADED, ref.id)
                continue
            source, chain = chosen[ref.id]
            fp = self.fingerprint_for_chain(chain, parse_id)
            existing = artifacts.get(ref.id, {}).get(source)
            if ctx.is_fresh(existing, fp):
                plan.fresh += 1
                continue
            if existing is not None and existing.fingerprint == fp:
                if source not in attempts:
                    attempts[source] = ctx.catalog.attempt_counts(ids, self.name, source)
                count, last = attempts[source].get(ref.id, (0, None))
                if not ctx.should_attempt(existing, count, last, self.name, source):
                    plan.permanent += 1
                    continue
            plan.pending.append(Work(ref=ref, source=source, fingerprint=fp,
                                     upstream=chain[-1]))
        return plan

    def execute(self, ctx: Context, works: List[Work]) -> Iterator[Outcome]:
        if self._stopped:
            return
        ids = sorted({w.article_id for w in works})
        chosen = self._chains(ctx, ids)
        uploads = ctx.catalog.artifacts(ids, "upload")
        synced = ctx.catalog.artifacts(ids, "sync")
        sending: List[Tuple[Work, Dict[str, Any]]] = []
        for work in works:
            sync = synced[work.article_id][""]
            upload = uploads[work.article_id][""]
            source, chain = chosen.get(work.article_id, (None, []))
            if source != work.source:  # its chain moved since the plan; the next plan has it
                continue
            runs = [ctx.catalog.run(a.run_id) for a in chain]
            summary = {"backend": "ingester", "dry_run": self.dry_run,
                       "base_study_id": upload.summary.get("base_study_id"),
                       "parse_id": upload.summary.get("parse_id"),
                       "runs": [a.run_id for a in chain]}
            if any(run is None for run in runs):
                yield from self._record(work, Outcome.failure(
                    work.article_id, self.name, work.source,
                    "a run of its chain is not kept", fingerprint=work.fingerprint,
                    summary=summary))
                continue
            request = build_request(ctx.catalog.payload(chain[-1]), runs,
                                    summary["parse_id"], summary["base_study_id"])
            summary["run_id"] = request["run_id"]
            self._requests[(work.article_id, work.source)] = (
                recorded_path(ctx.catalog.root, sync.summary["path"]), request)
            sending.append((work, summary))
        try:
            yield from self._send_all(sending, [])
        finally:
            self._requests.clear()

    def _send_one(self, ingester, work: Work) -> Dict[str, Any]:
        study_dir, request = self._requests[(work.article_id, work.source)]
        return ingester.upload_record(Path(study_dir), work.source, request)

    def _outcome(self, work: Work, artifact: Dict[str, Any], summary: Dict[str, Any],
                 learned) -> Outcome:
        from ingestion_workflow.services.neurostore_ingest import Result

        result = Result(artifact["status"])
        kept = {k: v for k, v in artifact.items() if k not in ("history", "last_uploaded")}
        summary = {**summary, "ingester": kept}
        status = result.catalog_status
        if status is Status.OK:
            return Outcome(article_id=work.article_id, stage=self.name, source=work.source,
                           status=Status.OK, fingerprint=work.fingerprint, summary=summary)
        error = kept.get("error") or {}
        detail = error.get("detail") or error.get("title") or "no detail"
        if error.get("codes"):  # UNKNOWN_PARSE, RUN_CONFLICT, ...
            detail = f"{', '.join(error['codes'])}: {detail}"
        if status is None:  # a dry run
            return Outcome(article_id=work.article_id, stage=self.name, source=work.source,
                           status=Status.SKIPPED, fingerprint=work.fingerprint,
                           summary={**summary, "reason": result.value})
        return Outcome.failure(
            work.article_id, self.name, work.source,
            f"{result.value}: {short_reason(str(detail))}",
            permanent=status is Status.PERMANENT, fingerprint=work.fingerprint, summary=summary)


def _uploaded_parse(sync: Optional[Artifact], upload: Optional[Artifact]) -> Optional[str]:
    """The parse_id neurostore holds as the article's current parse: sync's, once uploaded."""
    if sync is None or not sync.ok or upload is None or upload.status is not Status.OK:
        return None
    parse_id = (sync.summary or {}).get("parse_id")
    if not parse_id or (upload.summary or {}).get("parse_id") != parse_id:
        return None
    return parse_id if (upload.summary or {}).get("base_study_id") else None


__all__ = ["CHAIN", "NO_CHAIN", "NO_PARSE_UPLOADED", "RecordUploadStage", "build_request",
           "run_id_of", "sent_run"]
