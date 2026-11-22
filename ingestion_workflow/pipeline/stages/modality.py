"""Each paper's acquisition modalities, for its Neurostore base study: what its normalized
records state, and where they state none, what its methods text names.

Read for every article Neurostore knows: one sync wrote, or one held under a `neurostore`
id. That includes papers with no coordinate tables, which never reach sync and have no
record. The record's values are `normalization.modality`'s readings of its acquisitions;
the text's are `services.methods_modality`'s, read from the extraction triage judges. A
paper PubMed types as a review, and triage not as a meta-analysis, has no text reading: it
reports no acquisition of its own. No stage reads the artifact: `ingest modality export`
writes it for Neurostore.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterator, List, Optional, Sequence, Tuple

from ingestion_workflow.catalog import ArticleRef, Artifact, Outcome, Status, fingerprint

from ..plan import StagePlan, Work
from ..stage import Context

#: Bump when what is read, or how, changes.
MODALITY_VERSION = 1

NOT_IN_NEUROSTORE = "no neurostore base study"
NOTHING_TO_READ = "no normalized record and no extraction text"
REVIEW_TYPES = {"review"}


def base_study_of(sync: Optional[Artifact], neurostore_id: Optional[str]) -> Optional[str]:
    """The base study sync wrote the paper to, else the one the article is held under."""
    if sync is not None and sync.status is Status.OK:
        written = (sync.summary or {}).get("base_study_id")
        if written:
            return written
    return neurostore_id


def read_text(text_path: Optional[str]) -> Tuple[str, Optional[dict]]:
    """("ok", the text's reading) or (why it failed, None)."""
    from ingestion_workflow.services import methods_modality

    if not text_path or not Path(text_path).exists():
        return "extraction kept no text", None
    reading = methods_modality.read(Path(text_path).read_bytes().decode("utf-8", "replace"))
    return "ok", {"modalities": reading.modalities, "source": reading.where,
                  "mentions": reading.mentions}


def is_review(triage: Optional[dict]) -> bool:
    types = {str(t).strip().lower() for t in (triage or {}).get("publication_types") or []}
    return bool(types & REVIEW_TYPES) and not (triage or {}).get("is_meta_analysis")


class Inputs:
    """What the stage reads of each article: its base study, normalized records, the
    extraction triage judges and the triage itself."""

    def __init__(self, ctx: Context, ids: Sequence[str],
                 triages: Optional[Dict[str, Dict[str, Artifact]]] = None) -> None:
        from .triage import judged_extraction

        identifiers = ctx.catalog.identifiers(ids)
        syncs = ctx.catalog.artifacts(ids, "sync")
        normalized = ctx.catalog.artifacts(ids, "normalize")
        extracted = ctx.catalog.artifacts(ids, "extract")
        downloads = ctx.catalog.artifacts(ids, "download")
        triages = triages if triages is not None else ctx.catalog.artifacts(ids, "triage")
        self.base = {i: base_study_of(syncs.get(i, {}).get(""), identifiers[i].neurostore)
                     for i in ids}
        self.records = {i: sorted((a for a in normalized.get(i, {}).values() if a.ok),
                                  key=lambda a: a.source) for i in ids}
        self.text = {i: judged_extraction(ctx, extracted.get(i, {}), downloads.get(i, {}))
                     for i in ids}
        self.triage = {i: triages.get(i, {}).get("") for i in ids}


class ModalityStage:
    name = "modality"
    requires = "triage"

    def __init__(self, settings) -> None:
        self.settings = settings

    def fingerprint_for(self, inputs: Inputs, article_id: str) -> str:
        text, triage = inputs.text[article_id], inputs.triage[article_id]
        return fingerprint("modality", MODALITY_VERSION, inputs.base[article_id],
                           [(r.source, r.fingerprint) for r in inputs.records[article_id]],
                           text.source if text is not None else None,
                           triage.fingerprint if triage is not None else None,
                           upstream=text.fingerprint if text is not None else None)

    def plan(
        self,
        ctx: Context,
        refs: Sequence[ArticleRef],
        artifacts: Dict[str, Dict[str, Artifact]],
        upstream: Dict[str, Dict[str, Artifact]],
    ) -> StagePlan:
        plan = StagePlan(stage=self.name)
        ids = [ref.id for ref in refs]
        inputs = Inputs(ctx, ids, upstream)
        attempts = ctx.catalog.attempt_counts(ids, self.name, "")
        for ref in refs:
            if not inputs.base[ref.id]:
                plan.block(NOT_IN_NEUROSTORE)
                continue
            if not inputs.records[ref.id] and inputs.text[ref.id] is None:
                plan.block(NOTHING_TO_READ)
                continue
            fp = self.fingerprint_for(inputs, ref.id)
            existing = artifacts.get(ref.id, {}).get("")
            if ctx.is_fresh(existing, fp):
                plan.fresh += 1
                continue
            count, last = attempts.get(ref.id, (0, None))
            if not ctx.should_attempt(existing, count, last, self.name, ""):
                plan.permanent += 1
                continue
            plan.pending.append(Work(ref=ref, source="", fingerprint=fp,
                                     upstream=inputs.text[ref.id]))
        return plan

    def execute(self, ctx: Context, works: List[Work]) -> Iterator[Outcome]:
        from ingestion_workflow.normalization.modality import paper_modalities

        from .references import read_articles

        inputs = Inputs(ctx, [work.article_id for work in works])
        done: Dict[int, dict] = {}
        jobs, waiting = [], []
        for i, work in enumerate(works):
            aid = work.article_id
            if work.fingerprint != self.fingerprint_for(inputs, aid):
                done[i] = {"error": "an input changed after planning"}
                continue
            records = inputs.records[aid]
            stated = sorted({m for record in records
                             for m in paper_modalities(ctx.payload(record) or {})})
            triage = inputs.triage[aid]
            if stated:
                done[i] = {"modalities": stated, "source": "record",
                           "records": [r.source for r in records]}
            elif work.upstream is None:
                done[i] = {"modalities": [], "source": "none", "reason": "no extraction text"}
            elif is_review(ctx.payload(triage) if triage is not None else None):
                done[i] = {"modalities": [], "source": "none", "reason": "review"}
            else:
                path = ctx.recorded_path((ctx.payload(work.upstream) or {}).get("full_text_path"))
                jobs.append((str(path) if path else None,))
                waiting.append(i)
        workers = getattr(self.settings, "max_workers", 1) or 1
        for i, (how, payload) in zip(waiting, read_articles(read_text, jobs, workers)):
            done[i] = payload if payload is not None else {"error": how}
        for i, work in enumerate(works):
            found = done[i]
            if "error" in found:
                yield Outcome.failure(work.article_id, self.name, "", found["error"],
                                      fingerprint=work.fingerprint)
                continue
            payload = {"base_study_id": inputs.base[work.article_id], **found}
            if work.upstream is not None:
                payload["text_source"] = work.upstream.source
            yield Outcome(
                article_id=work.article_id, stage=self.name, source="", status=Status.OK,
                fingerprint=work.fingerprint, payload=payload,
                summary={"base_study_id": payload["base_study_id"],
                         "modalities": payload["modalities"], "source": payload["source"]})
