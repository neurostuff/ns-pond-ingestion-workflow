"""The data sources each synced paper's text names, from the dictionary `ingest data-sources
pull` keeps in the catalog (`data_sources.matching`).

Only papers sync wrote and did not retract are read: those are the ones upload sends. The
text is the extraction triage judges, which sync writes, and the artifact is keyed by its
source, because a hit's offsets belong to one text. No network. `ingest data-sources export`
writes the matches as the TSV neurostore's `manage data-sources link-matches` reads.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Sequence, Tuple

from ingestion_workflow.catalog import ArticleRef, Artifact, Outcome, Status, fingerprint

from ..plan import StagePlan, Work
from ..stage import Context

#: Bump when what is read, or how a match is found, changes.
DATASOURCES_VERSION = 1

#: Why every article waits: the catalog holds no dictionary yet.
NO_DICTIONARY = "no data sources pulled: run `ingest data-sources pull`"
NOT_SYNCED = "not synced"
NO_EXTRACTION = "no extraction triage judges"

#: The dictionary `read_article` searches, compiled once per digest; forked workers inherit it.
_compiled: Dict[str, object] = {}


def _dictionary(digest: str, aliases):
    from ingestion_workflow.data_sources.matching import Dictionary

    if digest not in _compiled:
        _compiled.clear()
        _compiled[digest] = Dictionary(aliases)
    return _compiled[digest]


def read_article(digest: str, text_path: Optional[str]) -> Tuple[str, Optional[dict]]:
    """("ok", payload) or (why it failed, None)."""
    if not text_path or not Path(text_path).exists():
        return "extraction kept no text", None
    text = Path(text_path).read_bytes().decode("utf-8")  # as stored: offsets index it
    found = _compiled[digest].find(text)
    return "ok", {"text_sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(), **found}


def synced(artifact: Optional[Artifact]) -> bool:
    """Whether sync wrote the paper, so upload sends it."""
    return (artifact is not None and artifact.status is Status.OK
            and not (artifact.summary or {}).get("retracted"))


class DataSourcesStage:
    name = "datasources"
    requires = "sync"

    def __init__(self, settings) -> None:
        self.settings = settings

    def fingerprint_for(self, digest: str, extraction: Artifact) -> str:
        return fingerprint("datasources", DATASOURCES_VERSION, digest, extraction.source,
                           upstream=extraction.fingerprint)

    def plan(
        self,
        ctx: Context,
        refs: Sequence[ArticleRef],
        artifacts: Dict[str, Dict[str, Artifact]],
        upstream: Dict[str, Dict[str, Artifact]],
    ) -> StagePlan:
        from .triage import judged_extraction

        plan = StagePlan(stage=self.name)
        digest = ctx.catalog.data_sources_digest()
        ids = [ref.id for ref in refs]
        extracted = ctx.catalog.artifacts(ids, "extract")
        downloads = ctx.catalog.artifacts(ids, "download")
        attempts: Dict[str, Dict[str, tuple]] = {}
        for ref in refs:
            if not synced(upstream.get(ref.id, {}).get("")):
                plan.block(NOT_SYNCED)
                continue
            if digest is None:
                plan.wait(NO_DICTIONARY, ref.id)
                continue
            judged = judged_extraction(ctx, extracted.get(ref.id, {}), downloads.get(ref.id, {}))
            if judged is None:
                plan.block(NO_EXTRACTION)
                continue
            source = judged.source
            fp = self.fingerprint_for(digest, judged)
            existing = artifacts.get(ref.id, {}).get(source)
            if ctx.is_fresh(existing, fp):
                plan.fresh += 1
                continue
            if source not in attempts:
                attempts[source] = ctx.catalog.attempt_counts(ids, self.name, source)
            count, last = attempts[source].get(ref.id, (0, None))
            if not ctx.should_attempt(existing, count, last, self.name, source):
                plan.permanent += 1
                continue
            plan.pending.append(Work(ref=ref, source=source, fingerprint=fp, upstream=judged))
        return plan

    def execute(self, ctx: Context, works: List[Work]) -> Iterator[Outcome]:
        from ingestion_workflow.data_sources.merge import digest as digest_of

        from .references import read_articles

        aliases = ctx.catalog.data_source_aliases()
        digest = digest_of(aliases)
        _dictionary(digest, aliases)
        jobs, ready = [], []
        for work in works:
            if work.fingerprint != self.fingerprint_for(digest, work.upstream):
                yield Outcome.failure(work.article_id, self.name, work.source,
                                      "the dictionary changed after planning",
                                      fingerprint=work.fingerprint)
                continue
            extraction = ctx.payload(work.upstream)
            if not extraction:
                yield Outcome.failure(work.article_id, self.name, work.source,
                                      "extraction payload missing from blob store",
                                      fingerprint=work.fingerprint)
                continue
            text_path = ctx.recorded_path(extraction.get("full_text_path"))
            jobs.append((digest, str(text_path) if text_path else None))
            ready.append(work)
        results = read_articles(read_article, jobs, getattr(self.settings, "max_workers", 1) or 1)
        for work, (how, payload) in zip(ready, results):
            if payload is None:
                yield Outcome.failure(work.article_id, self.name, work.source, how,
                                      fingerprint=work.fingerprint)
                continue
            yield Outcome(
                article_id=work.article_id, stage=self.name, source=work.source,
                status=Status.OK, fingerprint=work.fingerprint, payload=payload,
                summary={"sectioned": payload["sectioned"],
                         "matches": {m["data_source_id"]: m["status"]
                                     for m in payload["matches"]}},
            )
