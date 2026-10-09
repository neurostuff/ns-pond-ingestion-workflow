"""Each extraction's reference list and in-text citations, placed in its text.

One artifact per extracted source, because a citation's offsets belong to one text. No
network: the source's own markup is read (JATS `xref`, Elsevier `ce:cross-ref`,
publisher HTML links). A PDF marks nothing, so it is not read here.
"""

from __future__ import annotations

import hashlib
import logging
import multiprocessing
import signal
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Sequence, Tuple

from ingestion_workflow.catalog import ArticleRef, Artifact, Outcome, Status, fingerprint
from ingestion_workflow.models import DownloadResult

from ..plan import StagePlan, Work
from ..stage import Context
from .extract import current_extractions

logger = logging.getLogger(__name__)

#: Bump when the readers change in a way the extraction does not describe.
REFERENCES_VERSION = 1

#: Longest a worker may spend on one article.
ARTICLE_SECONDS = 120


def read_article(source: str, download: dict, text_path: Optional[str]) -> Tuple[str, Optional[dict]]:
    """("ok", payload) or (why it failed, None). A module function so a process pool can run it."""
    from ingestion_workflow.services.citations import read

    if not text_path or not Path(text_path).exists():
        return "extraction kept no text", None
    text = Path(text_path).read_text(encoding="utf-8")
    try:
        result = read(source, DownloadResult.from_dict(download), text)
    except Exception as exc:  # noqa: BLE001 - one unreadable download is that article's problem
        return f"unreadable: {type(exc).__name__}: {exc}", None
    if result is None:
        return "download has no file to read", None
    return "ok", {
        "source": source,
        "text_sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
        "references": result.references,
        "citations": result.citations,
        "notes": result.notes,
    }


class _TooSlow(BaseException):
    """Not an Exception, so read_article does not take it for an unreadable file."""


def _alarm(*_):
    raise _TooSlow


def _read_in_worker(job: Tuple[str, dict, Optional[str]]) -> Tuple[str, Optional[dict]]:
    signal.signal(signal.SIGALRM, _alarm)
    signal.alarm(ARTICLE_SECONDS)
    try:
        return read_article(*job)
    except _TooSlow:
        return "timeout", None
    finally:
        signal.alarm(0)


class ReferencesStage:
    name = "references"
    requires = "extract"

    def __init__(self, settings) -> None:
        self.settings = settings

    def fingerprint_for(self, extraction: Artifact) -> str:
        return fingerprint("references", REFERENCES_VERSION, extraction.source, upstream=extraction.fingerprint)

    def plan(
        self,
        ctx: Context,
        refs: Sequence[ArticleRef],
        artifacts: Dict[str, Dict[str, Artifact]],
        upstream: Dict[str, Dict[str, Artifact]],
    ) -> StagePlan:
        from ingestion_workflow.services.citations import READABLE_SOURCES

        plan = StagePlan(stage=self.name)
        ids = [ref.id for ref in refs]
        downloads = ctx.catalog.artifacts(ids, "download")
        attempts: Dict[str, Dict[str, tuple]] = {}
        for ref in refs:
            current = current_extractions(ctx, upstream.get(ref.id, {}), downloads.get(ref.id, {}))
            readable = {s: a for s, a in current.items() if s in READABLE_SOURCES}
            if not readable:
                plan.blocked += 1
                continue
            for source, extraction in readable.items():
                fp = self.fingerprint_for(extraction)
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
                plan.pending.append(Work(ref=ref, source=source, fingerprint=fp, upstream=extraction))
        return plan

    def execute(self, ctx: Context, works: List[Work]) -> Iterator[Outcome]:
        jobs, ready = [], []
        for work in works:
            download = ctx.catalog.artifact(work.article_id, "download", work.source)
            download_payload = ctx.payload(download) if download else None
            extraction = ctx.payload(work.upstream)
            if not download_payload or not extraction:
                yield Outcome.failure(work.article_id, self.name, work.source,
                                      "download or extraction payload missing from blob store",
                                      fingerprint=work.fingerprint)
                continue
            jobs.append((work.source, download_payload, extraction.get("full_text_path")))
            ready.append(work)
        workers = max(1, getattr(self.settings, "max_workers", 1) or 1)
        if len(jobs) < 8 or workers == 1:
            results = [read_article(*job) for job in jobs]
        else:
            # fork: nothing here touches CUDA, and spawn would re-import the package per worker
            with ProcessPoolExecutor(max_workers=workers, mp_context=multiprocessing.get_context("fork")) as pool:
                results = list(pool.map(_read_in_worker, jobs, chunksize=4))
        for work, (how, payload) in zip(ready, results):
            if payload is None:
                yield Outcome.failure(work.article_id, self.name, work.source, how, fingerprint=work.fingerprint)
                continue
            citations = payload["citations"]
            yield Outcome(
                article_id=work.article_id, stage=self.name, source=work.source, status=Status.OK,
                fingerprint=work.fingerprint, payload=payload,
                summary={"references": len(payload["references"]), "citations": len(citations),
                         "with_sentence": sum(c["sentence"] is not None for c in citations),
                         "markers_not_in_text": sum(not c["marker_in_text"] for c in citations),
                         **{k: v for k, v in payload["notes"].items() if k == "links_not_in_text"}},
            )
