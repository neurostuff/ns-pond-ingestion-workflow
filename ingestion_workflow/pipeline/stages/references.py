"""The article's reference list and in-text citations, placed in its text.

The text is the article's: that of the extraction triage judges, which sync copies
and passages index. The artifact is keyed by that extraction's source, because a
citation's offsets belong to one text. No
network: the source's own markup is read (JATS `xref`, Elsevier `ce:cross-ref`,
publisher HTML links). Where `reflist` has the paper's Crossref list, its DOIs and
PMIDs fill the source's entries, and for a source that marks nothing (a PDF, a page
without links) it is the list the text's citations are matched to.
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

logger = logging.getLogger(__name__)

#: Bump when the readers change in a way the extraction does not describe.
#: 1 -> 2: the Crossref list from `reflist`.
REFERENCES_VERSION = 2

#: Longest a worker may spend on one article.
ARTICLE_SECONDS = 120


def read_article(source: str, download: dict, text_path: Optional[str],
                 listed: Optional[List[dict]] = None) -> Tuple[str, Optional[dict]]:
    """("ok", payload) or (why it failed, None). A module function so a process pool can run it.

    `listed` is the paper's Crossref list, when `reflist` has one.
    """
    from ingestion_workflow.services.citation_markers import find
    from ingestion_workflow.services.citations import JATS_SOURCES, READABLE_SOURCES, ReadResult, read
    from ingestion_workflow.services.reference_lists import fill_identifiers

    if not text_path or not Path(text_path).exists():
        return "extraction kept no text", None
    text = Path(text_path).read_bytes().decode("utf-8")  # as stored: citation offsets index it
    result: Optional[ReadResult] = None
    if source in READABLE_SOURCES:
        try:
            result = read(source, DownloadResult.from_dict(download), text)
        except Exception as exc:  # noqa: BLE001 - one unreadable download is that article's problem
            return f"unreadable: {type(exc).__name__}: {exc}", None
    from ingestion_workflow.extractors.pubget_extractor import KEEPS_SUPERSCRIPTS

    provider = "source"
    # JATS text holds a loose superscript number only when pubget kept superscripts
    loose = source not in JATS_SOURCES or KEEPS_SUPERSCRIPTS
    if result is not None and result.references:
        if not result.citations:
            # a list but no link that landed in the text: match the text to the source's own list
            result.citations, style = find(text, result.references, loose_numbers=loose)
            result.notes["style_" + style] = 1
        if listed:
            result.notes["ids_from_crossref"] = fill_identifiers(result.references, listed)
    elif listed:
        # the download marks nothing it can be read by: match the text to Crossref's list
        citations, style = find(text, listed, loose_numbers=loose)
        result = ReadResult([dict(r) for r in listed], citations, {"style_" + style: 1})
        provider = "crossref"
    if result is None:
        return "download has no file to read", None
    return "ok", {
        "source": source,
        "list_provider": provider,
        "text_sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(),
        "references": result.references,
        "citations": result.citations,
        "notes": result.notes,
    }


class _TooSlow(BaseException):
    """Not an Exception, so read_article does not take it for an unreadable file."""


def _alarm(*_):
    raise _TooSlow


def _read_in_worker(job: Tuple[str, dict, Optional[str], Optional[List[dict]]]) -> Tuple[str, Optional[dict]]:
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

    def fingerprint_for(self, extraction: Artifact, reflist: Optional[Artifact] = None) -> str:
        listed = reflist.fingerprint if reflist is not None and reflist.status is Status.OK else ""
        return fingerprint("references", REFERENCES_VERSION, extraction.source, listed,
                           upstream=extraction.fingerprint)

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
        reflists = ctx.catalog.artifacts(ids, "reflist")
        attempts: Dict[str, Dict[str, tuple]] = {}
        from .triage import judged_extraction

        for ref in refs:
            judged = judged_extraction(ctx, upstream.get(ref.id, {}), downloads.get(ref.id, {}))
            reflist = reflists.get(ref.id, {}).get("")
            has_list = bool(reflist and reflist.status is Status.OK and reflist.summary.get("references"))
            readable = ({judged.source: judged}
                        if judged is not None and (judged.source in READABLE_SOURCES or has_list) else {})
            if not readable:
                plan.blocked += 1
                continue
            for source, extraction in readable.items():
                fp = self.fingerprint_for(extraction, reflist)
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
        reflists = ctx.catalog.artifacts([w.article_id for w in works], "reflist")
        for work in works:
            download = ctx.catalog.artifact(work.article_id, "download", work.source)
            download_payload = ctx.payload(download) if download else None
            extraction = ctx.payload(work.upstream)
            if not download_payload or not extraction:
                yield Outcome.failure(work.article_id, self.name, work.source,
                                      "download or extraction payload missing from blob store",
                                      fingerprint=work.fingerprint)
                continue
            reflist = reflists.get(work.article_id, {}).get("")
            listed = (ctx.payload(reflist) or {}).get("references") if reflist and reflist.status is Status.OK else None
            jobs.append((work.source, download_payload, extraction.get("full_text_path"), listed or None))
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
                summary={"list": payload["list_provider"], "references": len(payload["references"]),
                         "citations": len(citations),
                         "with_sentence": sum(c["sentence"] is not None for c in citations),
                         "markers_not_in_text": sum(not c["marker_in_text"] for c in citations),
                         **{k: v for k, v in payload["notes"].items() if k in ("links_not_in_text", "ids_from_crossref")}},
            )
