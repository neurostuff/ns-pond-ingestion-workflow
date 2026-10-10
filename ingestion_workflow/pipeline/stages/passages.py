"""Find the passages of an article's prose that report coordinates: prose's extraction stage.

What `extract` is to the tables this is to the text: no model, only the
detector run, so `metadata` can follow it and `prose` can read each passage
with the article's title and abstract.

The text read is the article's text: the text of the extraction triage judges,
which sync copies as `text.txt`. It is the only text read. An article with no
extraction text has no passages until its extraction is fixed. A passage is
`(start, end)` spans into the text, whose sha256 the payload records and the
fingerprint chains on.
"""

from __future__ import annotations

import logging
import multiprocessing
import signal
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Sequence, Tuple

from ingestion_workflow.catalog import ArticleRef, Artifact, Outcome, Status, fingerprint

from ..plan import StagePlan, Work
from ..stage import Context

logger = logging.getLogger(__name__)

#: Bump when the passages change for a reason the text does not describe:
#: the filter, the detector.
PASSAGES_VERSION = "2026-10-10.extraction-text-only"

#: Most passages kept from one article. Set above what any real paper needed.
MAX_PASSAGES = 40

#: Longest a worker may spend on one file. Overlapping regex quantifiers once
#: held a single file for minutes, and the batch with it.
FILE_SECONDS = 60


def find_passages(job: dict) -> Tuple[str, list, Optional[str], str]:
    """How the article's prose was read, its passages, the space it states, and its text.

    `job` names the extraction text (`text_path`) and its `figure_captions`. The
    space is read from the same Methods and Results by `space`'s rules.

    Figure legends are read: about half the coordinates in them are the study's
    results (2026-10-10 sample). They are where the extractor wrote them, the
    `figure_captions` spans (`extractors.figure_captions`), never searched for.
    A passage whose hits all lie in a legend is marked `from_legend`.

    A module function so a process pool can run it: the detector is
    processor-bound.
    """
    from ingestion_workflow.services.coordinate_space import read_space
    from ingestion_workflow.services.prose_text import kept_spans, may_hold_coordinates

    try:
        text = Path(job["text_path"]).read_bytes().decode("utf-8")
        if not may_hold_coordinates(text):
            return "filtered", [], None, text
        legends = _caption_spans(text, job.get("figure_captions") or [])
        # the legends are the text's last section: the body ends where they begin
        body = text[: _legends_start(text, legends)]
        spans, how = kept_spans(body)
        spans += legends
        found = [p for a, b in spans for p in _shifted(text, a, b)][:MAX_PASSAGES]
        for p in found:
            p.from_legend = bool(p.hits) and all(
                any(a <= h.span[0] and h.span[1] <= b for a, b in legends) for h in p.hits)
        reading = read_space(body) if found else None
        return how, found, reading.space.value if reading else None, text
    except Exception as exc:  # noqa: BLE001 - an unreadable file is that article's problem only
        return f"unreadable: {type(exc).__name__}", [], None, ""


def _caption_spans(text: str, captions: list) -> list:
    """The extraction payload's caption spans, in order, those that fit `text`."""
    out = []
    for c in captions:
        a, b = c.get("span") or (0, 0)
        if 0 <= a < b <= len(text):
            out.append((a, b))
    return sorted(set(out))


def _legends_start(text: str, legends: list) -> int:
    """Where the "Figure legends" section begins: its heading's line, before the first caption."""
    if not legends:
        return len(text)
    from ingestion_workflow.extractors.figure_captions import HEADING

    at = text.rfind(HEADING, 0, legends[0][0])
    if at < 0:
        return legends[0][0]
    line = text.rfind("\n", 0, at)
    return line + 1 if line >= 0 else 0


def _shifted(text: str, a: int, b: int) -> list:
    """The passages of text[a:b], their spans moved into `text`."""
    from ingestion_workflow.services.prose_passages import passages

    def move(span):
        return (span[0] + a, span[1] + a) if span else None

    out = []
    for p in passages(text[a:b]):
        p.span, p.before_span, p.after_span, p.heading_span = (
            move(p.span), move(p.before_span), move(p.after_span), move(p.heading_span))
        for h in p.hits:
            h.span = move(h.span)
        out.append(p)
    return out


class _TooSlow(BaseException):
    """Not an Exception, so find_passages does not take it for an unreadable file."""


def _alarm(*_):
    raise _TooSlow


def _find_in_worker(job: dict) -> Tuple[str, list, Optional[str], str]:
    signal.signal(signal.SIGALRM, _alarm)
    signal.alarm(FILE_SECONDS)
    try:
        return find_passages(job)
    except _TooSlow:
        return "timeout", [], None, ""
    finally:
        signal.alarm(0)


def _span(span) -> Optional[list]:
    return list(span) if span else None


def passage_dict(passage) -> dict:
    """A passage as stored: spans into the text, none of its characters."""
    return {"span": list(passage.span), "before": _span(passage.before_span), "after": _span(passage.after_span),
            "heading": _span(passage.heading_span), "space": passage.space,
            "from_legend": passage.from_legend,
            "hits": [{"pattern": h.pattern, "x": h.x, "y": h.y, "z": h.z, "span": list(h.span)}
                     for h in passage.hits]}


def passage_from(payload: dict, text: str):
    """A stored passage read back from the text its spans index."""
    from ingestion_workflow.services.prose_passages import Hit, Passage, heading_text, view

    def span(key):
        return tuple(payload[key]) if payload.get(key) else None

    return Passage(text=view(text, span("span")), before=view(text, span("before")), after=view(text, span("after")),
                   heading=heading_text(text, span("heading")), space=payload.get("space"),
                   hits=[Hit(h["pattern"], h["x"], h["y"], h["z"], tuple(h["span"])) for h in payload.get("hits", [])],
                   span=span("span"), before_span=span("before"), after_span=span("after"),
                   heading_span=span("heading"), from_legend=payload.get("from_legend", False))


def read_text(payload: dict) -> str:
    """The text a passages payload indexes; raises when it is gone or has changed since."""
    from ingestion_workflow.services.offsets import sha256

    path = payload.get("full_text_path")
    text = Path(path).read_bytes().decode("utf-8") if path and Path(path).is_file() else None
    if text is None or sha256(text) != payload.get("text_sha256"):
        raise LookupError("the text the passages index is gone or has changed")
    return text


def text_of(ctx: Context, extraction: Optional[Artifact]) -> Tuple[Optional[str], Optional[str]]:
    """An extraction's text file and its sha256: from the summary, else the file."""
    if extraction is None or extraction.status is not Status.OK or not extraction.summary.get("has_text", True):
        return None, None
    from .extract import file_sha256

    path = (ctx.payload(extraction) or {}).get("full_text_path")
    if not path or not Path(path).is_file():
        return None, None
    return path, extraction.summary.get("text_sha256") or file_sha256(path)


class PassagesStage:
    name = "passages"
    #: An extraction with no table still has its text, so a paper that reports
    #: its peaks only in the text is read.
    requires = "extract"

    def __init__(self, settings) -> None:
        self.settings = settings

    @staticmethod
    def fingerprint_for(extraction: Artifact, text_sha256: str) -> str:
        return fingerprint("passages", PASSAGES_VERSION, "extract", extraction.source, text_sha256,
                           upstream=extraction.fingerprint)

    def plan(
        self,
        ctx: Context,
        refs: Sequence[ArticleRef],
        artifacts: Dict[str, Dict[str, Artifact]],
        upstream: Dict[str, Dict[str, Artifact]],
    ) -> StagePlan:
        plan = StagePlan(stage=self.name)
        from .triage import judged_extraction

        ids = [ref.id for ref in refs]
        attempts = ctx.catalog.attempt_counts(ids, self.name, "")
        downloads = ctx.catalog.artifacts(ids, "download")
        for ref in refs:
            extraction = judged_extraction(ctx, upstream.get(ref.id, {}), downloads.get(ref.id, {}))
            path, sha = text_of(ctx, extraction)
            if path is None:
                plan.blocked += 1
                continue
            fp = self.fingerprint_for(extraction, sha)
            existing = artifacts.get(ref.id, {}).get("")
            if ctx.is_fresh(existing, fp):
                plan.fresh += 1
                continue
            count, last = attempts.get(ref.id, (0, None))
            if not ctx.should_attempt(existing, count, last, self.name):
                plan.permanent += 1
                continue
            work = Work(ref=ref, source="", fingerprint=fp, upstream=extraction)
            plan.pending.append(work)
        return plan

    def execute(self, ctx: Context, works: List[Work]) -> Iterator[Outcome]:
        jobs = []
        for work in works:
            payload = ctx.payload(work.upstream) or {}
            jobs.append({"text_path": payload.get("full_text_path"),
                         "figure_captions": payload.get("figure_captions") or []})
        workers = max(1, getattr(self.settings, "max_workers", 1) or 1)
        if len(works) < 8 or workers == 1:
            found = [find_passages(job) for job in jobs]
        else:
            # fork: nothing here touches CUDA, and spawn would re-import the
            # package in every worker. The pool lives for one batch.
            with ProcessPoolExecutor(max_workers=workers, mp_context=multiprocessing.get_context("fork")) as pool:
                found = list(pool.map(_find_in_worker, jobs, chunksize=8))

        from ingestion_workflow.services.offsets import sha256

        for work, job, (how, ps, article_space, text) in zip(works, jobs, found):
            if how in ("timeout",) or how.startswith("unreadable"):
                yield Outcome.failure(work.article_id, self.name, "", how, fingerprint=work.fingerprint)
                continue
            yield Outcome(
                article_id=work.article_id, stage=self.name, source="", status=Status.OK,
                fingerprint=work.fingerprint,
                payload={"source": work.upstream.source, "read": how, "space": article_space,
                         "full_text_path": job["text_path"] if ps else None,
                         "text_sha256": sha256(text) if ps else None,
                         "passages": [passage_dict(p) for p in ps]},
                summary={"source": work.upstream.source, "read": how, "passages": len(ps),
                         "hits": sum(len(p.hits) for p in ps)},
            )
