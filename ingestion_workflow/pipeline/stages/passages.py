"""Find the passages of an article's prose that report coordinates: prose's extraction stage.

What `extract` is to the tables this is to the text: no model, only the
download read and the detector run, so `metadata` can follow it and `prose`
can read each passage with the article's title and abstract.
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

#: Bump when the passages change for a reason the download does not
#: describe: the reader, the raw filter, the detector.
PASSAGES_VERSION = "2026-10-07.downloads+lists"

#: Which download is read, best first: XML marks its sections and its tables,
#: publisher HTML less reliably, a PDF's text layer not at all. pmc and
#: europepmc are the same JATS XML pubget downloads; pubget leads so an
#: article already read from it keeps its passages.
SOURCE_PREFERENCE = ("pubget", "pmc", "europepmc", "elsevier", "ace", "pdf")

#: Most passages kept from one article. Set above what any real paper needed.
MAX_PASSAGES = 40

#: Longest a worker may spend on one file. Overlapping regex quantifiers once
#: held a single file for minutes, and the batch with it.
FILE_SECONDS = 60


def _choose(downloads: Dict[str, Artifact]) -> Optional[Artifact]:
    ok = {source: a for source, a in downloads.items() if a.status is Status.OK}
    ranked = sorted(ok, key=lambda s: SOURCE_PREFERENCE.index(s) if s in SOURCE_PREFERENCE else len(SOURCE_PREFERENCE))
    return ok[ranked[0]] if ranked else None


def find_passages(files: List[dict]) -> Tuple[str, list, Optional[str], str]:
    """How the article's prose was read, its passages, the space it states, and its text.

    The space is read from the same Methods and Results by `space`'s rules,
    for an article whose extraction -- which `space` reads -- never succeeded.
    The text (all of it, legends after) is returned only with a passage: it
    is kept for sync and `space` when the article has no extraction.

    A module function so a process pool can run it: reading a download and
    the detector are processor-bound.
    """
    from ingestion_workflow.services.coordinate_space import read_space
    from ingestion_workflow.services.prose_passages import passages
    from ingestion_workflow.services.prose_text import (
        main_file,
        may_hold_coordinates,
        methods_and_results,
        read_download,
    )

    f = main_file(files)
    if f is None:
        return "no file", [], None, ""
    path = Path(f["file_path"])
    try:
        if f["file_type"] != "pdf" and not may_hold_coordinates(path.read_text(errors="ignore")):
            return "filtered", [], None, ""
        text, legends = read_download(path, f["file_type"])
        prose, how = methods_and_results(text, legends)
        found = passages(prose)[:MAX_PASSAGES]
        reading = read_space(text) if found else None
        kept = (text + ("\n\n" + legends if legends else "")) if found else ""
        return how, found, reading.space.value if reading else None, kept
    except Exception as exc:  # noqa: BLE001 - an unreadable file is that article's problem only
        return f"unreadable: {type(exc).__name__}", [], None, ""


class _TooSlow(BaseException):
    """Not an Exception, so find_passages does not take it for an unreadable file."""


def _alarm(*_):
    raise _TooSlow


def _find_in_worker(files: List[dict]) -> Tuple[str, list, Optional[str]]:
    signal.signal(signal.SIGALRM, _alarm)
    signal.alarm(FILE_SECONDS)
    try:
        return find_passages(files)
    except _TooSlow:
        return "timeout", [], None, ""
    finally:
        signal.alarm(0)


def text_path(settings, article_id: str) -> Path:
    """Where the text an article's passages came from is kept: a .txt with Markdown
    headings, as extract keeps its own, which sync writes as `text.txt`."""
    return Path(settings.data_root) / "passages" / f"{article_id}.txt"


def passage_dict(passage) -> dict:
    return {"text": passage.text, "before": passage.before, "after": passage.after,
            "heading": passage.heading, "space": passage.space,
            "hits": [{"pattern": h.pattern, "x": h.x, "y": h.y, "z": h.z, "span": list(h.span)}
                     for h in passage.hits]}


def passage_from(payload: dict):
    from ingestion_workflow.services.prose_passages import Hit, Passage

    return Passage(text=payload["text"], before=payload.get("before", ""), after=payload.get("after", ""),
                   heading=payload.get("heading"), space=payload.get("space"),
                   hits=[Hit(h["pattern"], h["x"], h["y"], h["z"], tuple(h["span"])) for h in payload.get("hits", [])])


class PassagesStage:
    name = "passages"
    #: download, not extract: a paper reporting its peaks only in the text
    #: often has no table that parsed, or none at all.
    requires = "download"

    def __init__(self, settings) -> None:
        self.settings = settings

    def fingerprint_for(self, download: Artifact) -> str:
        return fingerprint("passages", PASSAGES_VERSION, download.source, upstream=download.fingerprint)

    def plan(
        self,
        ctx: Context,
        refs: Sequence[ArticleRef],
        artifacts: Dict[str, Dict[str, Artifact]],
        upstream: Dict[str, Dict[str, Artifact]],
    ) -> StagePlan:
        plan = StagePlan(stage=self.name)
        attempts = ctx.catalog.attempt_counts([ref.id for ref in refs], self.name, "")
        for ref in refs:
            download = _choose(upstream.get(ref.id, {}))
            if download is None:
                plan.blocked += 1
                continue
            fp = self.fingerprint_for(download)
            existing = artifacts.get(ref.id, {}).get("")
            if ctx.is_fresh(existing, fp):
                plan.fresh += 1
                continue
            count, last = attempts.get(ref.id, (0, None))
            if not ctx.should_attempt(existing, count, last, self.name):
                plan.permanent += 1
                continue
            plan.pending.append(Work(ref=ref, source="", fingerprint=fp, upstream=download))
        return plan

    def execute(self, ctx: Context, works: List[Work]) -> Iterator[Outcome]:
        files = [(ctx.payload(work.upstream) or {}).get("files", []) for work in works]
        workers = max(1, getattr(self.settings, "max_workers", 1) or 1)
        if len(works) < 8 or workers == 1:
            found = [find_passages(f) for f in files]
        else:
            # fork: nothing here touches CUDA, and spawn would re-import the
            # package in every worker. The pool lives for one batch.
            with ProcessPoolExecutor(max_workers=workers, mp_context=multiprocessing.get_context("fork")) as pool:
                found = list(pool.map(_find_in_worker, files, chunksize=8))

        for work, (how, ps, article_space, text) in zip(works, found):
            if how in ("timeout",) or how.startswith("unreadable"):
                yield Outcome.failure(work.article_id, self.name, "", how, fingerprint=work.fingerprint)
                continue
            # The text is kept for an article with a passage: sync writes it,
            # and `space` reads it, when extract could not read the article.
            kept = None
            if text:
                kept = text_path(self.settings, work.article_id)
                kept.parent.mkdir(parents=True, exist_ok=True)
                kept.write_text(text, encoding="utf-8")
            yield Outcome(
                article_id=work.article_id, stage=self.name, source="", status=Status.OK,
                fingerprint=work.fingerprint,
                payload={"source": work.upstream.source, "read": how, "space": article_space,
                         "full_text_path": str(kept) if kept else None,
                         "passages": [passage_dict(p) for p in ps]},
                summary={"source": work.upstream.source, "read": how, "passages": len(ps),
                         "hits": sum(len(p.hits) for p in ps)},
            )
