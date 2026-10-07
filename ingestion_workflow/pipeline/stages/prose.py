"""Read the coordinates an article reports in its prose rather than its tables."""

from __future__ import annotations

import logging
import multiprocessing
import threading
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Sequence, Tuple

from ingestion_workflow.catalog import ArticleRef, Artifact, Outcome, Status, fingerprint
from ingestion_workflow.prompts.prose_coordinates import PROSE_PROMPT_VERSION

from ..plan import StagePlan, Work
from ..stage import Context

logger = logging.getLogger(__name__)

#: Bump when the passages sent to the model change for a reason the prompt
#: version and the model do not describe: the reader, the filter, the detector.
PROSE_VERSION = "2026-10-07.downloads+lists+in-order"

#: Which download is read, best first: XML marks its sections and its tables,
#: publisher HTML less reliably, a PDF's text layer not at all.
SOURCE_PREFERENCE = ("pubget", "elsevier", "ace", "pdf")

#: Most passages read in one article. Set above what any real paper needed;
#: past it the passages are counted and skipped.
MAX_PASSAGES = 40


def _choose(downloads: Dict[str, Artifact]) -> Optional[Artifact]:
    ok = {source: a for source, a in downloads.items() if a.status is Status.OK}
    ranked = sorted(ok, key=lambda s: SOURCE_PREFERENCE.index(s) if s in SOURCE_PREFERENCE else len(SOURCE_PREFERENCE))
    return ok[ranked[0]] if ranked else None


def find_passages(files: List[dict]) -> Tuple[str, list, Optional[str]]:
    """How the article's prose was read, its passages, and the space it states.

    The space is read from the same Methods and Results by `space`'s rules,
    for an article whose extraction -- which `space` reads -- never succeeded.

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
        return "no file", [], None
    path = Path(f["file_path"])
    try:
        if f["file_type"] != "pdf" and not may_hold_coordinates(path.read_text(errors="ignore")):
            return "filtered", [], None
        text, legends = read_download(path, f["file_type"])
        prose, how = methods_and_results(text, legends)
        found = passages(prose)[:MAX_PASSAGES]
        reading = read_space(text) if found else None
        return how, found, reading.space.value if reading else None
    except Exception as exc:  # noqa: BLE001 - an unreadable file is that article's problem only
        return f"unreadable: {type(exc).__name__}", [], None


class ProseStage:
    name = "prose"
    #: download, not extract or triage: a paper reporting its peaks only in
    #: the text often has no table that parsed, or none at all.
    requires = "download"

    def __init__(self, settings) -> None:
        self.settings = settings
        self._client = None
        self._lock = threading.Lock()

    def client(self):
        if self._client is None:
            with self._lock:
                if self._client is None:
                    from ingestion_workflow.clients.prose_coordinates import ProseCoordinateClient

                    self._client = ProseCoordinateClient(self.settings)
        return self._client

    def fingerprint_for(self, download: Artifact) -> str:
        return fingerprint("prose", PROSE_VERSION, PROSE_PROMPT_VERSION, self.settings.prose_model,
                           download.source, upstream=download.fingerprint)

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
        files = []
        for work in works:
            files.append((ctx.payload(work.upstream) or {}).get("files", []))
        workers = max(1, getattr(self.settings, "max_workers", 1) or 1)
        if len(works) < 8 or workers == 1:
            found = [find_passages(f) for f in files]
        else:
            # fork: nothing here touches CUDA, and spawn would re-import the
            # package in every worker. Before the model's threads exist.
            with ProcessPoolExecutor(max_workers=workers, mp_context=multiprocessing.get_context("fork")) as pool:
                found = list(pool.map(find_passages, files, chunksize=8))

        metadata = ctx.catalog.artifacts([w.article_id for w in works], "metadata")
        reading = []
        for work, (_, ps, _) in zip(works, found):
            meta = ctx.payload(metadata.get(work.article_id, {}).get("")) or {}
            reading += [(p, meta.get("title") or "", meta.get("abstract") or "") for p in ps]
        answers = {}
        if reading:
            with ThreadPoolExecutor(max_workers=max(1, self.settings.n_llm_workers)) as pool:
                futures = {id(p): pool.submit(self._read, p, title, abstract) for p, title, abstract in reading}
                answers = {key: f.result() for key, f in futures.items()}

        for work, (how, ps, article_space) in zip(works, found):
            out, errors, coords, results = [], 0, 0, 0
            for p in ps:
                answer, error = answers[id(p)]
                errors += error is not None
                points = [q for a in answer.get("analyses", []) for q in a["points"]]
                coords += len(points)
                results += sum(1 for q in points if q["role"] == "result")
                out.append({"text": p.text, "heading": p.heading, "space": answer.get("space") or p.space,
                            "analyses": answer.get("analyses", []), "error": error})
            if errors:
                # A partly read article would be cached as complete; retry it whole.
                yield Outcome.failure(work.article_id, self.name, "",
                                      f"{errors} of {len(ps)} passages failed", fingerprint=work.fingerprint)
                continue
            yield Outcome(
                article_id=work.article_id, stage=self.name, source="", status=Status.OK,
                fingerprint=work.fingerprint,
                payload={"source": work.upstream.source, "read": how, "space": article_space,
                         "passages": out},
                summary={"source": work.upstream.source, "read": how, "passages": len(ps),
                         "coordinates": coords, "results": results},
            )

    def _read(self, passage, title, abstract):
        try:
            return self.client().extract(passage, title=title, abstract=abstract), None
        except Exception as exc:  # noqa: BLE001 - one failed call fails its article, not the batch
            logger.warning("prose passage failed: %s", exc)
            return {}, f"{type(exc).__name__}: {exc}"
