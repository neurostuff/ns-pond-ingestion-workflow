"""Read the coordinates an article reports in its prose rather than its tables."""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, Iterator, List, Sequence

from ingestion_workflow.catalog import ArticleRef, Artifact, Outcome, Status, fingerprint
from ingestion_workflow.prompts.prose_coordinates import PROSE_PROMPT_VERSION

from ..plan import StagePlan, Work
from ..stage import Context

logger = logging.getLogger(__name__)

#: Bump when the passages sent to the model change for a reason the prompt
#: version and the model do not describe -- the detector, mostly.
PROSE_VERSION = "2026-10-06.loose-triplets"


class ProseStage:
    name = "prose"
    #: triage, without its flag. Triage names the extraction it judged, which
    #: is the text read here, but an article whose tables all failed -- or that
    #: has none -- is exactly the one whose coordinates are in its prose.
    requires = "triage"

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

    def fingerprint_for(self, triaged: Artifact) -> str:
        return fingerprint(
            "prose", PROSE_VERSION, PROSE_PROMPT_VERSION,
            self.settings.prose_model or self.settings.llm_model,
            self.settings.prose_context,
            upstream=triaged.fingerprint,
        )

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
            triaged = upstream.get(ref.id, {}).get("")
            if triaged is None or triaged.status is not Status.OK:
                plan.blocked += 1
                continue
            fp = self.fingerprint_for(triaged)
            existing = artifacts.get(ref.id, {}).get("")
            if ctx.is_fresh(existing, fp):
                plan.fresh += 1
                continue
            count, last = attempts.get(ref.id, (0, None))
            if not ctx.should_attempt(existing, count, last, self.name):
                plan.permanent += 1
                continue
            plan.pending.append(Work(ref=ref, source="", fingerprint=fp, upstream=triaged))
        return plan

    def execute(self, ctx: Context, works: List[Work]) -> Iterator[Outcome]:
        from ingestion_workflow.services.prose_passages import passages

        jobs = []
        metadata = ctx.catalog.artifacts([w.article_id for w in works], "metadata")
        for work in works:
            source = (ctx.payload(work.upstream) or {}).get("source", "")
            extraction = ctx.catalog.artifacts([work.article_id], "extract").get(work.article_id, {}).get(source)
            payload = ctx.payload(extraction) if extraction and extraction.ok else None
            path = (payload or {}).get("full_text_path")
            if not path or not Path(path).exists():
                yield Outcome(article_id=work.article_id, stage=self.name, source="",
                              status=Status.OK, fingerprint=work.fingerprint,
                              payload={"source": source, "passages": []},
                              summary={"passages": 0, "coordinates": 0, "reason": "no full text"})
                continue
            found = passages(Path(path).read_text(errors="ignore"))
            limit = self.settings.prose_max_passages
            meta = ctx.payload(metadata.get(work.article_id, {}).get("")) or {}
            jobs.append((work, source, found[:limit], len(found) - min(len(found), limit),
                         meta.get("title") or "", meta.get("abstract") or ""))

        reading = [(job, p) for job in jobs for p in job[2]]
        answers = {}
        if reading:
            with ThreadPoolExecutor(max_workers=max(1, self.settings.n_llm_workers)) as pool:
                futures = {id(p): pool.submit(self._read, p, job[4], job[5]) for job, p in reading}
                answers = {key: f.result() for key, f in futures.items()}

        for work, source, found, dropped, _, _ in jobs:
            out, errors, coords, results = [], 0, 0, 0
            for p in found:
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
                                      f"{errors} of {len(found)} passages failed",
                                      fingerprint=work.fingerprint)
                continue
            yield Outcome(
                article_id=work.article_id, stage=self.name, source="", status=Status.OK,
                fingerprint=work.fingerprint,
                payload={"source": source, "passages": out},
                summary={"passages": len(found), "coordinates": coords, "results": results,
                         "passages_skipped": dropped},
            )

    def _read(self, passage, title, abstract):
        try:
            return self.client().extract(passage, title=title, abstract=abstract), None
        except Exception as exc:  # noqa: BLE001 - one failed call fails its article, not the batch
            logger.warning("prose passage failed: %s", exc)
            return {}, f"{type(exc).__name__}: {exc}"
