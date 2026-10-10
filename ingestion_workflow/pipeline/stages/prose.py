"""Read the coordinates an article reports in its prose: what `analyses` is to the tables.

`passages` found them; this reads each with the prose model, the article's
title and abstract alongside -- which the fine-tune was trained with, and why
`metadata` runs between the two.
"""

from __future__ import annotations

import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Iterator, List, Optional, Sequence

from ingestion_workflow.catalog import ArticleRef, Artifact, Outcome, Status, fingerprint
from ingestion_workflow.prompts.prose_coordinates import PROSE_PROMPT_VERSION

from ..plan import StagePlan, Work
from ..stage import Context, take_back_or_block, taking_back
from .passages import passage_from, read_text
from .resolve import KEPT_ROLES

logger = logging.getLogger(__name__)

#: Bump when what is kept of the model's answer changes: `clean_answer`.
CLEAN_VERSION = "in-order"


class ProseStage:
    name = "prose"
    requires = "passages"

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

    def fingerprint_for(self, passages: Artifact, metadata: Optional[Artifact]) -> str:
        # The metadata read with it: an article read before its title and
        # abstract were fetched is read again once they are.
        return fingerprint("prose", PROSE_PROMPT_VERSION, CLEAN_VERSION, self.settings.prose_model,
                           metadata.fingerprint if metadata is not None else "no metadata",
                           upstream=passages.fingerprint)

    def plan(
        self,
        ctx: Context,
        refs: Sequence[ArticleRef],
        artifacts: Dict[str, Dict[str, Artifact]],
        upstream: Dict[str, Dict[str, Artifact]],
    ) -> StagePlan:
        plan = StagePlan(stage=self.name)
        ids = [ref.id for ref in refs]
        attempts = ctx.catalog.attempt_counts(ids, self.name, "")
        fetched = ctx.catalog.artifacts(ids, "metadata")
        for ref in refs:
            passages = upstream.get(ref.id, {}).get("")
            if passages is None or passages.status is not Status.OK:
                take_back_or_block(plan, ref, passages, artifacts.get(ref.id, {}).get(""))
                continue
            metadata = fetched.get(ref.id, {}).get("")
            metadata = metadata if metadata is not None and metadata.status is Status.OK else None
            # No passage, no call: the empty result costs nothing, lets resolve
            # pass the tables through, and does not wait on metadata.
            fp = self.fingerprint_for(passages, metadata if passages.summary.get("passages") else None)
            existing = artifacts.get(ref.id, {}).get("")
            if ctx.is_fresh(existing, fp):
                plan.fresh += 1
                continue
            count, last = attempts.get(ref.id, (0, None))
            if not ctx.should_attempt(existing, count, last, self.name):
                plan.permanent += 1
                continue
            plan.pending.append(Work(ref=ref, source="", fingerprint=fp, upstream=passages))
        return plan

    def execute(self, ctx: Context, works: List[Work]) -> Iterator[Outcome]:
        back, works = taking_back(self.name, works)
        yield from back
        found = [ctx.payload(work.upstream) or {} for work in works]
        metadata = ctx.catalog.artifacts([w.article_id for w in works], "metadata")
        reading, unreadable, read = [], {}, {}
        for work, payload in zip(works, found):
            meta_artifact = metadata.get(work.article_id, {}).get("")
            meta = (ctx.payload(meta_artifact) or {}) if meta_artifact is not None and meta_artifact.ok else {}
            try:
                text = read_text(ctx, payload) if payload.get("passages") else ""
            except LookupError as exc:
                # Read nothing rather than a passage the text no longer holds;
                # passages, re-run on the text as it is, will index it again.
                unreadable[work.article_id] = str(exc)
                continue
            read[work.article_id] = [passage_from(p, text) for p in payload.get("passages", [])]
            reading += [(p, meta.get("title") or "", meta.get("abstract") or "") for p in read[work.article_id]]
        answers: List = []
        if reading:
            with ThreadPoolExecutor(max_workers=max(1, self.settings.n_llm_workers)) as pool:
                futures = [pool.submit(self._read, p, title, abstract) for p, title, abstract in reading]
                answers = [f.result() for f in futures]
        answered = iter(answers)

        for work, payload in zip(works, found):
            if work.article_id in unreadable:
                yield Outcome.failure(work.article_id, self.name, "", unreadable[work.article_id],
                                      fingerprint=work.fingerprint)
                continue
            out, errors, coords, kept = [], 0, 0, 0
            for p, passage in zip(payload.get("passages", []), read[work.article_id]):
                answer, error = next(answered)
                errors += error is not None
                points = [q for a in answer.get("analyses", []) for q in a["points"]]
                coords += len(points)
                kept += sum(1 for q in points if q["role"] in KEPT_ROLES)
                out.append({"span": p["span"], "from_legend": passage.from_legend,
                            "text": passage.text, "heading": passage.heading,
                            "space": answer.get("space") or p.get("space"),
                            "analyses": answer.get("analyses", []), "error": error})
            if errors:
                # A partly read article would be cached as complete; retry it whole.
                yield Outcome.failure(work.article_id, self.name, "",
                                      f"{errors} of {len(out)} passages failed", fingerprint=work.fingerprint)
                continue
            yield Outcome(
                article_id=work.article_id, stage=self.name, source="", status=Status.OK,
                fingerprint=work.fingerprint,
                payload={"source": payload.get("source"), "read": payload.get("read"),
                         "space": payload.get("space"), "text_sha256": payload.get("text_sha256"),
                         "passages": out},
                summary={"source": payload.get("source"), "read": payload.get("read"), "passages": len(out),
                         "coordinates": coords, "kept": kept},
            )

    def _read(self, passage, title, abstract):
        try:
            return self.client().extract(passage, title=title, abstract=abstract), None
        except Exception as exc:  # noqa: BLE001 - one failed call fails its article, not the batch
            logger.warning("prose passage failed: %s", exc)
            return {}, f"{type(exc).__name__}: {exc}"
