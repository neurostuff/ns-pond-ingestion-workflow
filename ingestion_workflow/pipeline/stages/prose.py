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
from .extract import READ_FROM_BAD_PAGE, read_from_bad_page
from .passages import passage_from, read_text

logger = logging.getLogger(__name__)

#: Bump when what is kept of the model's answer changes: `clean_answer`.
#: An article whose stored answers were read the same way is cleaned again
#: from them, without the model.
CLEAN_VERSION = "named-empty-kept"


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

    def read_fingerprint(self, passages: Artifact, metadata: Optional[Artifact]) -> str:
        """What the model's answers depend on."""
        # The metadata read with it: an article read before its title and
        # abstract were fetched is read again once they are. No passage, no
        # call, so then the metadata is not waited on.
        metadata = metadata if passages.summary.get("passages") else None
        return fingerprint("prose", PROSE_PROMPT_VERSION, self.settings.prose_model,
                           metadata.fingerprint if metadata is not None else "no metadata",
                           upstream=passages.fingerprint)

    def fingerprint_for(self, passages: Artifact, metadata: Optional[Artifact]) -> str:
        return fingerprint("prose clean", CLEAN_VERSION,
                           upstream=self.read_fingerprint(passages, metadata))

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
        downloads = ctx.catalog.artifacts(ids, "download")
        for ref in refs:
            passages = upstream.get(ref.id, {}).get("")
            if passages is None or passages.status is not Status.OK:
                take_back_or_block(plan, ref, passages, artifacts.get(ref.id, {}).get(""))
                continue
            if read_from_bad_page(passages, downloads.get(ref.id, {})):
                # the passages stage blocks rather than takes back, so these stay OK
                plan.block(f"passages {READ_FROM_BAD_PAGE}")
                continue
            metadata = fetched.get(ref.id, {}).get("")
            metadata = metadata if metadata is not None and metadata.status is Status.OK else None
            # No passage, no call: the empty result costs nothing and lets
            # resolve pass the tables through.
            fp = self.fingerprint_for(passages, metadata)
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
        from ingestion_workflow.clients.prose_coordinates import clean_answer

        found = [ctx.payload(work.upstream) or {} for work in works]
        ids = [w.article_id for w in works]
        metadata = ctx.catalog.artifacts(ids, "metadata")
        stored = ctx.catalog.artifacts(ids, self.name)
        reading, unreadable, read, read_fps, kept_answers = [], {}, {}, {}, {}
        for work, payload in zip(works, found):
            meta_artifact = metadata.get(work.article_id, {}).get("")
            if meta_artifact is not None and not meta_artifact.ok:
                meta_artifact = None
            read_fp = self.read_fingerprint(work.upstream, meta_artifact)
            read_fps[work.article_id] = read_fp
            try:
                text = read_text(ctx, payload) if payload.get("passages") else ""
            except LookupError as exc:
                # Read nothing rather than a passage the text no longer holds;
                # passages, re-run on the text as it is, will index it again.
                unreadable[work.article_id] = str(exc)
                continue
            read[work.article_id] = [passage_from(p, text) for p in payload.get("passages", [])]
            # Answers the model gave for this same reading are cleaned again, not re-read.
            before = stored.get(work.article_id, {}).get("")
            if (before is not None and before.ok
                    and before.summary.get("read_fingerprint") == read_fp):
                answers = [p.get("answer")
                           for p in (ctx.payload(before) or {}).get("passages", [])]
                if len(answers) == len(read[work.article_id]) and None not in answers:
                    kept_answers[work.article_id] = answers
                    continue
            meta = (ctx.payload(meta_artifact) or {}) if meta_artifact is not None else {}
            reading += [(p, meta.get("title") or "", meta.get("abstract") or "")
                        for p in read[work.article_id]]
        answers: List = []
        if reading:
            with ThreadPoolExecutor(max_workers=max(1, self.settings.n_llm_workers)) as pool:
                futures = [pool.submit(self._read, p, title, abstract)
                           for p, title, abstract in reading]
                answers = [f.result() for f in futures]
        answered = iter(answers)

        for work, payload in zip(works, found):
            if work.article_id in unreadable:
                yield Outcome.failure(work.article_id, self.name, "", unreadable[work.article_id],
                                      fingerprint=work.fingerprint)
                continue
            stored_answers = kept_answers.get(work.article_id)
            out, errors, coords, named, unwritten = [], 0, 0, 0, 0
            passages = zip(payload.get("passages", []), read[work.article_id])
            for i, (p, passage) in enumerate(passages):
                if stored_answers is not None:
                    raw, error = stored_answers[i], None
                else:
                    raw, error = next(answered)
                answer = clean_answer(raw, passage.text) if error is None else {}
                errors += error is not None
                points = [q for a in answer.get("analyses", []) for q in a["points"]]
                coords += len(points)
                named += len(answer.get("analyses", []))
                unwritten += sum(a.get("unwritten", 0)
                                 for a in answer.get("analyses", []) + answer.get("omitted", []))
                out.append({"span": p["span"], "from_legend": passage.from_legend,
                            "text": passage.text, "heading": passage.heading,
                            "space": answer.get("space") or p.get("space"),
                            "analyses": answer.get("analyses", []),
                            "omitted": answer.get("omitted", []), "error": error, "answer": raw})
            if errors:
                # A partly read article would be cached as complete; retry it whole.
                yield Outcome.failure(work.article_id, self.name, "",
                                      f"{errors} of {len(out)} passages failed",
                                      fingerprint=work.fingerprint)
                continue
            yield Outcome(
                article_id=work.article_id, stage=self.name, source="", status=Status.OK,
                fingerprint=work.fingerprint,
                payload={"source": payload.get("source"), "read": payload.get("read"),
                         "space": payload.get("space"), "text_sha256": payload.get("text_sha256"),
                         "passages": out},
                # the passages these `span`s and indices are into, for roles to check
                summary={"source": payload.get("source"), "read": payload.get("read"),
                         "passages": len(out),
                         "coordinates": coords, "analyses": named, "unwritten": unwritten,
                         "read_fingerprint": read_fps[work.article_id],
                         "passages_read": work.upstream.fingerprint},
            )

    def _read(self, passage, title, abstract):
        """The model's answer as it gave it, and the error if the call failed."""
        try:
            return self.client().read(passage, title=title, abstract=abstract), None
        except Exception as exc:  # noqa: BLE001 - one failed call fails its article, not the batch
            logger.warning("prose passage failed: %s", exc)
            return None, f"{type(exc).__name__}: {exc}"
