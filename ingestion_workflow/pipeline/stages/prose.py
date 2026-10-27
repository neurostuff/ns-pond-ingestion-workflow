"""Read the coordinates an article reports in its prose: what `analyses` is to the tables.

`passages` found them; this reads each with the prose model, the article's
title and abstract alongside -- which the fine-tune was trained with, and why
`metadata` runs between the two.
"""

from __future__ import annotations

import collections
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Iterator, List, Optional, Sequence, Tuple

from ingestion_workflow.catalog import ArticleRef, Artifact, Outcome, Status, fingerprint
from ingestion_workflow.clients.model_server import ModelUnavailable
from ingestion_workflow.prompts.prose_coordinates import PROSE_PROMPT_VERSION

from ..plan import StagePlan, Work
from ..stage import (
    NO_DIGESTS,
    Context,
    alone,
    metadata_fingerprint,
    take_back_or_block,
    taking_back,
)
from ..stage import metadata_read as read_fields
from .extract import READ_FROM_BAD_PAGE, read_from_bad_page
from .passages import passage_from, read_text

logger = logging.getLogger(__name__)

#: Bump when what is kept of the model's answer changes: `clean_answer`.
#: An article whose stored answers were read the same way is cleaned again
#: from them, without the model.
CLEAN_VERSION = "2026-10-12"

#: Who read a stored answer that names no producer: every such result was written
#: 2026-10-07 to 10-08, by nu-prose under the prompt as it stood 2026-10-06 to 10-11.
FIRST_PRODUCER = {"model": "nu-prose", "prompt_version": "2026-10-06.n5"}
#: The one reader those answers stand for: nu-prose-v2, nu-prose retrained without
#: roles, reads the same points and names. Any other reader reads their passages again.
FIRST_PRODUCER_STANDS_FOR = {"model": "nu-prose-v2", "prompt_version": "2026-10-11.nu-prose-v2"}

#: Why a passage was read by the model rather than given an earlier answer.
NO_EARLIER = "no earlier result"
TEXT_CHANGED = "passage text changed"
OTHER_PROMPT = "earlier prompt not interchangeable"
OTHER_MODEL = "earlier model not interchangeable"
METADATA_CHANGED = "title and abstract changed"


#: What the prose model reads of the article's metadata, beside each passage. The stage
#: reads the metadata only through `metadata_read`, and its fingerprint holds these
#: fields' digests, so a field the model does not read changes nothing here.
METADATA_READ = ("title", "abstract")


def metadata_read(payload: Optional[dict]) -> dict:
    return read_fields(payload, METADATA_READ)


def earlier_answers(payload: Optional[dict]) -> Dict[str, dict]:
    """An earlier prose payload's readings by the sha256 of each passage's
    `prose_text.normalized` text; of two copies of one text, the first.

    A payload that stored no answer, nu-prose's, keeps what was cleaned of it,
    which is given as the answer.
    """
    from ingestion_workflow.services.offsets import sha256
    from ingestion_workflow.services.prose_text import normalized

    out: Dict[str, dict] = {}
    for p in (payload or {}).get("passages", []):
        answer, by = p.get("answer"), p.get("producer")
        if answer is None:
            answer = {"space": p.get("space"), "analyses": p.get("analyses") or []}
            by = producer(**FIRST_PRODUCER)
        out.setdefault(sha256(normalized(p["text"])),
                       {"answer": answer, "producer": by, "inputs": p.get("inputs")})
    return out


def producer(model: Optional[str], prompt_version: str) -> dict:
    """Who read a passage, as a study_schema Producer."""
    from study_schema.models.paper_parse import Producer

    from ingestion_workflow.services.paper_parse import PRODUCER

    return Producer(name=PRODUCER, version=f"prose.{prompt_version}", stage=ProseStage.name,
                    model=model, prompt_version=prompt_version).model_dump(mode="json")


def _metadata_input(inputs: Optional[list]) -> Optional[str]:
    return next((i["fingerprint"] for i in inputs or []
                 if i.get("artifact_kind") == "metadata"), None)


def reuse(earlier: Dict[str, dict], text: str, prompt_version: str, model: Optional[str],
          inputs: list) -> Tuple[Optional[dict], Optional[str]]:
    """The earlier reading a passage of `text` read with `inputs` takes, or None and why not."""
    from ingestion_workflow.services.offsets import sha256
    from ingestion_workflow.services.prose_text import normalized

    if not earlier:
        return None, NO_EARLIER
    found = earlier.get(sha256(normalized(text)))
    if found is None:
        return None, TEXT_CHANGED
    made = {k: found["producer"][k] for k in ("model", "prompt_version")}
    now = {"model": model, "prompt_version": prompt_version}
    if made != now and not (made == FIRST_PRODUCER and now == FIRST_PRODUCER_STANDS_FOR):
        return None, OTHER_PROMPT if made["model"] == model else OTHER_MODEL
    if (found["inputs"] is not None
            and _metadata_input(found["inputs"]) != _metadata_input(inputs)):
        return None, METADATA_CHANGED
    return found, None


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

    def metadata_fingerprint(self, metadata: Artifact) -> Optional[str]:
        """The fingerprint of what the model reads of `metadata`, an OK one."""
        return metadata_fingerprint(metadata, METADATA_READ)

    def read_fingerprint(self, passages: Artifact, metadata: Optional[str]) -> str:
        """What the model's answers depend on; `metadata` is `metadata_fingerprint`'s."""
        # The metadata read with it: an article read before its title and
        # abstract were fetched is read again once they are. No passage, no
        # call, so then the metadata is not waited on.
        metadata = metadata if passages.summary.get("passages") else None
        return fingerprint("prose", PROSE_PROMPT_VERSION, self.settings.prose_model,
                           metadata if metadata is not None else "no metadata",
                           upstream=passages.fingerprint)

    def fingerprint_for(self, passages: Artifact, metadata: Optional[str]) -> str:
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
        extracted = ctx.catalog.artifacts(ids, "extract")
        for ref in refs:
            passages = upstream.get(ref.id, {}).get("")
            if passages is None or passages.status is not Status.OK:
                take_back_or_block(plan, ref, passages, artifacts.get(ref.id, {}).get(""))
                continue
            if read_from_bad_page(passages, downloads.get(ref.id, {}), extracted.get(ref.id, {})):
                # the passages stage blocks rather than takes back, so these stay OK
                plan.block(f"passages {READ_FROM_BAD_PAGE}")
                continue
            metadata = fetched.get(ref.id, {}).get("")
            metadata = metadata if metadata is not None and metadata.status is Status.OK else None
            # No passage, no call: the empty result costs nothing and lets
            # resolve pass the tables through.
            read = None
            if metadata is not None and passages.summary.get("passages"):
                read = self.metadata_fingerprint(metadata)
                if read is None:
                    plan.block(NO_DIGESTS)
                    continue
            fp = self.fingerprint_for(passages, read)
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

        found = [ctx.payload(work.upstream) or {} for work in works]
        ids = [w.article_id for w in works]
        metadata = ctx.catalog.artifacts(ids, "metadata")
        stored = ctx.catalog.artifacts(ids, self.name)
        reading, unreadable, read, read_fps, plans = [], {}, {}, {}, {}
        for work, payload in zip(works, found):
            meta_artifact = metadata.get(work.article_id, {}).get("")
            if meta_artifact is not None and not meta_artifact.ok:
                meta_artifact = None
            meta = metadata_read(ctx.payload(meta_artifact)) if meta_artifact is not None else {}
            meta_fp = (self.metadata_fingerprint(meta_artifact)
                       if meta_artifact is not None else None)
            read_fps[work.article_id] = self.read_fingerprint(work.upstream, meta_fp)
            try:
                text = read_text(ctx, payload) if payload.get("passages") else ""
            except LookupError as exc:
                # Read nothing rather than a passage the text no longer holds;
                # passages, re-run on the text as it is, will index it again.
                unreadable[work.article_id] = str(exc)
                continue
            read[work.article_id] = [passage_from(p, text) for p in payload.get("passages", [])]
            # A passage whose text, once normalized, was read before takes that answer, cleaned
            # again, wherever its span now lies: only a passage whose text changed is read again.
            before = stored.get(work.article_id, {}).get("")
            earlier = (earlier_answers(ctx.payload(before))
                       if before is not None and before.ok else {})
            plans[work.article_id] = []
            for passage in read[work.article_id]:
                inputs = self._inputs(passage.text, meta_fp)
                taken, reason = reuse(earlier, passage.text, PROSE_PROMPT_VERSION,
                                      self.settings.prose_model, inputs)
                plans[work.article_id].append((taken, reason, inputs))
                if taken is None:
                    reading.append((passage, meta.get("title") or "", meta.get("abstract") or ""))
        answers: List = []
        if reading:
            with ThreadPoolExecutor(max_workers=max(1, self.settings.n_llm_workers)) as pool:
                futures = [pool.submit(self._read, p, title, abstract)
                           for p, title, abstract in reading]
                # A `ModelUnavailable` from any read leaves here before anything is
                # yielded: recording one would replace each OK reading, and the answers
                # stored in it, with a failure.
                answers = [f.result() for f in futures]
        # After the reads: a stopped batch writes nothing, take-backs included.
        yield from back
        answered = iter(answers)
        current = producer(self.settings.prose_model, PROSE_PROMPT_VERSION)

        for work, payload in zip(works, found):
            if work.article_id in unreadable:
                yield Outcome.failure(work.article_id, self.name, "", unreadable[work.article_id],
                                      fingerprint=work.fingerprint)
                continue
            passages = list(zip(payload.get("passages", []), read[work.article_id],
                                plans[work.article_id]))
            # Taken before the article is made, so one that raises leaves the rest theirs.
            fresh = [next(answered) for _, _, (earlier, _, _) in passages if earlier is None]
            yield alone(self.name, work, lambda: self._outcome(
                work, payload, passages, iter(fresh), current, read_fps[work.article_id]))

    def _outcome(self, work, payload, passages, fresh, current, read_fingerprint) -> Outcome:
        """The article's outcome from its passages' answers: `fresh` the model's, in order,
        for those without an earlier one."""
        from ingestion_workflow.clients.prose_coordinates import clean_answer
        from ingestion_workflow.services.prose_text import NORMALIZED_VERSION

        out, errors, coords, named, unwritten = [], 0, 0, 0, 0
        reused, read_again, made_by = [], collections.Counter(), collections.Counter()
        for i, (p, passage, (earlier, reason, inputs)) in enumerate(passages):
            if earlier is not None:
                raw, error, by = earlier["answer"], None, earlier["producer"]
                reused.append(i)
            else:
                (raw, error), by = next(fresh), current
                read_again[reason] += 1
            made_by[by["prompt_version"]] += 1
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
                        "omitted": answer.get("omitted", []), "error": error, "answer": raw,
                        "producer": by, "inputs": inputs})
        if errors:
            # A partly read article would be cached as complete; retry it whole.
            return Outcome.failure(work.article_id, self.name, "",
                                   f"{errors} of {len(out)} passages failed",
                                   fingerprint=work.fingerprint)
        return Outcome(
            article_id=work.article_id, stage=self.name, source="", status=Status.OK,
            fingerprint=work.fingerprint,
            payload={"source": payload.get("source"), "read": payload.get("read"),
                     "space": payload.get("space"), "text_sha256": payload.get("text_sha256"),
                     "passages": out},
            # the passages these `span`s and indices are into, for roles to check
            summary={"source": payload.get("source"), "read": payload.get("read"),
                     "passages": len(out),
                     "coordinates": coords, "analyses": named, "unwritten": unwritten,
                     # which passages took an earlier answer of their text, the
                     # normalization that matched it, and why each other one was read
                     "reused": reused, "text_normalization": NORMALIZED_VERSION,
                     "read_again": dict(read_again), "prompt_versions": dict(made_by),
                     "read_fingerprint": read_fingerprint,
                     "passages_read": work.upstream.fingerprint},
        )

    def _inputs(self, passage_text: str, metadata: Optional[str]) -> list:
        """What a passage's answer is read from, as study_schema InputRefs: its text first,
        then the fingerprint of what was read of the metadata, if any was."""
        from study_schema.models.paper_parse import InputRef

        from ingestion_workflow.services.offsets import sha256

        refs = [InputRef(artifact_kind="passage_text", fingerprint=sha256(passage_text))]
        if metadata is not None:
            refs.append(InputRef(artifact_kind="metadata", fingerprint=metadata))
        return [r.model_dump(mode="json") for r in refs]

    def _read(self, passage, title, abstract):
        """The model's answer as it gave it, and the error if the call failed."""
        try:
            return self.client().read(passage, title=title, abstract=abstract), None
        except ModelUnavailable:
            raise
        except Exception as exc:  # noqa: BLE001 - one failed call fails its article, not the batch
            logger.warning("prose passage failed: %s", exc)
            return None, f"{type(exc).__name__}: {exc}"
