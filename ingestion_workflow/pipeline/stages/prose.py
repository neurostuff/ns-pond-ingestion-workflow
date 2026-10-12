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
from ingestion_workflow.prompts.prose_coordinates import PROSE_PROMPT_VERSION

from ..plan import StagePlan, Work
from ..stage import Context, take_back_or_block, taking_back
from .extract import READ_FROM_BAD_PAGE, read_from_bad_page
from .passages import passage_from, read_text

logger = logging.getLogger(__name__)

#: Bump when what is kept of the model's answer changes: `clean_answer`.
#: An article whose stored answers were read the same way is cleaned again
#: from them, without the model.
CLEAN_VERSION = "role-word-not-read"

#: Prompts whose answers stand for one another, oldest first: the role-free
#: prompt asks for the same points and names, only without the role word that
#: nothing reads. The oldest stands for all of them in the read fingerprint, so
#: moving to a later one makes no answer stale. A prompt outside the family
#: reads every passage again.
INTERCHANGEABLE_PROMPTS = ("2026-10-06.n5", "2026-10-11.n5-norole")
#: The served models of that family: nu-prose and the role-free nu-prose.
#: An answer is reused only from one of them, for one of them.
INTERCHANGEABLE_MODELS = ("nu-prose", "nu-prose-norole")
#: Who read an answer stored before producers were recorded: every such
#: result was written 2026-10-07 to 10-08, by nu-prose under the prompt as it
#: stood from 2026-10-06 until 2026-10-11.
LEGACY_MODEL, LEGACY_PROMPT = "nu-prose", "2026-10-06.n5"

#: Why a passage was read by the model rather than given an earlier answer.
NO_EARLIER = "no earlier result"
TEXT_CHANGED = "passage text changed"
EARLIER_FAILED = "earlier reading failed"
OTHER_PROMPT = "earlier prompt not interchangeable"
OTHER_MODEL = "earlier model not interchangeable"
METADATA_CHANGED = "title and abstract changed"
#: Why an earlier answer was given to a passage instead, each with its rule's version:
#: the same text, or the same once `prose_text.normalized`.
EXACT = "exact"
NORMALIZED = "normalized"
#: An answer stored before the prose stage recorded its producer.
UNRECORDED = "unrecorded"


def reading_basis(prompt_version: str) -> str:
    """The prompt the read fingerprint names: its family's oldest."""
    if prompt_version in INTERCHANGEABLE_PROMPTS:
        return INTERCHANGEABLE_PROMPTS[0]
    return prompt_version


def earlier_answers(payload: Optional[dict]) -> Dict[str, Dict[str, dict]]:
    """An earlier prose payload's readings by the sha256 of each passage's text: `exact` by
    the text itself, `normalized` by `prose_text.normalized` of it.

    A payload from before the stage stored the model's answer keeps only what
    was cleaned of it; that is given as the answer, with no producer recorded.
    """
    from ingestion_workflow.services.offsets import sha256
    from ingestion_workflow.services.prose_text import normalized

    out: Dict[str, Dict[str, dict]] = {EXACT: {}, NORMALIZED: {}}
    for p in (payload or {}).get("passages", []):
        if not isinstance(p.get("text"), str):
            continue
        answer = p.get("answer")
        if answer is None and "producer" not in p and p.get("error") is None:
            answer = {"space": p.get("space"), "analyses": p.get("analyses") or []}
        entry = {"answer": answer, "error": p.get("error"), "producer": p.get("producer"),
                 "inputs": p.get("inputs")}
        # The first answered copy of a text wins; a failed copy gives way to an answered one.
        for rule, key in ((EXACT, sha256(p["text"])), (NORMALIZED, sha256(normalized(p["text"])))):
            if key not in out[rule] or _failed(out[rule][key]) and not _failed(entry):
                out[rule][key] = entry
    return out


def _failed(entry: dict) -> bool:
    return entry["error"] is not None or entry["answer"] is None


def earlier_reading(earlier: Dict[str, Dict[str, dict]], text: str) -> Tuple[Optional[dict], str]:
    """The earlier reading of `text`, exact before normalized, and the rule that matched it."""
    from ingestion_workflow.services.offsets import sha256
    from ingestion_workflow.services.prose_text import NORMALIZED_VERSION, normalized

    found = earlier[EXACT].get(sha256(text))
    if found is not None:
        return found, f"{EXACT}.1"
    found = earlier[NORMALIZED].get(sha256(normalized(text)))
    return found, f"{NORMALIZED}.{NORMALIZED_VERSION}"


def _metadata_input(inputs: Optional[list]) -> Optional[str]:
    return next((i["fingerprint"] for i in inputs or []
                 if i.get("artifact_kind") == "metadata"), None)


def reuse(earlier: Optional[dict], rule: str, prompt_version: str, model: Optional[str],
          inputs: list) -> Tuple[bool, str]:
    """Whether a passage read with `inputs` takes `earlier`, the reading `rule` found, and why."""
    if earlier is None:
        return False, TEXT_CHANGED
    if _failed(earlier):
        return False, EARLIER_FAILED
    made = earlier["producer"] or {"model": LEGACY_MODEL, "prompt_version": LEGACY_PROMPT}
    if made.get("model") != model and not (
            made.get("model") in INTERCHANGEABLE_MODELS and model in INTERCHANGEABLE_MODELS):
        return False, OTHER_MODEL
    made_with = made.get("prompt_version")
    if made_with != prompt_version and not (
            made_with in INTERCHANGEABLE_PROMPTS and prompt_version in INTERCHANGEABLE_PROMPTS):
        return False, OTHER_PROMPT
    if (earlier["inputs"] is not None
            and _metadata_input(earlier["inputs"]) != _metadata_input(inputs)):
        return False, METADATA_CHANGED
    return True, rule


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
        return fingerprint("prose", reading_basis(PROSE_PROMPT_VERSION), self.settings.prose_model,
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
        reading, unreadable, read, read_fps, plans = [], {}, {}, {}, {}
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
            # A passage whose exact text was read before takes that answer, cleaned again,
            # wherever its span now lies: only a passage whose text changed is read again.
            before = stored.get(work.article_id, {}).get("")
            earlier = (earlier_answers(ctx.payload(before))
                       if before is not None and before.ok else None)
            meta = (ctx.payload(meta_artifact) or {}) if meta_artifact is not None else {}
            plans[work.article_id] = []
            for passage in read[work.article_id]:
                inputs = self._inputs(passage.text, meta_artifact)
                found_before, rule = (earlier_reading(earlier, passage.text)
                                      if earlier else (None, ""))
                reused, reason = reuse(found_before, rule, PROSE_PROMPT_VERSION,
                                       self.settings.prose_model, inputs)
                if not earlier or not earlier[EXACT]:
                    reason = NO_EARLIER
                plans[work.article_id].append((found_before if reused else None, reason, inputs))
                if not reused:
                    reading.append((passage, meta.get("title") or "", meta.get("abstract") or ""))
        answers: List = []
        if reading:
            with ThreadPoolExecutor(max_workers=max(1, self.settings.n_llm_workers)) as pool:
                futures = [pool.submit(self._read, p, title, abstract)
                           for p, title, abstract in reading]
                answers = [f.result() for f in futures]
        answered = iter(answers)
        producer = self._producer(PROSE_PROMPT_VERSION)

        for work, payload in zip(works, found):
            if work.article_id in unreadable:
                yield Outcome.failure(work.article_id, self.name, "", unreadable[work.article_id],
                                      fingerprint=work.fingerprint)
                continue
            out, errors, coords, named, unwritten, stray_roles = [], 0, 0, 0, 0, 0
            reused, read_again, made_by = {}, collections.Counter(), collections.Counter()
            unrecorded = 0
            passages = zip(payload.get("passages", []), read[work.article_id],
                           plans[work.article_id])
            for i, (p, passage, (earlier, reason, inputs)) in enumerate(passages):
                if earlier is not None:
                    raw, error = earlier["answer"], None
                    by = earlier["producer"] or self._producer(None)
                    reused.setdefault(reason, []).append(i)
                else:
                    (raw, error), by = next(answered), producer
                    read_again[reason] += 1
                made_by[by.get("prompt_version") or UNRECORDED] += 1
                unrecorded += by.get("version") == f"prose.{UNRECORDED}"
                answer = clean_answer(raw, passage.text) if error is None else {}
                errors += error is not None
                points = [q for a in answer.get("analyses", []) for q in a["points"]]
                coords += len(points)
                named += len(answer.get("analyses", []))
                unwritten += sum(a.get("unwritten", 0)
                                 for a in answer.get("analyses", []) + answer.get("omitted", []))
                stray_roles += answer.get("stray_roles", 0)
                out.append({"span": p["span"], "from_legend": passage.from_legend,
                            "text": passage.text, "heading": passage.heading,
                            "space": answer.get("space") or p.get("space"),
                            "analyses": answer.get("analyses", []),
                            "omitted": answer.get("omitted", []), "error": error, "answer": raw,
                            "producer": by, "inputs": inputs})
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
                         # role words of reused answers of the role-bearing prompt
                         "stray_roles": stray_roles,
                         # which passages took an earlier answer of their text, by the rule
                         # that matched it, and why each other one was read
                         "reused": reused,
                         "read_again": dict(read_again), "prompt_versions": dict(made_by),
                         "unrecorded_producers": unrecorded,
                         "read_fingerprint": read_fps[work.article_id],
                         "passages_read": work.upstream.fingerprint},
            )

    def _inputs(self, passage_text: str, meta_artifact: Optional[Artifact]) -> list:
        """What a passage's answer is read from, as study_schema InputRefs: its text first."""
        from study_schema.models.paper_parse import InputRef

        from ingestion_workflow.services.offsets import sha256

        refs = [InputRef(artifact_kind="passage_text", fingerprint=sha256(passage_text))]
        if meta_artifact is not None:
            refs.append(InputRef(artifact_kind="metadata", fingerprint=meta_artifact.fingerprint))
        return [r.model_dump(mode="json") for r in refs]

    def _producer(self, prompt_version: Optional[str]) -> dict:
        """Who read a passage, as a study_schema Producer. An answer stored without one (no
        `prompt_version` given) was nu-prose's: `LEGACY_MODEL` under `LEGACY_PROMPT`."""
        from study_schema.models.paper_parse import Producer

        from ingestion_workflow.services.paper_parse import PRODUCER

        if prompt_version is None:
            producer = Producer(name=PRODUCER, version=f"prose.{UNRECORDED}", stage=self.name,
                                model=LEGACY_MODEL, prompt_version=LEGACY_PROMPT)
        else:
            producer = Producer(name=PRODUCER, version=f"prose.{prompt_version}", stage=self.name,
                                model=self.settings.prose_model, prompt_version=prompt_version)
        return producer.model_dump(mode="json")

    def _read(self, passage, title, abstract):
        """The model's answer as it gave it, and the error if the call failed."""
        try:
            return self.client().read(passage, title=title, abstract=abstract), None
        except Exception as exc:  # noqa: BLE001 - one failed call fails its article, not the batch
            logger.warning("prose passage failed: %s", exc)
            return None, f"{type(exc).__name__}: {exc}"
