"""The stages run away from ns-pond: `record`, `evidence` and `normalize`.

Each holds, per article and extractor family (the source), what a run made on a cluster or
on beast, imported by `ingest offload import`. `record` and `evidence` never run here, so
their plans never have work. `normalize` runs in-process once `normalize_model` names the
task-design model; without one it too waits for an import.

    record      run identity + text_sha256 + the parse keys the record points at
    evidence    run identity + text_sha256 + the record's fingerprint and content, same source
    normalize   run identity + the content of the record it read + the vocabulary release

`normalize` reads the evidence record, or the record itself when evidence was skipped.

A parse change that keeps the keys a record points at leaves the record fresh. So does one
uploaded as a revision that carries every analysis (`carried_from`): each step since the
parse the record read accepted, edited, split or merged every analysis, added none, and kept
or extended the text. Neurostore carried the record's claims along, so the record stays as
imported, keyed to the parse it read, and evidence reads the text the record read.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Set

from ingestion_workflow.catalog import ArticleRef, Artifact, Outcome, Run, Status, fingerprint

from ..plan import StagePlan, Work
from ..stage import Context, recorded_path

#: Why an article has no fresh output of an offloaded stage.
AWAITS_IMPORT = "waits for an offload import"
NO_NORMALIZE_INPUT = "waits for an evidence record (or a skipped evidence run's record)"
NO_PARSE = "no coordinate parse"
#: Why a normalize fails when an answer is missing and nothing serves the model.
NO_SERVER = "{} task(s) have no stored answer and normalize_model has no url to ask"
NO_RECORD = "no record of this source"
#: The reason of a skip for want of a usable answer: cut short, or not JSON (pull writes it).
NO_ANSWER = "no prediction: {}"

OFFLOADED_STAGES = ("record", "evidence", "normalize")


def record_digest(record: Mapping[str, Any]) -> str:
    """The sha256 of a record's content: canonical JSON, without `extraction_metadata`, which
    import stamps and which says who made the record, not what it holds."""
    held = {k: v for k, v in record.items() if k != "extraction_metadata"}
    raw = json.dumps(held, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def drop_siblings(record: Dict[str, Any]) -> int:
    """Remove the per-field normalizations study_schema#56 replaced with sidecars, in place;
    how many were removed. The 27B's records carry them (pondie-astra restores the pre-#56
    shape), and the schema refuses them; what they said, normalize makes again."""
    dropped = 0
    for group in record.get("groups") or []:
        if not isinstance(group, dict):
            continue
        dropped += group.pop("age_unit_normalized", None) is not None
        # every distribution (sex, gender, race, handedness, ...) carried one per category
        for slot in group.values():
            for entry in slot if isinstance(slot, list) else ():
                if isinstance(entry, dict):
                    dropped += entry.pop("category_normalized", None) is not None
    return dropped


def _key(value: Any) -> Optional[str]:
    if isinstance(value, dict):
        value = value.get("value")
    return value if isinstance(value, str) else None


def parse_keys(record: Mapping[str, Any]) -> Set[str]:
    """The parse keys a record points at: its coordinate sets' `local_id` and its analyses'
    `source_table_analysis`."""
    named = [_key(s.get("local_id")) for s in record.get("coordinate_sets") or []
             if isinstance(s, dict)]
    named += [_key(a.get("source_table_analysis")) for a in record.get("analyses") or []
              if isinstance(a, dict)]
    return {key for key in named if key}


def record_fingerprint(run: Run, text_sha256: Optional[str], keys: Iterable[str]) -> str:
    return fingerprint("record", run.identity, text_sha256, sorted(keys))


def evidence_fingerprint(run: Run, text_sha256: Optional[str], record: Artifact) -> str:
    """The record's content as well as its fingerprint: evidence cites the record's values,
    and a second run of one model can word them otherwise."""
    return fingerprint("evidence", run.identity, text_sha256, record.fingerprint,
                       record.summary.get("record_digest"))


def normalize_fingerprint(run: Run, record: Artifact, vocabulary: Any) -> str:
    return fingerprint("normalize", run.identity, record.summary.get("record_digest"),
                       json.dumps(vocabulary, sort_keys=True))


class Parse:
    """What an offloaded stage reads of an article's coordinate parse: its text and keys, and
    the file they were read from."""

    def __init__(self, text_sha256: Optional[str], keys: Set[str],
                 path: Optional[Path] = None, parse_id: Optional[str] = None) -> None:
        self.text_sha256 = text_sha256
        self.keys = keys
        self.path = path
        self.parse_id = parse_id

    @classmethod
    def of(cls, catalog_root: Path, sync: Optional[Artifact]) -> Optional["Parse"]:
        """The parse sync wrote for an article, or None when there is none to read."""
        if sync is None or not sync.ok or not (sync.summary or {}).get("path"):
            return None
        path = (recorded_path(catalog_root, sync.summary["path"])
                / "parse" / "coordinate_parse.json")
        try:
            held = json.loads(path.read_text("utf-8"))
        except (OSError, ValueError):
            return None
        return cls(held.get("text_sha256"),
                   {a["key"] for a in held.get("analyses") or [] if a.get("key")}, path,
                   held.get("parse_id"))


def carried_from(record: Artifact, parse: Optional[Parse],
                 upload: Optional[Artifact]) -> Optional[str]:
    """The parse_id of the earlier parse `record` read, when the revisions uploaded since
    carry it to `parse` whole; None otherwise.

    `parse` must be the parse last uploaded, and the record must have read the text and the
    keys of a parse that a step of the upload's `revisions` revised. From that step on, every
    step must carry every analysis (`carries_all`) and keep or extend the text, and the keys
    the record names must reach keys `parse` has."""
    summary = (upload.summary or {}) if upload is not None and upload.ok else {}
    if parse is None or not parse.parse_id or summary.get("parse_id") != parse.parse_id:
        return None
    named = set((record.summary or {}).get("parse_keys") or ())
    text = (record.summary or {}).get("text_sha256")
    steps = summary.get("revisions") or []
    start = next((i for i in range(len(steps) - 1, -1, -1)
                  if steps[i].get("revised_text_sha256") == text
                  and named <= set(steps[i].get("map") or {})), None)
    if start is None:
        return None
    keys = named
    for step in steps[start:]:
        if not step.get("carries_all") or step.get("text") not in ("same", "extended"):
            return None
        mapped = step.get("map") or {}
        if not keys <= set(mapped):
            return None
        keys = {new for key in keys for new in mapped[key]}
    return steps[start]["revision_of"] if keys <= parse.keys else None


class _Runs:
    """The runs a batch's artifacts name, each read once."""

    def __init__(self, ctx: Context) -> None:
        self._ctx = ctx
        self._held: Dict[str, Optional[Run]] = {}

    def __call__(self, run_id: Optional[str]) -> Optional[Run]:
        if not run_id:
            return None
        if run_id not in self._held:
            self._held[run_id] = self._ctx.catalog.run(run_id)
        return self._held[run_id]


#: The error of a skip for a request over the spec's `max_input_tokens` (export's OVER).
_OVER = re.compile(r"^over (\d+) input tokens")


@dataclass(frozen=True)
class Reach:
    """What a spec asking for records can send: its version, its `max_input_tokens`, and
    whether it trims the text (`record_text` other than "full")."""

    version: str
    max_input_tokens: int
    trims: bool


def _asked_again(mine: Artifact, run: Run, retry: Optional[str],
                 reach: Optional[Reach] = None) -> bool:
    """Whether the paper of `mine`, a skip, is asked again: by `retry`, a version that decodes
    otherwise (sampled or guided), when another version had no usable answer for it; or, when
    its request was over the limit, by a spec that sends longer requests (`reach`) or a
    shorter text than the version that skipped it. A skipped record's skip is the same
    whoever asks, and so is an over-long one for a spec that reaches no further."""
    error = mine.error or ""
    if (retry is not None and run.version not in (None, retry)
            and error.startswith(NO_ANSWER.format(""))):
        return True
    over = _OVER.match(error)
    return bool(over and reach is not None and run.version != reach.version
                and (reach.max_input_tokens > int(over.group(1)) or reach.trims))


class _OffloadedStage:
    """A stage whose output is imported. Fresh per source when its fingerprint, worked out
    again from what it read, still matches; a SKIPPED one counts as done, but for a retry
    (`_asked_again`)."""

    name: str
    requires: str

    def __init__(self, settings) -> None:
        self.settings = settings

    def expected(self, ctx: Context, run: Run, mine: Artifact, inputs: Dict[str, Any],
                 source: str) -> Optional[str]:
        raise NotImplementedError

    def inputs(self, ctx: Context, ids: List[str], upstream) -> Dict[str, Dict[str, Any]]:
        raise NotImplementedError

    def fresh_sources(
        self,
        ctx: Context,
        ids: Sequence[str],
        artifacts: Dict[str, Dict[str, Artifact]],
        upstream: Dict[str, Dict[str, Artifact]],
        retry: Optional[str] = None,
        reach: Optional[Reach] = None,
    ) -> Dict[str, Set[str]]:
        """{article: the sources whose artifact is fresh}, for `retry`'s run when one is
        named, and for a spec that reaches `reach`."""
        runs = _Runs(ctx)
        inputs = self.inputs(ctx, list(ids), upstream)
        out: Dict[str, Set[str]] = {}
        for article_id in ids:
            for source, mine in artifacts.get(article_id, {}).items():
                run = runs(mine.run_id)
                if run is None or mine.status not in (Status.OK, Status.SKIPPED):
                    continue
                if mine.status is Status.SKIPPED:
                    if not _asked_again(mine, run, retry, reach):
                        out.setdefault(article_id, set()).add(source)
                elif (
                        self.expected(ctx, run, mine, inputs.get(article_id, {}), source)
                        == mine.fingerprint and ctx.is_fresh(mine, mine.fingerprint)):
                    out.setdefault(article_id, set()).add(source)
        return out

    def plan(
        self,
        ctx: Context,
        refs: Sequence[ArticleRef],
        artifacts: Dict[str, Dict[str, Artifact]],
        upstream: Dict[str, Dict[str, Artifact]],
    ) -> StagePlan:
        plan = StagePlan(stage=self.name)
        fresh = self.fresh_sources(ctx, [ref.id for ref in refs], artifacts, upstream)
        for ref in refs:
            if fresh.get(ref.id):
                plan.add_fresh(ref)
            else:
                plan.wait(ref, AWAITS_IMPORT)
        return plan

    def execute(self, ctx: Context, works: List[Work]) -> Iterator[Outcome]:
        """Nothing runs in-process yet: a plan here never has work."""
        return iter(())


def _parses(ctx: Context, ids: List[str]) -> Dict[str, Optional[Parse]]:
    synced = ctx.catalog.artifacts(ids, "sync")
    return {article_id: Parse.of(ctx.catalog.root, synced.get(article_id, {}).get(""))
            for article_id in ids}


def _with_uploads(ctx: Context, ids: List[str]) -> Dict[str, Dict[str, Any]]:
    uploads = ctx.catalog.artifacts(ids, "upload")
    return {article_id: {"parse": parse, "upload": uploads.get(article_id, {}).get("")}
            for article_id, parse in _parses(ctx, ids).items()}


class RecordStage(_OffloadedStage):
    """A schema-extraction record per extractor family, pointing into the article's parse,
    or into an earlier one the uploaded revisions carry whole to it (`carried_from`)."""

    name = "record"
    requires = "sync"

    def inputs(self, ctx, ids, upstream):
        return _with_uploads(ctx, ids)

    def expected(self, ctx, run, mine, inputs, source):
        parse = inputs.get("parse")
        if parse is None:
            return None
        named = set(mine.summary.get("parse_keys") or ())
        here = record_fingerprint(run, parse.text_sha256, named & parse.keys)
        if here != mine.fingerprint and carried_from(mine, parse, inputs.get("upload")) \
                and record_fingerprint(run, mine.summary.get("text_sha256"), named) \
                == mine.fingerprint:
            return mine.fingerprint
        return here


class EvidenceStage(_OffloadedStage):
    """The record of the same source with evidence spans in its text."""

    name = "evidence"
    requires = "record"

    def inputs(self, ctx, ids, upstream):
        return {article_id: {**held, "records": upstream.get(article_id, {})}
                for article_id, held in _with_uploads(ctx, ids).items()}

    def expected(self, ctx, run, mine, inputs, source):
        parse, record = inputs.get("parse"), inputs.get("records", {}).get(source)
        if parse is None or record is None or not record.ok:
            return None
        # A carried record's evidence reads the text the record read.
        text = (record.summary.get("text_sha256")
                if carried_from(record, parse, inputs.get("upload")) else parse.text_sha256)
        return evidence_fingerprint(run, text, record)


def normalize_input(evidence: Optional[Artifact],
                    record: Optional[Artifact]) -> Optional[Artifact]:
    """The artifact normalize reads: the evidence record, or the record when evidence was
    skipped. None while neither is there."""
    if evidence is not None and evidence.status is Status.OK:
        return evidence
    if evidence is not None and evidence.status is Status.SKIPPED and record is not None \
            and record.ok:
        return record
    return None


class NormalizeStage(_OffloadedStage):
    """The record's tasks, conditions and contrasts normalized, for the same source: each task's
    design from the task-design model `normalize_model` names, each analysis's structure and
    contrast identity assembled from the record and that design, and the medical conditions and
    modalities linked (`ingestion_workflow.normalization`). Its artifact is the whole record,
    its `normalized` sidecars written.

    With a model named, freshness is against the run it makes (`normalize_assign.run_for`): a new
    model, or new normalization code, makes every earlier normalization stale. The answers are kept
    in the answer store, so a rerun under new code asks the model only what it was never asked.
    Without a model, the stage only holds imports, as `record` and `evidence` do."""

    name = "normalize"
    requires = "evidence"
    #: Articles prepared ahead of the one whose answers are awaited, per request in flight.
    AHEAD = 2
    #: Articles a batch holds: the server idles while a batch's last answers finish and the next
    #: batch's first article is prepared, so batches are long.
    batch_size = 2000

    def __init__(self, settings) -> None:
        super().__init__(settings)
        self._configured: Dict[str, Run] = {}
        self._about: Optional[Dict[str, Any]] = None

    @property
    def model(self):
        return getattr(self.settings, "normalize_model", None)

    def configured_run(self, source: str) -> Optional[Run]:
        """The run the named model makes for `source`, or None when no model is named."""
        if self.model is None:
            return None
        if source not in self._configured:
            from ingestion_workflow.services import normalize_assign
            from ingestion_workflow.services.offload.export import ns_pond_commit

            if self._about is None:
                self._about = normalize_assign.describe(self.model.name)
            self._configured[source] = normalize_assign.run_for(
                source, self.model.name, ns_pond_commit(), self._about)
        return self._configured[source]

    def inputs(self, ctx, ids, upstream):
        """Each article's fresh evidence and records: normalizing a stale one is wasted."""
        records = ctx.catalog.artifacts(ids, "record")
        fresh_records = RecordStage(self.settings).fresh_sources(
            ctx, ids, records, ctx.catalog.artifacts(ids, "sync"))
        fresh_evidence = EvidenceStage(self.settings).fresh_sources(ctx, ids, upstream, records)

        def only(held, fresh):
            return {s: a for s, a in held.items() if s in fresh}

        return {article_id: {
            "evidence": only(upstream.get(article_id, {}), fresh_evidence.get(article_id, ())),
            "records": only(records.get(article_id, {}), fresh_records.get(article_id, ())),
        } for article_id in ids}

    @staticmethod
    def read_of(inputs, source) -> Optional[Artifact]:
        return normalize_input(inputs.get("evidence", {}).get(source),
                               inputs.get("records", {}).get(source))

    def expected(self, ctx, run, mine, inputs, source):
        read = self.read_of(inputs, source)
        if read is None:
            return None
        run = self.configured_run(source) or run
        return normalize_fingerprint(run, read, run.config.get("vocabulary"))

    def plan(self, ctx, refs, artifacts, upstream):
        if self.model is None:
            return super().plan(ctx, refs, artifacts, upstream)
        plan = StagePlan(stage=self.name)
        ids = [ref.id for ref in refs]
        fresh = self.fresh_sources(ctx, ids, artifacts, upstream)
        inputs = self.inputs(ctx, ids, upstream)
        attempts: Dict[str, Dict[str, Any]] = {}
        for ref in refs:
            held = inputs[ref.id]
            sources = sorted(set(held["evidence"]) | set(held["records"]))
            due = [(s, self.read_of(held, s)) for s in sources if s not in fresh.get(ref.id, ())]
            due = [(s, read) for s, read in due if read is not None]
            if not due:
                if fresh.get(ref.id):
                    plan.add_fresh(ref)
                else:
                    plan.wait(ref, NO_NORMALIZE_INPUT)
                continue
            for source, read in due:
                run = self.configured_run(source)
                fp = normalize_fingerprint(run, read, run.config.get("vocabulary"))
                existing = artifacts.get(ref.id, {}).get(source)
                if existing is not None and existing.fingerprint == fp:
                    if source not in attempts:
                        attempts[source] = ctx.catalog.attempt_counts(ids, self.name, source)
                    count, last = attempts[source].get(ref.id, (0, None))
                    if not ctx.should_attempt(existing, count, last, self.name, source):
                        plan.add_permanent(ref)
                        continue
                plan.pending.append(Work(ref=ref, source=source, fingerprint=fp, upstream=read))
        return plan

    def answer_store(self, ctx):
        from ingestion_workflow.services.normalize_assign import AnswerStore

        return AnswerStore(self.model.answers
                           or Path(ctx.catalog.root) / "task_design_answers.sqlite")

    def execute(self, ctx: Context, works: List[Work]) -> Iterator[Outcome]:
        """Each work's record renormalized and linked in `normalize_workers` forked processes, its
        tasks' prompts answered from the store or by the server (`concurrency` at once), then its
        designs and contrast identities written. A server that is down or serves another model
        stops the stage (`ModelUnavailable`): no article's failure."""
        if self.model is None or not works:
            return
        from collections import deque
        from concurrent.futures import ThreadPoolExecutor

        from ingestion_workflow.clients.model_server import ModelUnavailable
        from ingestion_workflow.normalization import api, design
        from ingestion_workflow.services import normalize_assign as na

        model = self.model
        kept: Dict[str, Run] = {}
        for work in works:
            if work.source not in kept:
                run = self.configured_run(work.source)
                # A rerun of a kept run is that run, as it was first kept.
                kept[work.source] = ctx.catalog.record_run(ctx.catalog.run_like(run) or run)
        version = self._about["version"]

        def failed(work, error):
            out = Outcome.failure(work.article_id, self.name, work.source, error,
                                  fingerprint=work.fingerprint)
            out.run_id = kept[work.source].run_id
            return out

        client = na.Client(model.url, model.name, model.timeout_s) if model.url else None
        if client is not None:
            served = client.served()
            if model.name not in served:
                raise ModelUnavailable(f"{model.url} serves {served}, not {model.name}")

        synced = ctx.catalog.artifacts(sorted({w.article_id for w in works}), "sync")
        parses: Dict[str, Optional[Path]] = {}

        def jobs():
            for work in works:
                sync = synced.get(work.article_id, {}).get("")
                if sync is None or not sync.ok:
                    yield work, None
                    continue
                parses[work.article_id] = na.parse_of(ctx.catalog.root, sync)
                yield work, (ctx.catalog.payload(work.upstream),
                             na.text_of(ctx.catalog.root, sync), work.article_id,
                             parses[work.article_id], model.name, version)

        store = self.answer_store(ctx)

        def finish(work, record, answers, asked):
            for tid, (key, future) in asked.items():
                try:
                    reason, content, usage = future.result()
                except na.RequestFailed as exc:
                    return failed(work, f"task {tid}: {exc}")
                store.put(key, model.name, reason, content, usage.get("prompt_tokens"),
                          usage.get("completion_tokens"), source=work.article_id)
                answers[tid] = na.answer_of(reason, content)
            try:
                tally = api.finish(record, answers, na.load_parse(parses.get(work.article_id)),
                                   model.name, version)
            except Exception as exc:  # noqa: BLE001 - this article's failure
                return failed(work, f"{type(exc).__name__}: {exc}")
            return Outcome(work.article_id, self.name, work.source, Status.OK,
                           fingerprint=work.fingerprint, payload=record,
                           summary={"record_digest": work.upstream.summary.get("record_digest"),
                                    "read": work.upstream.stage, "model": model.name,
                                    "version": version,
                                    "vocabulary": kept[work.source].config.get("vocabulary"),
                                    "tasks": len(answers), "asked": len(asked),
                                    **{k: n for k, n in tally.items() if n}},
                           run_id=kept[work.source].run_id)

        workers = max(1, int(getattr(self.settings, "normalize_workers", 1) or 1))
        try:
            with ThreadPoolExecutor(model.concurrency) as pool:
                waiting: deque = deque()
                for work, (prepared, error) in na.prepare_all(jobs(), workers, NO_PARSE):
                    if error is not None:
                        yield failed(work, error)
                        continue
                    record, prompts = prepared
                    answers, asked, missing = {}, {}, 0
                    for tid, prompt in prompts.items():
                        key = design.key_of(prompt, model.name)
                        held = store.get(key)
                        if held is not None:
                            answers[tid] = na.answer_of(*held)
                        elif client is not None:
                            asked[tid] = (key, pool.submit(client.ask, prompt))
                        else:
                            missing += 1
                    if missing:
                        yield failed(work, NO_SERVER.format(missing))
                        continue
                    waiting.append((work, record, answers, asked))
                    while waiting and (len(waiting) > self.AHEAD * model.concurrency or all(
                            f.done() for _, f in waiting[0][3].values())):
                        yield finish(*waiting.popleft())
                while waiting:
                    yield finish(*waiting.popleft())
        finally:
            store.close()


def parse_check(parse: Optional[Parse], text_sha256: Optional[str],
                keys: Set[str]) -> Optional[str]:
    """Why a record cannot be taken as pointing into `parse`, or None when it can."""
    if parse is None:
        return NO_PARSE
    if text_sha256 != parse.text_sha256:
        return f"text hash differs from the parse's ({text_sha256} vs {parse.text_sha256})"
    missing = sorted(keys - parse.keys)
    if missing:
        return (f"names {len(missing)} parse key(s) the parse does not have: "
                + ", ".join(missing[:5]))
    return None


__all__ = [
    "AWAITS_IMPORT",
    "EvidenceStage",
    "NO_PARSE",
    "NO_NORMALIZE_INPUT",
    "NO_SERVER",
    "NO_RECORD",
    "NormalizeStage",
    "OFFLOADED_STAGES",
    "Parse",
    "RecordStage",
    "carried_from",
    "evidence_fingerprint",
    "normalize_fingerprint",
    "normalize_input",
    "drop_siblings",
    "parse_check",
    "parse_keys",
    "record_digest",
    "record_fingerprint",
]
