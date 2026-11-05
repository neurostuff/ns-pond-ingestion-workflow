"""The stages run away from ns-pond: `record`, `evidence` and `normalize`.

Each holds, per article and extractor family (the source), what a run made on a cluster or
on beast, imported by `ingest offload import`. `record` and `evidence` never run here, so
their plans never have work. `normalize` runs in-process once `normalize_fit_dir` names a
saved fit; without one it too waits for an import.

    record      run identity + text_sha256 + the parse keys the record points at
    evidence    run identity + text_sha256 + the record's fingerprint and content, same source
    normalize   run identity + the content of the record it read + the vocabulary release

`normalize` reads the evidence record, or the record itself when evidence was skipped.

A parse change that keeps the keys a record points at leaves the record fresh.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Set

from ingestion_workflow.catalog import ArticleRef, Artifact, Outcome, Run, Status, fingerprint

from ..plan import StagePlan, Work
from ..stage import Context, recorded_path

#: Why an article has no fresh output of an offloaded stage.
AWAITS_IMPORT = "waits for an offload import"
NO_NORMALIZE_INPUT = "waits for an evidence record (or a skipped evidence run's record)"
NO_PARSE = "no coordinate parse"
NO_RECORD = "no record of this source"

OFFLOADED_STAGES = ("record", "evidence", "normalize")


def record_digest(record: Mapping[str, Any]) -> str:
    """The sha256 of a record's content: canonical JSON, without `extraction_metadata`, which
    import stamps and which says who made the record, not what it holds."""
    held = {k: v for k, v in record.items() if k != "extraction_metadata"}
    raw = json.dumps(held, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


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
    """What an offloaded stage reads of an article's coordinate parse: its text and keys."""

    def __init__(self, text_sha256: Optional[str], keys: Set[str]) -> None:
        self.text_sha256 = text_sha256
        self.keys = keys

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
                   {a["key"] for a in held.get("analyses") or [] if a.get("key")})


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


class _OffloadedStage:
    """A stage whose output is imported. Fresh per source when its fingerprint, worked out
    again from what it read, still matches; a SKIPPED one counts as done."""

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
    ) -> Dict[str, Set[str]]:
        """{article: the sources whose artifact is fresh}."""
        runs = _Runs(ctx)
        inputs = self.inputs(ctx, list(ids), upstream)
        out: Dict[str, Set[str]] = {}
        for article_id in ids:
            for source, mine in artifacts.get(article_id, {}).items():
                run = runs(mine.run_id)
                if run is None or mine.status not in (Status.OK, Status.SKIPPED):
                    continue
                if mine.status is Status.SKIPPED or (
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
                plan.fresh += 1
            else:
                plan.wait(AWAITS_IMPORT, ref.id)
        return plan

    def execute(self, ctx: Context, works: List[Work]) -> Iterator[Outcome]:
        """Nothing runs in-process yet: a plan here never has work."""
        return iter(())


def _parses(ctx: Context, ids: List[str]) -> Dict[str, Optional[Parse]]:
    synced = ctx.catalog.artifacts(ids, "sync")
    return {article_id: Parse.of(ctx.catalog.root, synced.get(article_id, {}).get(""))
            for article_id in ids}


class RecordStage(_OffloadedStage):
    """A schema-extraction record per extractor family, pointing into the article's parse."""

    name = "record"
    requires = "sync"

    def inputs(self, ctx, ids, upstream):
        return {article_id: {"parse": parse} for article_id, parse in _parses(ctx, ids).items()}

    def expected(self, ctx, run, mine, inputs, source):
        parse = inputs.get("parse")
        if parse is None:
            return None
        named = set(mine.summary.get("parse_keys") or ())
        return record_fingerprint(run, parse.text_sha256, named & parse.keys)


class EvidenceStage(_OffloadedStage):
    """The record of the same source with evidence spans in its text."""

    name = "evidence"
    requires = "record"

    def inputs(self, ctx, ids, upstream):
        return {article_id: {"parse": parse, "records": upstream.get(article_id, {})}
                for article_id, parse in _parses(ctx, ids).items()}

    def expected(self, ctx, run, mine, inputs, source):
        parse, record = inputs.get("parse"), inputs.get("records", {}).get(source)
        if parse is None or record is None or not record.ok:
            return None
        return evidence_fingerprint(run, parse.text_sha256, record)


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
    """The record's terms mapped to vocabularies, for the same source, by the fit
    `normalize_fit_dir` names (`ingestion_workflow.normalization`), one article at a time.
    Its artifact is the whole record, its `normalized` sidecars written.

    With a fit named, freshness is against the run that fit makes (`normalize_assign`): a
    new fit, or new assign code, makes every earlier normalization stale. Without one, the
    stage only holds imports, as `record` and `evidence` do."""

    name = "normalize"
    requires = "evidence"

    def __init__(self, settings) -> None:
        super().__init__(settings)
        self._configured: Dict[str, Run] = {}
        self._about: Optional[Dict[str, Any]] = None

    @property
    def fit_dir(self) -> Optional[Path]:
        held = getattr(self.settings, "normalize_fit_dir", None)
        return Path(held) if held else None

    def configured_run(self, source: str) -> Optional[Run]:
        """The run the named fit makes for `source`, or None when no fit is named."""
        if self.fit_dir is None:
            return None
        if source not in self._configured:
            from ingestion_workflow.services import normalize_assign
            from ingestion_workflow.services.offload.export import ns_pond_commit

            if self._about is None:
                self._about = normalize_assign.describe(self.fit_dir)
            self._configured[source] = normalize_assign.run_for(
                source, self.fit_dir, ns_pond_commit(), self._about)
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
        if self.fit_dir is None:
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
                    plan.fresh += 1
                else:
                    plan.wait(NO_NORMALIZE_INPUT, ref.id)
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
                        plan.permanent += 1
                        continue
                plan.pending.append(Work(ref=ref, source=source, fingerprint=fp, upstream=read))
        return plan

    def execute(self, ctx: Context, works: List[Work]) -> Iterator[Outcome]:
        """Each work's input assigned against the fit, `normalize_workers` at once: forked
        processes, which share the fit once it is loaded here."""
        if self.fit_dir is None or not works:
            return
        from ingestion_workflow.services import normalize_assign as na

        synced = ctx.catalog.artifacts(sorted({w.article_id for w in works}), "sync")
        kept: Dict[str, Run] = {}
        for work in works:
            if work.source not in kept:
                run = self.configured_run(work.source)
                # A rerun of a kept run is that run, as it was first kept.
                kept[work.source] = ctx.catalog.record_run(ctx.catalog.run_like(run) or run)

        def jobs():
            for work in works:
                sync = synced.get(work.article_id, {}).get("")
                if sync is None or not sync.ok:
                    yield work, None
                    continue
                yield work, (ctx.catalog.payload(work.upstream),
                             na.text_of(ctx.catalog.root, sync),
                             na.bibliography_of(ctx.catalog, work.article_id,
                                                na.text_source(ctx.catalog.root, sync)),
                             str(self.fit_dir), work.article_id)

        workers = max(1, int(getattr(self.settings, "normalize_workers", 1) or 1))
        for work, (normalized, error) in na.assign_all(jobs(), workers, NO_PARSE):
            run = kept[work.source]
            if error is not None:
                failed = Outcome.failure(work.article_id, self.name, work.source, error,
                                         fingerprint=work.fingerprint)
                failed.run_id = run.run_id
                yield failed
                continue
            yield Outcome(work.article_id, self.name, work.source, Status.OK,
                          fingerprint=work.fingerprint, payload=normalized,
                          summary={"record_digest": work.upstream.summary.get("record_digest"),
                                   "read": work.upstream.stage, "fit": run.fit,
                                   "vocabulary": run.config.get("vocabulary")},
                          run_id=run.run_id)


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
    "NO_RECORD",
    "NormalizeStage",
    "OFFLOADED_STAGES",
    "Parse",
    "RecordStage",
    "evidence_fingerprint",
    "normalize_fingerprint",
    "normalize_input",
    "parse_check",
    "parse_keys",
    "record_digest",
    "record_fingerprint",
]
