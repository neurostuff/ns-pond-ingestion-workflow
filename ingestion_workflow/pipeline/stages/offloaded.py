"""The stages run away from ns-pond: `record`, `evidence` and `normalize`.

Each holds, per article and extractor family (the source), what a run made on a cluster or
on beast, imported by `ingest offload import`. No stage here calls a model yet, so a plan
never has work: what is missing or stale waits for an import.

    record      run identity + text_sha256 + the parse keys the record points at
    evidence    run identity + text_sha256 + the record's fingerprint and content, same source
    normalize   run identity + the record's content + the vocabulary release

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

    def plan(
        self,
        ctx: Context,
        refs: Sequence[ArticleRef],
        artifacts: Dict[str, Dict[str, Artifact]],
        upstream: Dict[str, Dict[str, Artifact]],
    ) -> StagePlan:
        plan = StagePlan(stage=self.name)
        runs = _Runs(ctx)
        inputs = self.inputs(ctx, [ref.id for ref in refs], upstream)
        for ref in refs:
            held = artifacts.get(ref.id, {})
            fresh = False
            for source, mine in held.items():
                run = runs(mine.run_id)
                if run is None or mine.status not in (Status.OK, Status.SKIPPED):
                    continue
                if mine.status is Status.SKIPPED:
                    fresh = True
                elif self.expected(ctx, run, mine, inputs.get(ref.id, {}), source) == \
                        mine.fingerprint and ctx.is_fresh(mine, mine.fingerprint):
                    fresh = True
            if fresh:
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


class NormalizeStage(_OffloadedStage):
    """The record's terms mapped to vocabularies, for the same source."""

    name = "normalize"
    requires = "record"

    def inputs(self, ctx, ids, upstream):
        return {article_id: {"records": upstream.get(article_id, {})} for article_id in ids}

    def expected(self, ctx, run, mine, inputs, source):
        record = inputs.get("records", {}).get(source)
        if record is None or not record.ok:
            return None
        return normalize_fingerprint(run, record, run.config.get("vocabulary"))


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
    "NO_RECORD",
    "NormalizeStage",
    "OFFLOADED_STAGES",
    "Parse",
    "RecordStage",
    "evidence_fingerprint",
    "normalize_fingerprint",
    "parse_check",
    "parse_keys",
    "record_digest",
    "record_fingerprint",
]
