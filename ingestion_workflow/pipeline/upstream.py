"""One rule for what is made from an article's readings when one changes, fails or goes.

A parse is built from two readings of the article, its tables (`analyses`) and its prose
(`passages`, `prose`), through the stages after them. `decide` says whether what was made
from them goes on, waits, or is taken back:

    PROCEED     every reading is fresh and was read whole
    HOLD        a reading, or a stage after it, is stale, failed, cut short or not known;
                waiting costs nothing, and a failure says nothing about the article
    TAKE_BACK   the tables and the prose were both read whole and hold nothing, so what
                an earlier reading made is no longer the article's

`resolve` asks it before taking back a resolve, and `upload` before it sends a parse
(`Assessor`, which adds what each later stage's own plan says of its output). The reasons
both record are the ones below and `stage.TAKE_BACK_REASONS`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from ingestion_workflow.catalog import ArticleRef, Artifact, Status

from . import exclusions as excl
from .plan import StagePlan
from .stage import NO_ANALYSES, Context, is_take_back

#: Why the tables are not known: they were not read, or not read whole.
TABLES_NOT_READ = "tables not read"
#: Why the prose is not known.
PROSE_NOT_READ = "prose not read"
#: What a held-back upload's error starts with.
GATED = "held back: upstream not fresh"

#: The stages a parse is built from, in pipeline order; the prose ones run only with prose.
GATE_STAGES = ("analyses", "passages", "prose", "resolve", "roles", "space", "sync")
#: The readings a person can leave out of an upload as a whole, with an exclusion on the
#: table id `source:<name>` (`ingest exclude <article> -t source:tables --reason ...`),
#: and the stages each covers. The stages after them are never excused.
SOURCE_STAGES = {"tables": ("analyses",), "prose": ("passages", "prose")}
SOURCE_MARK = "source:"

#: A table left unread for one of these is the same numbers as a table that was read.
_UNREAD_AS_NOTHING = ("repeats another table's numbers",)


class Verdict(str, Enum):
    PROCEED = "proceed"
    HOLD = "hold"
    TAKE_BACK = "take back"


class Kind(str, Enum):
    #: Read whole; holds analyses.
    HOLDS = "holds"
    #: Read whole; holds none.
    EMPTY = "empty"
    #: Not read, not read whole, or failed: says nothing about the article.
    UNKNOWN = "unknown"
    #: Not known, but a person left it out of the upload with a reason.
    EXCUSED = "excused"


@dataclass(frozen=True)
class Reading:
    kind: Kind
    why: Optional[str] = None


def short_reason(text: Optional[str], limit: int = 200) -> str:
    text = " ".join((text or "no reason recorded").split())
    return text if len(text) <= limit else text[: limit - 3] + "..."


def tables_reading(ctx: Context, article_id: str, analyses: Optional[Artifact]) -> Reading:
    """What the article's tables are known to hold.

    Empty: the analyses stage found no coordinate table (SKIPPED), or read every table
    and none held an analysis; with no analyses run, triage passed no table or the
    extraction holds none. Unknown: a failed run, a table whose raw content is missing,
    an analyses stage or triage not yet run.
    """
    if analyses is not None:
        summary = analyses.summary or {}
        unread = {k: v for k, v in (summary.get("unread") or {}).items()
                  if v not in _UNREAD_AS_NOTHING}
        empty = (analyses.status is Status.SKIPPED and not summary.get("tables")) or (
            analyses.status is Status.OK and not unread and not summary.get("tables"))
        if empty and _passed_since(ctx, article_id, analyses):
            # A triage run since passed tables this reading never saw (emcznvwqpaor: skipped
            # for "no coordinate tables" on 09-30, triage passed two on 10-15).
            return Reading(Kind.UNKNOWN, f"{TABLES_NOT_READ}: triage passed tables since "
                                         "the analyses stage last ran")
        if analyses.status is Status.SKIPPED and not summary.get("tables"):
            return Reading(Kind.EMPTY)
        if analyses.status is Status.OK and not unread:
            return Reading(Kind.HOLDS if summary.get("tables") else Kind.EMPTY)
        why = analyses.error if analyses.status is Status.FAILED else (
            next(iter(unread.values()), None) or analyses.status.value)
        return Reading(Kind.UNKNOWN, f"{TABLES_NOT_READ}: {short_reason(why)}")
    triaged = ctx.catalog.artifacts([article_id], "triage").get(article_id, {}).get("")
    if triaged is not None and triaged.status is Status.OK:
        verdicts = ctx.payload(triaged) or {}
        if not any(v.get("passes") for v in verdicts.get("tables", [])):
            return Reading(Kind.EMPTY)
        return Reading(Kind.UNKNOWN, f"{TABLES_NOT_READ}: the analyses stage has not run")
    extracted = ctx.catalog.artifacts([article_id], "extract").get(article_id, {})
    if extracted and all(e.status is Status.OK and not (e.summary or {}).get("tables")
                         for e in extracted.values()):
        return Reading(Kind.EMPTY)
    return Reading(Kind.UNKNOWN, f"{TABLES_NOT_READ}: triage has not run")


def _passed_since(ctx: Context, article_id: str, analyses: Artifact) -> bool:
    """Whether the article's triage, run after `analyses`, passes a table."""
    triaged = ctx.catalog.artifacts([article_id], "triage").get(article_id, {}).get("")
    if triaged is None or triaged.status is not Status.OK or triaged.updated_at <= analyses.updated_at:
        return False
    return any(v.get("passes") for v in (ctx.payload(triaged) or {}).get("tables", []))


def prose_reading(prose: Optional[Artifact]) -> Reading:
    """What the article's prose is known to hold: an OK reading is a whole one."""
    if prose is None:
        return Reading(Kind.UNKNOWN, f"{PROSE_NOT_READ}: the prose stage has not run")
    if prose.status is not Status.OK:
        why = short_reason(prose.error or prose.status.value)
        return Reading(Kind.UNKNOWN, f"{PROSE_NOT_READ}: {why}")
    summary = prose.summary or {}
    return Reading(Kind.HOLDS if summary.get("coordinates") or summary.get("analyses")
                   else Kind.EMPTY)


def decide(tables: Reading, prose: Optional[Reading],
           behind: Mapping[str, str] = ()) -> Tuple[Verdict, Optional[str]]:
    """The verdict on what was made from these readings, and why when it is not PROCEED.

    `prose` is None for a run without prose. `behind` is {stage: why} for the stages
    whose own plans say their output is not fresh; a reading's stage there says it for
    the reading.
    """
    behind = dict(behind)
    unknown = [reading.why for reading, stages in ((tables, SOURCE_STAGES["tables"]),
                                                   (prose, SOURCE_STAGES["prose"]))
               if reading is not None and reading.kind is Kind.UNKNOWN
               and not set(stages) & set(behind)]
    unknown += [f"{name} {state}" for name, state in behind.items()]
    if unknown:
        return Verdict.HOLD, "; ".join(unknown)
    if tables.kind is Kind.EMPTY and (prose is None or prose.kind is Kind.EMPTY):
        return Verdict.TAKE_BACK, NO_ANALYSES
    return Verdict.PROCEED, None


def upstream_state(plan: StagePlan, existing: Optional[Artifact]) -> Optional[str]:
    """Why a stage's output for one article is not fresh, from that stage's own plan of it.

    None when it is fresh, or when the stage has nothing for the article (it skipped it).
    """
    if plan.fresh or plan.skipped:
        return None
    if plan.pending:
        if is_take_back(plan.pending[0]):
            return "taken back"
        if existing is None:
            return "not run"
        if existing.status is Status.OK:
            return "stale"
        return f"failed: {short_reason(existing.error)}"
    if plan.permanent:
        if existing is not None and existing.status is Status.SKIPPED:
            return None
        # FAILED here is waiting out its retry backoff, or has used up its tries.
        if existing is not None and existing.status is Status.FAILED:
            return f"failed: {short_reason(existing.error)}"
        return f"failed, not retried: {short_reason(existing.error if existing else None)}"
    reasons = ", ".join(plan.reasons)
    return "blocked" + (f" ({reasons})" if reasons else "")


def excused(stage: str, marks: Mapping[str, Mapping[str, str]]) -> Optional[str]:
    """The recorded reason a person left this stage's reading out of the upload, if any."""
    for key in [excl.WHOLE_ARTICLE] + [
            SOURCE_MARK + source for source, stages in SOURCE_STAGES.items() if stage in stages]:
        reason = (marks.get(key) or {}).get("reason")
        if reason:
            return reason
    return None


@dataclass
class Assessment:
    """What `decide` says of one article, with each stage's part."""

    verdict: Verdict
    reason: Optional[str]
    #: {stage: why} for what is not fresh and nobody excused.
    behind: Dict[str, str] = field(default_factory=dict)
    #: {stage: the reason a person gave} for what is not fresh and was excused.
    excused: Dict[str, str] = field(default_factory=dict)


class Assessor:
    """Assesses articles for a stage that sends what the parse was built from."""

    def __init__(self, settings) -> None:
        self.settings = settings
        self._chain: Optional[List[Tuple[str, Any]]] = None

    @property
    def prose(self) -> bool:
        return bool(getattr(self.settings, "prose_model", None))

    def stages(self) -> List[Tuple[str, Any]]:
        """(name, stage) for the stages the parse is built from, as this run builds them."""
        if self._chain is None:
            from .stages import PROSE_STAGES, STAGE_TYPES

            self._chain = [(name, STAGE_TYPES[name](self.settings)) for name in GATE_STAGES
                           if self.prose or name not in PROSE_STAGES]
        return self._chain

    def assess(self, ctx: Context, refs: Sequence[ArticleRef]) -> Dict[str, Assessment]:
        ids = [ref.id for ref in refs]
        marks = ctx.catalog.exclusions(ids)
        found_by_stage: Dict[str, Dict[str, Dict[str, Artifact]]] = {}
        behind: Dict[str, Dict[str, str]] = {ref.id: {} for ref in refs}
        for name, stage in self.stages():
            found = found_by_stage[name] = ctx.catalog.artifacts(ids, name)
            parents = ctx.catalog.artifacts(ids, stage.requires) if stage.requires else {}
            # Sync's claims on shared base studies are one query over the whole catalog:
            # asked once for every article here, not once per article's plan.
            shared = stage.contended(ctx, refs, parents) if name == "sync" else None
            for ref in refs:
                plan = (stage.plan(ctx, [ref], found, parents) if shared is None
                        else stage.plan_contended(ctx, [ref], found, parents, shared))
                state = upstream_state(plan, found.get(ref.id, {}).get(""))
                if state:
                    behind[ref.id][name] = state
        if "prose" not in found_by_stage:
            found_by_stage["prose"] = {}
        out = {}
        for ref in refs:
            mine = marks.get(ref.id, {})
            analyses = found_by_stage["analyses"].get(ref.id, {}).get("")
            tables = tables_reading(ctx, ref.id, analyses)
            prose = (prose_reading(found_by_stage["prose"].get(ref.id, {}).get(""))
                     if self.prose else None)
            late = behind[ref.id]
            if late.get("analyses") == "not run":
                del late["analyses"]  # the tables reading says whether there was anything to read
            given = {name: why for name in late if (why := excused(name, mine))}
            if tables.kind is Kind.UNKNOWN and (why := excused("analyses", mine)):
                tables, given["analyses"] = Reading(Kind.EXCUSED, tables.why), why
            left = {name: state for name, state in late.items() if name not in given}
            verdict, reason = decide(tables, prose, left)
            for name, reading in (("analyses", tables), ("prose", prose)):
                if reading is not None and reading.kind is Kind.UNKNOWN:
                    left.setdefault(name, reading.why)
            out[ref.id] = Assessment(verdict, reason, left, given)
        return out


__all__ = [
    "Assessment",
    "Assessor",
    "GATED",
    "GATE_STAGES",
    "Kind",
    "PROSE_NOT_READ",
    "Reading",
    "SOURCE_MARK",
    "SOURCE_STAGES",
    "TABLES_NOT_READ",
    "Verdict",
    "decide",
    "short_reason",
    "excused",
    "prose_reading",
    "tables_reading",
    "upstream_state",
]
