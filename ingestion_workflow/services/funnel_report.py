"""Where every article stands at every stage, counted from the catalog.

Each live article gets exactly one state per stage, so a stage's states always
add up to the catalog's article count. The state comes from the article's own
artifact when it has one, and otherwise from its input:

passed        ok, and the next stage takes it as input
filtered      recorded, but holding nothing the next stage takes (no tables, ...)
failed        a failure the scheduler retries
permanent     a failure it does not retry
taken_back    what was made from the article's text was withdrawn: every extraction
              found no text, or the tables and the prose were read again and hold
              no analysis
refused       its input passed, but this stage will not read it: the text was
              made from a downloaded page that is not the article
blocked       never ran here because its input did not pass; the reason names
              the stage where the article dropped out and why. A failed or empty
              record left here from an earlier run counts as blocked too, and its
              reason adds that record's own status and reason ("leftover ...")
pending       its input passed and nothing is recorded here yet; the reason says
              when the input holds nothing for this stage to read
not_attempted its input passed and this stage has never run on the catalog
rerun         recorded, and the stage's plan would make it again: why, its
              freshness test says (no fingerprint, its inputs changed, ...)
held          its input passed, but the stage's plan does not take it yet, and says
              why (no text hash, its model not served, ...)
spent         failed, and its plan will not try it again on its own: its tries are
              used up, or no source is left; `--refresh <stage>` redoes it
stale         an ok artifact exists although its input did not pass, so it was
              made from an input that has since gone or under an older stage graph
unaccounted   an attempt is recorded but no outcome, or a status the catalog does
              not define

Given the pipeline's settings, each stage's own `plan()` decides, as `ingest run
--dry-run` does, whether the stage takes the article, whether its record is fresh,
and whether a failure is tried again: the report keeps no second copy of those rules.
The records say what happened and why, from small summary fields read through
`json_extract`. Articles are streamed in id order and planned 500 at a time, so
memory does not grow with the catalog.
"""

from __future__ import annotations

import json
import re
import sqlite3
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence, Tuple
from urllib.parse import parse_qs, urlparse

from ingestion_workflow.catalog import ArticleRef, Artifact, Catalog
from ingestion_workflow.catalog.store import BACKING_OFF, STATUS_RANK
from ingestion_workflow.pipeline import plan as planning
from ingestion_workflow.pipeline.scheduler import BATCH_SIZE, plan_batch
from ingestion_workflow.pipeline.stage import (
    CHANGED,
    FRESH,
    INPUT_BACK,
    NO_ANALYSES,
    NO_FINGERPRINT,
    NO_TEXT,
    NOT_OK,
    PAYLOAD_GONE,
    REFRESHING,
    REMADE,
    TAKE_BACK_REASONS,
    TRANSIENT_DUE,
    Context,
)
from ingestion_workflow.pipeline.stages.extract import READ_FROM_BAD_PAGE, SYMBOLS_AS_DIGITS
from ingestion_workflow.pipeline.stages.sync import (
    NO_IDENTIFIER,
    RETRACTION_NOTICE,
    SHARED_BASE_STUDY,
)
from ingestion_workflow.services.neurostore_ingest import Result

PASSED = "passed"
FILTERED = "filtered"
FAILED = "failed"
PERMANENT = "permanent"
TAKEN_BACK = "taken_back"
REFUSED = "refused"
BLOCKED = "blocked"
PENDING = "pending"
NOT_ATTEMPTED = "not_attempted"
STALE = "stale"
UNACCOUNTED = "unaccounted"
RERUN = "rerun"
HELD = "held"
SPENT = "spent"

STATES = (PASSED, FILTERED, FAILED, SPENT, PERMANENT, TAKEN_BACK, REFUSED, BLOCKED, PENDING,
          NOT_ATTEMPTED, RERUN, HELD, STALE, UNACCOUNTED)

#: What each state means, in plain words, for the page.
MEANINGS = {
    PASSED: "done, and handed on to the next stage",
    FILTERED: "done, but it holds nothing the next stage reads",
    FAILED: "failed; the pipeline will try again",
    SPENT: "failed, and the pipeline will not try again on its own: no tries or no source "
           "left; `--refresh <stage>` redoes it",
    RERUN: "made before, and the pipeline will make it again",
    HELD: "its input passed, but the stage does not take it yet",
    PERMANENT: "failed in a way the pipeline does not retry",
    TAKEN_BACK: "what was made from its text was withdrawn: no extraction found any text, "
                "or a re-read found no analyses",
    REFUSED: "its input passed, but that text came from a downloaded page that is not the article",
    BLOCKED: "never reached this stage: it dropped out at an earlier one",
    PENDING: "ready for this stage, which has not run on it yet",
    NOT_ATTEMPTED: "ready, but this stage has never run on the catalog",
    STALE: "a record made from an input that has since gone or changed",
    UNACCOUNTED: "an attempt with no outcome, or a status the catalog does not know: a bug",
}

#: Of the articles a stage on the upload path takes in, those it leaves out and
#: those still waiting; the rest passed. The flow diagram sums these.
EXCLUDED = (FILTERED, FAILED, SPENT, PERMANENT, TAKEN_BACK, REFUSED, UNACCOUNTED)
WAITING = (PENDING, NOT_ATTEMPTED, RERUN, HELD)

#: The reason upload holds an article back on (`GATED` in the upload stage):
#: "<this>: <stage> <state>; ...; the parse holds N analyses, the last upload M
#: (K missing)". The funnel keeps the stages in the reason and the counts in the
#: detail, so articles group by what they wait on.
HELD_BACK = "held back: upstream not fresh"

#: Plain words for common reasons, matched as a substring, first match wins.
GLOSSARY: Tuple[Tuple[str, str], ...] = (
    ("leftover", "a failed or empty record left from an earlier run; nothing downstream "
                 "reads it"),
    ("tables wait behind it", "the article has coordinate tables, which reach resolve only "
                              "after this free step runs"),
    ("nothing to read (passages = 0)", "no passage to send to the model; running it costs "
                                       "nothing and records an empty answer"),
    (READ_FROM_BAD_PAGE, "the text it would read came from a page that is not the article"),
    (HELD_BACK, "upload waits: a stage this parse is built from is stale or failed, so it may "
                "hold fewer analyses than the last upload; the detail names the stage and how "
                "many are missing"),
    ("bad page: paywall", "the download was a paywall or login page, not the article"),
    ("bad page: error_page", "the download was an error page (not found, server error)"),
    ("bad page: empty_shell", "the download held almost no visible text (a viewer shell)"),
    ("bad page: citation_stub", "the download was a citation or record page with no article "
                                "text"),
    ("not open access", "the source will not serve the full text"),
    ("triage passed no table", "triage read the tables and judged none to hold coordinates"),
    ("the extraction has no table", "the extracted text has no table to triage"),
    ("no coordinate tables", "no table holds coordinates"),
    ("neither the tables nor the prose", "nothing for resolve to combine"),
    ("never run on the catalog", "this stage has not run on any article yet"),
    (NO_TEXT, "no extractor found text in the download"),
    (NO_ANALYSES, "the tables and the prose were read again and hold no analysis, so what an "
                  "earlier reading made was withdrawn; neurostore keeps the last upload"),
    (RETRACTION_NOTICE, "the paper is a retraction notice, not a study"),
    (NO_IDENTIFIER, "neurostore holds no base study for it, and it has no doi, pmid or pmcid "
                    "to create one by"),
    (SHARED_BASE_STUDY, "another catalog article that reaches sync names the same neurostore "
                        "base study, so uploading either would replace the other's analyses; "
                        "the detail names the base study and the other article"),
    ("no analyses to upload", "never uploaded, and its parse holds no analysis"),
    ("ingest refresh-text", "older extractions record no text hash; `ingest refresh-text` "
                            "records it"),
    ("is not_attempted", "made before its input stage was required, and that stage has "
                         "never run"),
    ("is stale", "its input is itself a record left from before"),
    ("is pending", "its input waits to run again, so this record predates it"),
    ("is filtered", "its input now holds nothing for it"),
    ("is refused", "its input came from a page that is not the article"),
    ("is failed", "its input has failed since"),
    ("is blocked", "its input dropped out at an earlier stage"),
    # The upload's failures, by `_upload_reason`.
    ("refused by neurostore", "neurostore refused the parse and would again for the same "
                              "body; the code it gave, if any, follows"),
    ("neurostore failed every try", "neurostore was up but failed this parse on every try "
                                    "(5xx, 429, a timeout, or a 2xx that is not its summary)"),
    ("invalid parse", "the parse failed the checks made before sending it"),
    ("parse files missing", "sync recorded a parse whose files are not on disk"),
)

#: A stage that runs per source keeps one artifact per source; the article's
#: state is the best any source reached (`STATUS_RANK`, as `Catalog.status_counts`).
_RANK = {status: rank for rank, status in enumerate(STATUS_RANK)}

#: Inputs a stage's fingerprint reads besides its declared parent
#: (`fingerprint_for` in each stage), for the trace's "built from".
_SIDE_INPUTS = {"triage": ("extract",), "analyses": ("extract", "metadata"),
                "prose": ("metadata",), "resolve": ("analyses",),
                "sync": ("metadata", "triage")}

#: Which part of a reason joined from several sources ("x + y") names the
#: article's loss, first match wins: our own errors, which a fix here clears;
#: then ones a retry may clear; then the source refusing; then the source not
#: having the article. The flow groups reasons by it and lists the joins under it.
_PRIMARY = ("bad page", "code error", "node too old for readabilipy", "database error",
            "ACE scrape invalid", "ACE could not identify the source", "PDF conversion failed",
            "XML parse error", "model answer unreadable", "file or payload missing",
            "some passages failed", "rate limited", "timeout", "connection error", "HTTP 5xx",
            "HTTP 403 forbidden", "HTTP 4xx other", "HTTP 404 not found", "not a PDF",
            "not open access", "source returned no article", "no metadata found")
_ORIGIN_PREFIX = re.compile(r"^((?:[a-z_]+ (?:%s): )*)" % "|".join(STATES))


def reason_group(reason: str) -> str:
    """The reason a joined reason is grouped under: its origin prefix ("download
    failed: ") and its primary part, without a leftover record's note."""
    main = reason.partition("; leftover ")[0]
    prefix = _ORIGIN_PREFIX.match(main).group(1)
    parts = main[len(prefix):].split(" + ")
    if len(parts) == 1:
        return main

    def rank(part: str) -> int:
        return next((i for i, p in enumerate(_PRIMARY) if part.startswith(p)), len(_PRIMARY))

    return prefix + min(parts, key=rank)

#: A stage whose input can pass while holding nothing for it to read, and the
#: count on the input's summary that says so. Prose answers an article with no
#: passage without a model call (`ProseStage.plan`), so such an article is
#: pending for free, unlike one waiting on the model.
_EMPTY_INPUT = {"prose": "passages"}

#: Free-text errors folded into a few categories; the raw text stays for the
#: drill-down. First match wins, so the specific patterns come first.
_CATEGORIES: Tuple[Tuple[str, str], ...] = (
    (r"not open access|does not allow full-text|no open-access pdf", "not open access"),
    (r"did not produce output|did not return a payload|returned no article",
     "source returned no article"),
    (r"is not a pdf", "not a PDF"),
    (r"rate limit|\b429\b", "rate limited"),
    (r"\b403\b|forbidden", "HTTP 403 forbidden"),
    (r"\b404\b|not found", "HTTP 404 not found"),
    (r"\b5\d\d\b (client|server) error|server error", "HTTP 5xx"),
    (r"\b4\d\d client error", "HTTP 4xx other"),
    (r"timed out|timeout", "timeout"),
    (r"readabilipy|node >=", "node too old for readabilipy"),
    (r"missing on disk|is gone", "file or payload missing"),
    (r"unreadableanswer|no json object", "model answer unreadable"),
    (r"has no attribute|attributeerror|typeerror|keyerror|nameerror|indexerror",
     "code error"),
    (r"line \d+, column \d+|document is empty|xmlsyntaxerror", "XML parse error"),
    (r"psycopg|programlimitexceeded|sqlalchemy|integrityerror", "database error"),
    (r"passages failed", "some passages failed"),
    (r"no provider returned metadata", "no metadata found"),
    (r"could not identify an article source", "ACE could not identify the source"),
    (r"ace validation failed|ace scrape (did not produce|produced invalid)",
     "ACE scrape invalid"),
    (r"docling", "PDF conversion failed"),
    (r"no outcome", "no outcome returned"),
    (r"connection|max retries|reset by peer|remotedisconnected", "connection error"),
)
_COMPILED = tuple((re.compile(p, re.I), c) for p, c in _CATEGORIES)


#: Anything in an error that looks like a credential: a key=value secret, a
#: signed URL's parameters, or a long opaque token.
_SECRETS = (
    (re.compile(r"(?i)\b(api[_-]?key|access[_-]?token|token|secret|password|bearer|"
                r"authorization|sig|signature)(\s*[=:]\s*|\s+)[^\s&,;'\"]+"), r"\1\2<redacted>"),
    (re.compile(r"\b[A-Za-z0-9_\-]{32,}\b"), "<redacted>"),
)


def scrub(text: str, limit: int = 500) -> str:
    """Raw error text fit to show: anything token-like removed, cut to `limit`."""
    for pattern, replacement in _SECRETS:
        text = pattern.sub(replacement, text)
    return text[:limit]


_UNMATCHED = re.compile(r"other: [^+;]*?(?= ?(?:[+;]|$))")


def plain_reason(reason: str) -> str:
    """A reason in words: an error no category matched says only that the stage
    raised one, and its text stays in the technical detail."""
    return _UNMATCHED.sub("the stage raised an error", reason).rstrip()


def categorize(error: Optional[str]) -> str:
    """The category of a recorded error; unmatched text keeps its own shape."""
    if not error:
        return "no reason recorded"
    if error.startswith(HELD_BACK):
        return error.split("; the parse holds", 1)[0]
    for pattern, category in _COMPILED:
        if pattern.search(error):
            return category
    text = re.sub(r"https?://\S+", "<url>", scrub(error.strip()))
    text = re.sub(r"\d+", "N", text)
    return "other: " + text[:70]


@dataclass(frozen=True)
class StageNode:
    name: str
    upstream: Optional[str]
    #: Summary key the downstream stage reads on this stage's artifact.
    flag: Optional[str] = None


def stage_graph(prose: bool) -> List[StageNode]:
    """The stages in order, each with the input it waits for, as the stages declare it."""
    from ingestion_workflow.pipeline.stages import (
        PROSE_STAGES,
        STAGE_ORDER,
        STAGE_TYPES,
        upstream_of,
    )

    settings = SimpleNamespace(prose_model="on" if prose else None)
    names = [n for n in STAGE_ORDER if prose or n not in PROSE_STAGES]
    upstream: Dict[str, Optional[str]] = {}
    flags: Dict[str, str] = {}
    for name in names:
        kind = STAGE_TYPES[name]
        up, flag = upstream_of(kind, settings)
        upstream[name] = up
        if up and flag:
            flags.setdefault(up, flag)
    if "resolve" in upstream and "analyses" in upstream:
        # Resolve reads the tables' analyses alongside its declared parent.
        flags.setdefault("analyses", "tables")
    return [StageNode(n, upstream[n], flags.get(n)) for n in names]


def trunk(graph: Sequence[StageNode]) -> List[str]:
    """The chain from the last stage back to the first: the path an upload takes."""
    by_name = {node.name: node for node in graph}
    chain = [graph[-1].name]
    while by_name[chain[-1]].upstream:
        chain.append(by_name[chain[-1]].upstream)
    return chain[::-1]


@dataclass
class _Row:
    stage: str
    source: str
    status: str
    error: Optional[str]
    passed: object
    tables: object
    reason: Optional[str]
    text_source: Optional[str]
    coordinates: object
    analyses: object
    retracted: object
    bad_page: Optional[str]
    passages: object
    hashed: object
    #: An extraction set aside for what its file is (`extract.unusable`).
    set_aside: object


@dataclass
class Placed:
    """One article's state at one stage."""

    state: str
    reason: str = ""
    raw: str = ""
    #: Whether the article has an artifact at this stage.
    recorded: bool = False
    #: The reason a stage downstream inherits, when it differs from `reason`.
    carry: str = ""


def _truthy(value: object) -> bool:
    return value not in (None, 0, "0", "", False, "false")


#: The outcomes the upload records as failures, by the first word of its error
#: ("<result>: [<CODES>: ]<detail>"), and the category each is counted under.
_UPLOAD_RESULTS = {
    Result.REFUSED.value: "refused by neurostore",
    Result.BLOCKED.value: "neurostore failed every try",
    Result.INVALID.value: "invalid parse",
    Result.MISSING.value: "parse files missing",
}
_UPLOAD_CODES = re.compile(r"[A-Z][A-Z_]+(?:, [A-Z][A-Z_]+)*(?=: )")


def _upload_reason(error: str) -> Optional[str]:
    """An upload failure's category, with neurostore's codes when it gave any."""
    result, sep, detail = error.partition(": ")
    if not sep or result not in _UPLOAD_RESULTS:
        return None
    codes = _UPLOAD_CODES.match(detail)
    return _UPLOAD_RESULTS[result] + (f": {codes.group(0)}" if codes else "")


def _failure_reason(row: _Row) -> str:
    if row.bad_page:
        return f"bad page: {row.bad_page}"
    if row.stage == "upload" and row.error and (found := _upload_reason(row.error)):
        return found
    return categorize(row.error)


def _own_state(node: StageNode, rows: List[_Row]) -> Placed:
    """The state an article's own artifacts at a stage give it."""
    best = min(rows, key=lambda r: _RANK.get(r.status, 9))
    if best.status not in _RANK:
        return Placed(UNACCOUNTED, f"unknown status {best.status!r}", best.error or "")
    if best.status == "ok":
        if node.flag and not _truthy(getattr(best, node.flag, None)):
            if best.reason:
                return Placed(FILTERED, best.reason)
            if node.name == "triage":
                if best.tables in (0, "0"):
                    return Placed(FILTERED, "the extraction has no table")
                return Placed(FILTERED, "triage passed no table")
            return Placed(FILTERED, f"its summary has {node.flag} = 0")
        return Placed(PASSED, "retracted" if _truthy(best.retracted) else "")
    if best.status == "skipped":
        return Placed(FILTERED,
                      best.reason or best.error or "skipped: the source cannot handle it",
                      scrub(best.error or "", 300))
    failures = [r for r in rows if r.status != "ok" and r.error]
    if failures and all(r.error in TAKE_BACK_REASONS for r in failures):
        return Placed(TAKEN_BACK, " + ".join(sorted({r.error for r in failures})))
    reason = " + ".join(sorted({_failure_reason(r) for r in failures})) or "no reason recorded"
    raw = "; ".join((f"{r.source}: " if r.source else "") + scrub(r.error or "", 200)
                    for r in failures)
    return Placed(FAILED if best.status == "failed" else PERMANENT, reason, raw[:600])


def _origin(stage: str, placed: Placed) -> Placed:
    """What a stage downstream of `placed` reports as the reason it was blocked."""
    if placed.state == BLOCKED:
        return Placed(BLOCKED, placed.carry or placed.reason, placed.raw)
    why = f"{stage} {placed.state}" + (f": {placed.reason}" if placed.reason else "")
    return Placed(BLOCKED, why, placed.raw)


@dataclass(frozen=True)
class Verdict:
    """What a stage's own plan decided for one article (`plan_verdicts`)."""

    #: `StagePlan.by_article`'s verdict; None when the stage could not plan.
    plan: Optional[str]
    #: The plan's reason for blocking it, or why the stage could not plan.
    reason: Optional[str] = None
    #: What the plan's freshness and retry tests answered of the article's records.
    answers: Tuple[str, ...] = ()


class _Watching(Context):
    """A run's context that notes what its freshness and retry tests answer of the
    records of the stage being planned, per article."""

    def __init__(self, base: Context) -> None:
        super().__init__(base.settings, base.catalog, refresh=sorted(base.refresh),
                         max_attempts=base.max_attempts, retry_after=base.retry_after,
                         now=base.now)
        self.stage: Optional[str] = None
        self.answers: Dict[str, List[str]] = defaultdict(list)

    def freshness(self, artifact: Optional[Artifact], expected: str) -> str:
        found = super().freshness(artifact, expected)
        if artifact is not None and artifact.stage == self.stage and found != NOT_OK:
            self.answers[artifact.article_id].append(found)
        return found

    def attempt_state(self, artifact: Optional[Artifact], attempts: int,
                      last_attempt: Optional[str], stage: str, source: str = "") -> str:
        found = super().attempt_state(artifact, attempts, last_attempt, stage, source)
        if artifact is not None and stage == self.stage:
            self.answers[artifact.article_id].append(found)
        return found


def plan_verdicts(ctx: Context, stages: Sequence[Any],
                  refs: Sequence[ArticleRef]) -> Dict[str, Dict[str, Verdict]]:
    """article_id -> stage -> what that stage's plan decides for it, planned as
    `ingest run --dry-run` plans, read-only."""
    watching = _Watching(ctx)
    out: Dict[str, Dict[str, Verdict]] = defaultdict(dict)
    for stage in stages:
        watching.stage, watching.answers = stage.name, defaultdict(list)
        try:
            plan = plan_batch(watching, stage, list(refs))
        except Exception as exc:  # noqa: BLE001 - one stage that cannot plan hides no other
            why = _cannot_plan(exc)
            for ref in refs:
                out[ref.id][stage.name] = Verdict(None, why)
            continue
        for article_id, (verdict, reason) in plan.by_article().items():
            out[article_id][stage.name] = Verdict(
                verdict, reason, tuple(dict.fromkeys(watching.answers.get(article_id, ()))))
    return out


def _cannot_plan(exc: Exception) -> str:
    """Why a stage's plan could not run, in words: the setting it asked for, if it named one."""
    named = re.search(r"\bneeds ([a-z_]+)", str(exc))
    if named:
        return f"not known: this stage needs {named.group(1)} in the config to plan"
    return f"not known: this stage cannot plan with these settings ({type(exc).__name__})"


#: What the plan's freshness test answers that says why a record is made again.
_REMADE_WHY = (NO_FINGERPRINT, CHANGED, PAYLOAD_GONE, TRANSIENT_DUE, REFRESHING, INPUT_BACK)


def _overlay(placed: Placed, verdict: Verdict) -> Placed:
    """An article's recorded state at a stage the plan takes, with what the plan will
    do to it: a record it makes again waits; a failure it will not retry is spent."""
    if verdict.plan == planning.PENDING:
        if placed.state in (FAILED, UNACCOUNTED):
            return placed  # due a retry, or a bug either way
        why = next((a for a in verdict.answers if a in _REMADE_WHY), REMADE)
        return Placed(RERUN, why, placed.raw)
    if placed.state == FAILED and BACKING_OFF not in verdict.answers:
        return Placed(SPENT, placed.reason, placed.raw)
    return placed


def place_article(graph: Sequence[StageNode], rows: Sequence[_Row], *, ran: set,
                  attempted: set,
                  verdicts: Optional[Dict[str, Verdict]] = None) -> Dict[str, Placed]:
    """Every stage's state for one article. `attempted` holds the stages with an
    attempt but no artifact; `ran` the stages with any artifact in the catalog;
    `verdicts`, each stage's plan for the article (`plan_verdicts`). Without them, or
    for a stage that could not plan, a stage takes the article when its input passed,
    and nothing is said of what a run would do: the records alone."""
    by_stage: Dict[str, List[_Row]] = defaultdict(list)
    for row in rows:
        by_stage[row.stage].append(row)
    # An extraction set aside for what its file is names why the article has no text.
    set_aside = any(_truthy(r.set_aside) for r in by_stage.get("extract", ()))
    out: Dict[str, Placed] = {}
    for node in graph:
        own = by_stage.get(node.name)
        before = out[node.upstream] if node.upstream else None
        verdict = verdicts.get(node.name) if verdicts is not None else None
        if verdicts is not None and verdict is None:
            out[node.name] = Placed(UNACCOUNTED, "the stage's plan did not place it",
                                    recorded=bool(own))
            continue
        if verdict is not None and verdict.plan is None:
            verdict = None  # this stage could not plan: its records alone
        if verdict is not None:
            entered = verdict.plan != planning.BLOCKED
        else:
            # A record made again later still passed: the records alone take it.
            entered = before is None or before.state in (PASSED, RERUN)
        if not entered:
            if verdict is not None and (before is None or before.state in (PASSED, RERUN)):
                # The input passed, and the plan holds the article for a reason of its own.
                why = verdict.reason or "the stage does not take it yet"
                out[node.name] = Placed(REFUSED if READ_FROM_BAD_PAGE in why else HELD, why,
                                        recorded=bool(own))
            elif own:
                out[node.name] = _recorded(node, own, before)
            else:
                out[node.name] = _origin(node.upstream, before)
        elif own:
            placed = _own_state(node, own)
            out[node.name] = _overlay(placed, verdict) if verdict is not None else placed
            out[node.name].recorded = True
        elif node.name in attempted:
            out[node.name] = Placed(UNACCOUNTED, "an attempt is recorded but no outcome")
        elif verdict is not None and verdict.plan == planning.SKIPPED:
            out[node.name] = Placed(FILTERED,
                                    verdict.reason or f"{node.name} has nothing to do for it")
        elif node.name not in ran:
            out[node.name] = Placed(NOT_ATTEMPTED, "this stage has never run on the catalog")
        elif before is not None and before.state not in (PASSED, RERUN):
            # Its plan takes what its input did not pass on: a run records why.
            out[node.name] = Placed(PENDING, f"taken by its plan although {node.upstream} is "
                                             f"{before.state}: {before.reason or 'no reason'}")
        else:
            out[node.name] = Placed(PENDING, _pending_reason(node, by_stage))
        if set_aside and out[node.name].reason == NO_TEXT:
            out[node.name].reason = SYMBOLS_AS_DIGITS
    return out


def _recorded(node: StageNode, own: List[_Row], before: Placed) -> Placed:
    """The state of an article with an artifact at a stage that does not take it."""
    if all(r.status != "ok" and r.error in TAKE_BACK_REASONS for r in own):
        # The record a stage leaves when its input was taken back: the
        # article dropped out upstream, so it counts as blocked from there.
        placed = _origin(node.upstream, before)
    else:
        mine = _own_state(node, own)
        why = f"input {node.upstream} is {before.state}"
        if mine.state == PASSED:
            placed = Placed(STALE, why + (f"; {mine.reason}" if mine.reason else ""),
                            mine.raw or before.reason)
        elif mine.state == UNACCOUNTED:
            placed = mine
        else:
            # Nothing downstream reads a failed or empty record, so the article
            # dropped out upstream; the record's own reason stays beside it.
            origin = _origin(node.upstream, before)
            status = min(own, key=lambda r: _RANK.get(r.status, 9)).status
            placed = Placed(BLOCKED, f"{origin.reason}; leftover {status}: {mine.reason}",
                            mine.raw or origin.raw, carry=origin.reason)
    placed.recorded = True
    return placed


def _pending_reason(node: StageNode, by_stage: Dict[str, List[_Row]]) -> str:
    key = _EMPTY_INPUT.get(node.name)
    given = [r for r in by_stage.get(node.upstream or "", []) if r.status == "ok"]
    if key and given and getattr(given[0], key) in (0, "0"):
        tables = [r for r in by_stage.get("analyses", []) if r.status == "ok"]
        if tables and _truthy(tables[0].tables):
            # Resolve passes the tables on only once this stage has recorded.
            return f"input ready, nothing to read ({key} = 0); tables wait behind it"
        return f"input ready, nothing to read ({key} = 0)"
    return "input ready, not run yet"


def _dimensions(rows: Sequence[_Row]) -> Tuple[str, str, str]:
    """(sources with an ok download, source of the text triage or passages judged,
    sources with any download recorded, failed ones included)."""
    downloaded = sorted({r.source for r in rows if r.stage == "download" and r.status == "ok"})
    tried = sorted({r.source for r in rows if r.stage == "download"})
    judged = ([r.text_source for r in rows if r.stage == "triage" and r.text_source]
              or [r.text_source for r in rows if r.stage == "passages" and r.text_source])
    return ("+".join(downloaded) or "none", (judged[0] if judged else "none"),
            "+".join(tried) or "none")


def _row_reason(row: _Row) -> str:
    """The reason on one per-source download or extraction row."""
    if row.status == "ok":
        return SYMBOLS_AS_DIGITS if _truthy(row.set_aside) else ""
    if row.status == "skipped" and not row.bad_page:
        return row.reason or categorize(row.error)
    return _failure_reason(row)


_ROW_COLUMNS = """
SELECT a.article_id, a.stage, a.source, a.status, substr(a.error, 1, 400),
       json_extract(a.summary, '$.passed'), json_extract(a.summary, '$.tables'),
       json_extract(a.summary, '$.reason'), json_extract(a.summary, '$.source'),
       json_extract(a.summary, '$.coordinates'), json_extract(a.summary, '$.analyses'),
       json_extract(a.summary, '$.retracted'), json_extract(a.summary, '$.bad_page'),
       json_extract(a.summary, '$.passages'), json_extract(a.summary, '$.text_sha256') IS NOT NULL,
       json_extract(a.summary, '$.symbols_as_digits') IS NOT NULL
FROM artifacts a"""
_ROWS_SQL = _ROW_COLUMNS + " ORDER BY a.article_id"

_ATTEMPTED_SQL = """
SELECT DISTINCT t.article_id, t.stage FROM attempts t
WHERE NOT EXISTS (SELECT 1 FROM artifacts a
                  WHERE a.article_id = t.article_id AND a.stage = t.stage)"""


#: Articles with an artifact per stage, counted apart from the placement, for
#: `Funnel.reconcile` to check every record was placed.
_RECORDED_SQL = """
SELECT a.stage, COUNT(DISTINCT a.article_id) FROM artifacts a
JOIN articles r ON r.id = a.article_id WHERE r.merged_into IS NULL GROUP BY a.stage
"""


#: Per stage and field, the ok artifacts of live articles noting an error of it (`Notes`),
#: how many of those errors are transient, and how many lack it (`missing`).
_NOTED_SQL = """
SELECT a.stage, e.key, COUNT(*),
       SUM(EXISTS (SELECT 1 FROM json_each(a.summary, '$.transient') t WHERE t.value = e.key)), 0
FROM artifacts a JOIN articles r ON r.id = a.article_id, json_each(a.summary, '$.errors') e
WHERE r.merged_into IS NULL AND a.status = 'ok' GROUP BY a.stage, e.key
UNION ALL
SELECT a.stage, m.value, 0, 0, COUNT(*)
FROM artifacts a JOIN articles r ON r.id = a.article_id, json_each(a.summary, '$.missing') m
WHERE r.merged_into IS NULL AND a.status = 'ok' GROUP BY a.stage, m.value
"""


def noted(conn: sqlite3.Connection) -> Dict[str, Dict[str, List[int]]]:
    """stage -> field -> [errors noted, of them transient, missing], for the kept records."""
    out: Dict[str, Dict[str, List[int]]] = defaultdict(dict)
    for stage, name, errors, transient, missing in conn.execute(_NOTED_SQL):
        counts = out[stage].setdefault(name, [0, 0, 0])
        counts[0] += errors
        counts[1] += transient
        counts[2] += missing
    return dict(out)


def open_readonly(catalog_root: Path | str) -> sqlite3.Connection:
    """The catalog opened read-only, so a report can never write to it."""
    path = Path(catalog_root).resolve() / "catalog.sqlite"
    if not path.exists():
        raise FileNotFoundError(f"no catalog at {path}")
    return sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)


def _articles(conn: sqlite3.Connection) -> Iterator[Tuple[str, str]]:
    yield from conn.execute(
        "SELECT id, substr(created_at, 1, 10) FROM articles WHERE merged_into IS NULL ORDER BY id")


def _artifact_groups(conn: sqlite3.Connection) -> Iterator[Tuple[str, List[_Row]]]:
    current, group = None, []
    for r in conn.execute(_ROWS_SQL):
        if r[0] != current:
            if current is not None:
                yield current, group
            current, group = r[0], []
        group.append(_Row(*r[1:]))
    if current is not None:
        yield current, group


@dataclass
class Funnel:
    """The counts, keyed by every dimension the page filters on."""

    graph: List[StageNode]
    total: int = 0
    #: (stage, state, reason, downloaded, judged, day, tried) -> [count, [(article_id, raw)]]
    cells: Dict[tuple, list] = field(default_factory=dict)
    #: (stage, source, status, reason, downloaded, judged, day, tried)
    #: -> [count, [(article_id, raw)]]
    sources: Dict[tuple, list] = field(default_factory=dict)
    #: Artifacts whose article is not live (merged away or never registered).
    stranded: int = 0
    #: stage -> articles placed from their own artifact, and the catalog's own count.
    recorded: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    expected: Dict[str, int] = field(default_factory=dict)
    collected: List[Tuple[str, str, str, str]] = field(default_factory=list)
    #: `noted`: what the kept records lack, and the errors that left it out.
    noted: Dict[str, Dict[str, List[int]]] = field(default_factory=dict)
    seconds: float = 0.0
    samples: int = 0
    #: Whether each stage's own plan placed the articles (`Planner`), or the records alone.
    planned: bool = False
    #: stage -> why its plan could not run, for the articles it then placed by records.
    unplanned: Dict[str, str] = field(default_factory=dict)

    def stage_totals(self) -> Dict[str, Dict[str, int]]:
        out: Dict[str, Dict[str, int]] = {n.name: {s: 0 for s in STATES} for n in self.graph}
        for (stage, state, *_), (count, _) in self.cells.items():
            out[stage][state] += count
        return out

    def reconcile(self) -> List[str]:
        """What does not add up: empty when every stage accounts for every article,
        and placed every article that has a record there from that record."""
        problems = []
        totals = self.stage_totals()
        for stage, counts in totals.items():
            if sum(counts.values()) != self.total:
                problems.append(f"{stage}: states sum to {sum(counts.values()):,}, "
                                f"not {self.total:,}")
            if counts[UNACCOUNTED]:
                problems.append(f"{stage}: {counts[UNACCOUNTED]:,} unaccounted")
            placed, held = self.recorded.get(stage, 0), self.expected.get(stage, 0)
            if placed != held:
                problems.append(f"{stage}: {placed:,} articles placed from a record, "
                                f"but {held:,} have one")
        if self.stranded:
            problems.append(f"{self.stranded:,} artifacts belong to no live article")
        problems.extend(f"{stage}: placed by its records alone, as its plan could not run: {why}"
                        for stage, why in sorted(self.unplanned.items()))
        return problems


def flow_stages(graph: Sequence[StageNode]) -> List[str]:
    """Every stage that gates articles, in run order: all but those no stage waits for
    (a stage's `gates`)."""
    from ingestion_workflow.pipeline.stages import STAGE_TYPES

    return [node.name for node in graph if getattr(STAGE_TYPES[node.name], "gates", True)]


def _grouped(pairs: Dict[Tuple[str, str], int], states: Sequence[str]) -> List[list]:
    """[state, group, count, [[reason, count], ...]] for the given states, largest first."""
    groups: Dict[Tuple[str, str], Dict[str, int]] = defaultdict(dict)
    for (state, reason), n in pairs.items():
        if state in states:
            groups[(state, reason_group(reason))][reason] = n
    out = [[state, group, sum(members.values()),
            sorted(([r, n] for r, n in members.items()), key=lambda m: (-m[1], m[0]))]
           for (state, group), members in groups.items()]
    return sorted(out, key=lambda g: (-g[2], g[0], g[1]))


def page_model(funnel: Funnel) -> Dict[str, Any]:
    """Every number the page shows, counted once: per stage its state totals and its
    reasons grouped, and the flow. The CLI prints the flow and the page draws all of
    it; the page recounts in the browser only for a filter."""
    by: Dict[str, Dict[Tuple[str, str], int]] = defaultdict(lambda: defaultdict(int))
    for (stage, state, reason, *_), (count, _) in funnel.cells.items():
        by[stage][(state, reason)] += count
    totals = {node.name: {state: sum(n for (s, _), n in by[node.name].items() if s == state)
                          for state in STATES} for node in funnel.graph}
    upstream = {node.name: node.upstream for node in funnel.graph}
    steps = []
    for stage in flow_stages(funnel.graph):
        count = totals[stage]
        came = funnel.total - count[BLOCKED] - count[STALE]
        excluded, waiting = _grouped(by[stage], EXCLUDED), _grouped(by[stage], WAITING)
        steps.append({
            "stage": stage, "input": upstream[stage], "in": came, "passed": count[PASSED],
            "excluded": excluded, "waiting": waiting, "stale": _grouped(by[stage], (STALE,)),
            "adds_up": came == count[PASSED] + sum(g[2] for g in excluded + waiting),
        })
    return {"total": funnel.total, "totals": totals, "flow": steps,
            "reasons": {node.name: _grouped(by[node.name], STATES) for node in funnel.graph}}


def flow(funnel: Funnel) -> List[Dict[str, Any]]:
    """The flow along the upload path, each reason listed on its own: `page_model`'s
    steps for the trunk stages."""
    chain = trunk(funnel.graph)
    out = []
    for step in page_model(funnel)["flow"]:
        if step["stage"] not in chain:
            continue

        def flat(groups: List[list]) -> List[list]:
            return sorted(([g[0], r, n] for g in groups for r, n in g[3]),
                          key=lambda item: (-item[2], item[0], item[1]))

        out.append({**step, "excluded": flat(step["excluded"]), "waiting": flat(step["waiting"]),
                    "stale": sum(g[2] for g in step["stale"])})
    return out


def article_filter(*, sources: Sequence[str] = (), tried: Sequence[str] = (),
                   judged: Sequence[str] = (), day_from: Optional[str] = None,
                   day_to: Optional[str] = None) -> Callable[[str, str, str, str], bool]:
    """The page's article filters, as one test on (downloaded, judged, day, tried):
    each list matches any of its values, and the days bound the registration day."""
    def ok(dl: str, txt: str, day: str, tr: str) -> bool:
        return ((not sources or any(s in dl.split("+") for s in sources))
                and (not tried or any(s in tr.split("+") for s in tried))
                and (not judged or txt in judged)
                and (not day_from or day >= day_from) and (not day_to or day <= day_to))
    return ok


def filter_funnel(funnel: Funnel, **filters) -> Funnel:
    """The funnel of only the articles `article_filter(**filters)` keeps."""
    ok = article_filter(**filters)
    out = Funnel(graph=funnel.graph, samples=funnel.samples)
    out.cells = {k: v for k, v in funnel.cells.items() if ok(k[3], k[4], k[5], k[6])}
    out.sources = {k: v for k, v in funnel.sources.items() if ok(k[4], k[5], k[6], k[7])}
    first = funnel.graph[0].name
    out.total = sum(n for k, (n, _) in out.cells.items() if k[0] == first)
    return out


def _add(table: Dict[tuple, list], key: tuple, article_id: str, raw: str, samples: int) -> None:
    cell = table.get(key)
    if cell is None:
        cell = table[key] = [0, []]
    cell[0] += 1
    if len(cell[1]) < samples:
        cell[1].append((article_id, raw))


class Planner:
    """Each stage's own plan, as `ingest run --dry-run` makes it under `settings`, over
    the catalog at `root` opened read-only."""

    def __init__(self, root: Path | str, settings, graph: Sequence[StageNode]) -> None:
        from ingestion_workflow.pipeline.stages import build

        self._conn = open_readonly(root)
        self._conn.row_factory = sqlite3.Row
        self.catalog = Catalog(self._conn, Path(root).resolve())
        self.ctx = Context.from_settings(settings, self.catalog)
        #: (stage, or why it cannot be built under these settings), in graph order.
        self.stages: List[Tuple[str, Any]] = []
        for node in graph:
            try:
                self.stages.append((node.name, build([node.name], settings)[0]))
            except ValueError as exc:
                self.stages.append((node.name, _cannot_plan(exc)))

    def verdicts(self, article_ids: Sequence[str]) -> Dict[str, Dict[str, Verdict]]:
        """article_id -> stage -> what that stage's plan decides for it."""
        found = self.catalog.identifiers(list(article_ids))
        refs = [ArticleRef(id=a, identifier=found[a]) for a in article_ids]
        out = plan_verdicts(self.ctx, [s for _, s in self.stages if not isinstance(s, str)],
                            refs)
        for name, stage in self.stages:
            if isinstance(stage, str):
                for ref in refs:
                    out[ref.id][name] = Verdict(None, stage)
        return out

    def close(self) -> None:
        self._conn.close()


def build_funnel(conn: sqlite3.Connection, *, planner: Optional[Planner] = None,
                 prose: Optional[bool] = None, samples: int = 5,
                 collect: Optional[Callable[[str, tuple], bool]] = None,
                 ) -> Funnel:
    """Count every article's state at every stage.

    With a `planner`, each stage's own plan decides whether it takes each article and
    what a run would do with it (`place_article`); without, the records alone.
    `prose` defaults to whether the catalog holds any prose-stage artifact.
    `collect(kind, key)` picks articles to list in full, beyond the `samples`
    kept per cell: kind "stage" with a `Funnel.cells` key, or "source" with a
    `Funnel.sources` key.
    """
    started = time.monotonic()
    ran = {r[0] for r in conn.execute("SELECT DISTINCT stage FROM artifacts")}
    if prose is None:
        prose = bool(ran & {"passages", "prose", "resolve"})
    graph = stage_graph(prose)
    funnel = Funnel(graph=graph, samples=samples, planned=planner is not None)
    names = {node.name for node in graph}
    funnel.expected = {stage: n for stage, n in conn.execute(_RECORDED_SQL) if stage in names}
    funnel.noted = noted(conn)
    attempted: Dict[str, set] = defaultdict(set)
    for article_id, stage in conn.execute(_ATTEMPTED_SQL):
        attempted[article_id].add(stage)

    def place(batch: List[Tuple[str, str, List[_Row]]]) -> None:
        verdicts = planner.verdicts([a for a, _, _ in batch]) if planner is not None else {}
        for by_stage in verdicts.values():
            for stage, verdict in by_stage.items():
                if verdict.plan is None:
                    funnel.unplanned.setdefault(stage, verdict.reason or "")
        for article_id, day, rows in batch:
            funnel.total += 1
            downloaded, judged, tried = _dimensions(rows)
            placed = place_article(graph, rows, ran=ran,
                                   attempted=attempted.get(article_id, set()),
                                   verdicts=verdicts.get(article_id, {}) if planner else None)
            for stage, where in placed.items():
                key = (stage, where.state, where.reason, downloaded, judged, day, tried)
                _add(funnel.cells, key, article_id, where.raw, samples)
                if where.recorded:
                    funnel.recorded[stage] += 1
                if collect and collect("stage", key):
                    funnel.collected.append((article_id, stage, where.state, where.reason))
            for row in rows:
                if row.stage in ("download", "extract"):
                    status = TAKEN_BACK if row.error == NO_TEXT else row.status
                    reason = _row_reason(row)
                    key = (row.stage, row.source, status, reason, downloaded, judged, day, tried)
                    _add(funnel.sources, key, article_id, scrub(row.error or "", 300), samples)
                    if collect and collect("source", key):
                        funnel.collected.append(
                            (article_id, f"{row.stage}:{row.source}", status, reason))

    groups = _artifact_groups(conn)
    pending_group = next(groups, None)
    batch: List[Tuple[str, str, List[_Row]]] = []
    for article_id, day in _articles(conn):
        while pending_group is not None and pending_group[0] < article_id:
            funnel.stranded += len(pending_group[1])
            pending_group = next(groups, None)
        rows: List[_Row] = []
        if pending_group is not None and pending_group[0] == article_id:
            rows = pending_group[1]
            pending_group = next(groups, None)
        batch.append((article_id, day, rows))
        if len(batch) >= BATCH_SIZE:  # planned together, as a run plans them
            place(batch)
            batch = []
    place(batch)
    while pending_group is not None:
        funnel.stranded += len(pending_group[1])
        pending_group = next(groups, None)
    funnel.seconds = time.monotonic() - started
    return funnel


# -- the page ------------------------------------------------------------------


def _encode(funnel: Funnel, catalog: str) -> dict:
    """The funnel as compact JSON: strings are listed once and cells index them."""
    tables: Dict[str, Dict[str, int]] = defaultdict(dict)

    def ix(kind: str, value: str) -> int:
        table = tables[kind]
        if value not in table:
            table[value] = len(table)
        return table[value]

    def cell_rows(source: Dict[tuple, list], kinds: Sequence[str]) -> list:
        """[dimension indexes..., count, samples, tried index]: `tried`, the last
        key, goes after the samples."""
        out = []
        for key, (count, sample) in source.items():
            dims = [ix(k, v) for k, v in zip(kinds, key)]
            out.append(dims + [count, [[ix("id", a), ix("raw", raw)] for a, raw in sample],
                               ix("tried", key[len(kinds)])])
        return out

    for state in STATES:
        ix("state", state)
    for node in funnel.graph:
        ix("stage", node.name)
    cells = cell_rows(funnel.cells, ("stage", "state", "reason", "dl", "text", "day"))
    sources = cell_rows(funnel.sources,
                        ("stage", "source", "status", "reason", "dl", "text", "day"))
    out = {
        "catalog": catalog,
        "generated": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "seconds": round(funnel.seconds, 1),
        "total": funnel.total,
        "samples": funnel.samples,
        "stages": [{"name": n.name, "upstream": n.upstream, "flag": n.flag} for n in funnel.graph],
        "prose": any(n.name == "prose" for n in funnel.graph),
        "model": page_model(funnel),
        "noted": funnel.noted,
        "flow_stages": flow_stages(funnel.graph),
        "side_inputs": _SIDE_INPUTS,
        "meanings": MEANINGS,
        "glossary": GLOSSARY,
        "excluded": EXCLUDED,
        "waiting": WAITING,
        "trunk": trunk(funnel.graph),
        "problems": funnel.reconcile(),
        "planned": funnel.planned,
        "stranded": funnel.stranded,
        "strings": {kind: list(table) for kind, table in tables.items()},
        "cells": cells,
        "sources": sources,
    }
    out["groups"] = [reason_group(r) for r in out["strings"].get("reason", [])]
    return out


def render_html(funnel: Funnel, catalog: str) -> str:
    """One self-contained page: the data as JSON and the code inline, so it opens offline."""
    data = json.dumps(_encode(funnel, catalog), separators=(",", ":"))
    # A "</" inside the JSON would end the script element early.
    return _PAGE.replace("__DATA__", data.replace("</", "<\\/"))


def funnel_of(catalog_root: Path | str, settings=None, *, prose: Optional[bool] = None,
              samples: int = 20, collect: Optional[Callable[[str, tuple], bool]] = None,
              ) -> Funnel:
    """The funnel of the catalog at `catalog_root`, opened read-only. With the pipeline's
    `settings`, each stage's own plan places the articles, and the stage graph is the one
    those settings run (`prose_model`)."""
    if settings is not None and prose is None:
        prose = bool(getattr(settings, "prose_model", None))
    conn = open_readonly(catalog_root)
    planner = None
    try:
        if settings is not None:
            planner = Planner(catalog_root, settings, stage_graph(prose))
        return build_funnel(conn, planner=planner, prose=prose, samples=samples,
                            collect=collect)
    finally:
        conn.close()
        if planner is not None:
            planner.close()


def write_report(catalog_root: Path | str, out: Path | str, *, samples: int = 20,
                 prose: Optional[bool] = None, settings=None) -> Funnel:
    funnel = funnel_of(catalog_root, settings, prose=prose, samples=samples)
    Path(out).write_text(render_html(funnel, str(catalog_root)), encoding="utf-8")
    return funnel


def matching_ids(catalog_root: Path | str, *, stage: str, states: Sequence[str] = (),
                 reason: Optional[str] = None, exact_reason: Optional[str] = None,
                 sources: Sequence[str] = (), tried: Sequence[str] = (),
                 judged: Sequence[str] = (), day_from: Optional[str] = None,
                 day_to: Optional[str] = None, prose: Optional[bool] = None,
                 by_source: Optional[str] = None, group: Optional[str] = None,
                 settings=None) -> List[Tuple[str, str, str, str]]:
    """Every article in one bucket, chosen as the page chooses it:
    (article_id, stage, state, reason).

    `reason` matches as a case-insensitive substring, as the reason box does,
    and `exact_reason` exactly, as a clicked reason does. `sources` (an ok
    download from), `tried` (any download recorded from) and `judged` each match
    any of their values; `day_from`/`day_to` bound the registration day. With
    `by_source`, the rows are that source's own download or extraction rows at
    `stage`, and `states` matches their status (ok, failed, permanent, skipped,
    taken_back), as the page's source table does. `group` matches a reason's
    `reason_group`, as a clicked group in the flow does."""
    needle = (reason or "").lower()
    article_ok = article_filter(sources=sources, tried=tried, judged=judged,
                                day_from=day_from, day_to=day_to)

    def wanted(kind: str, key: tuple) -> bool:
        if (kind == "source") != (by_source is not None) or key[0] != stage:
            return False
        if by_source is not None:
            _, src, status, rea, dl, txt, day, tr = key
            if src != by_source:
                return False
        else:
            _, status, rea, dl, txt, day, tr = key
        return ((not states or status in states)
                and (exact_reason is None or rea == exact_reason) and needle in rea.lower()
                and (group is None or reason_group(rea) == group)
                and article_ok(dl, txt, day, tr))

    return funnel_of(catalog_root, settings, prose=prose, samples=0, collect=wanted).collected


def _versions(stage: str) -> Dict[str, Any]:
    """The version constants of the stage's code as it is now."""
    from ingestion_workflow.pipeline.stages import STAGE_TYPES

    module = sys.modules[STAGE_TYPES[stage].__module__]
    return {k: v for k, v in sorted(vars(module).items())
            if k.isupper() and k.endswith("VERSION") and isinstance(v, (str, int))}


def _said(verdict: Optional[Verdict]) -> str:
    """What a stage's plan decides for the article, in words."""
    if verdict is None:
        return ""
    if verdict.plan is None:
        return verdict.reason or ""
    words = {planning.FRESH: "fresh: nothing to do",
             planning.PENDING: "would run now",
             planning.BLOCKED: "does not take it",
             planning.SKIPPED: "nothing to do for it",
             planning.PERMANENT: "not tried again now"}[verdict.plan]
    why = [verdict.reason] if verdict.reason else []
    why += [a for a in verdict.answers if a != FRESH]
    return words + (f": {'; '.join(why)}" if why else "")


def trace_article(catalog_root: Path | str, article_id: str, *, settings=None,
                  prose: Optional[bool] = None) -> Dict[str, Any]:
    """How one article got where it is: per stage its state (the same placement the
    funnel counts), and every artifact's path, status, reason, summary, when it was
    written, its fingerprint and attempts, and what it was built from. With the
    pipeline's `settings`, each stage's own plan places it and says what a run would
    do, as the funnel's `Planner` does."""
    conn = open_readonly(catalog_root)
    try:
        found = conn.execute("SELECT created_at, merged_into FROM articles WHERE id = ?",
                             (article_id,)).fetchone()
        if found is None:
            return {"article_id": article_id, "error": "no such article in the catalog"}
        if found[1]:
            return {"article_id": article_id, "merged_into": found[1],
                    "error": f"merged into {found[1]}"}
        ran = {r[0] for r in conn.execute("SELECT DISTINCT stage FROM artifacts")}
        if prose is None:
            prose = bool(ran & {"passages", "prose", "resolve"})
        graph = stage_graph(prose)
        rows = [_Row(*r[1:]) for r in conn.execute(
            _ROW_COLUMNS + " WHERE a.article_id = ?", (article_id,))]
        attempted = {r[1] for r in conn.execute(_ATTEMPTED_SQL + " AND t.article_id = ?",
                                                (article_id,))}
        verdicts = None
        if settings is not None:
            planner = Planner(catalog_root, settings, graph)
            try:
                verdicts = planner.verdicts([article_id])[article_id]
            finally:
                planner.close()
        placed = place_article(graph, rows, ran=ran, attempted=attempted, verdicts=verdicts)
        by_key = {(r.stage, r.source): r for r in rows}
        records: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for stage, source, status, fp, blob, summary, error, written in conn.execute(
                "SELECT stage, source, status, fingerprint, blob, summary, error, updated_at "
                "FROM artifacts WHERE article_id = ? ORDER BY stage, source", (article_id,)):
            path = Path(catalog_root).resolve() / "blobs" / blob[:2] / f"{blob}.json.gz" \
                if blob else None
            records[stage].append({
                "source": source, "status": status,
                # Upload's held-back reason is already in words, and its counts matter.
                "reason": error if (error or "").startswith(HELD_BACK)
                else plain_reason(_row_reason(by_key[(stage, source)])),
                "error_detail": scrub(error or ""),
                "summary": json.loads(summary), "fingerprint": fp, "written": written,
                "path": str(path) if path else None,
                "on_disk": path.exists() if path else None,
            })
        tries: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for stage, source, n, failed, last in conn.execute(
                "SELECT stage, source, COUNT(*), SUM(1 - ok), MAX(attempted_at) FROM attempts "
                "WHERE article_id = ? GROUP BY stage, source", (article_id,)):
            tries[(stage, source)] = {"attempts": n, "failed": failed, "last": last}
    finally:
        conn.close()
    stages = []
    for node in graph:
        where = placed[node.name]
        mine = records.get(node.name, [])
        for rec in mine:
            rec.update(tries.get((node.name, rec["source"]), {"attempts": 0}))
        inputs = [node.upstream] if node.upstream else []
        inputs += [i for i in _SIDE_INPUTS.get(node.name, ()) if i not in inputs]
        upstream = [(i, u) for i in inputs for u in records.get(i, [])]
        stages.append({
            "stage": node.name, "input": node.upstream, "state": where.state,
            "reason": plain_reason(where.reason), "meaning": MEANINGS[where.state],
            "records": mine,
            "built_from": [{"stage": i, "source": u["source"],
                            "status": u["status"], "fingerprint": u["fingerprint"],
                            "written": u["written"],
                            "newer": bool(mine) and u["written"] > max(r["written"] for r in mine)}
                           for i, u in upstream],
            "version": _versions(node.name),
            "plan": _said(verdicts.get(node.name)) if verdicts is not None
            else "pass the pipeline's --config to see what a run would do",
        })
    return {"article_id": article_id, "registered": found[0], "stages": stages,
            "attempts_without_outcome": sorted(attempted)}


def serve(catalog_root: Path | str, *, host: str = "127.0.0.1", port: int = 8765,
          samples: int = 20, prose: Optional[bool] = None, settings=None) -> ThreadingHTTPServer:
    """The page, plus each article's trace on click, served from this machine. The
    caller runs `serve_forever()`."""
    funnel = funnel_of(catalog_root, settings, prose=prose, samples=samples)
    page = render_html(funnel, str(catalog_root)).encode("utf-8")
    graph_prose = any(n.name == "prose" for n in funnel.graph)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            url = urlparse(self.path)
            if url.path in ("/", "/index.html"):
                self._send(200, "text/html; charset=utf-8", page)
            elif url.path == "/trace":
                article = (parse_qs(url.query).get("id") or [""])[0]
                body = trace_article(catalog_root, article, settings=settings,
                                     prose=graph_prose)
                self._send(200, "application/json", json.dumps(body).encode("utf-8"))
            else:
                self._send(404, "text/plain", b"not found")

        def _send(self, code: int, kind: str, body: bytes) -> None:
            self.send_response(code)
            self.send_header("Content-Type", kind)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args) -> None:
            pass

    server = ThreadingHTTPServer((host, port), Handler)
    server.funnel = funnel  # type: ignore[attr-defined]
    return server


_PAGE = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Ingestion funnel</title>
<style>
:root{--fg:#1d2330;--muted:#5b6475;--line:#d9dde5;--bg:#fff;--panel:#f6f7f9;--accent:#2f5bd3}
body{font:13px/1.4 system-ui,-apple-system,Segoe UI,sans-serif;color:var(--fg);background:var(--bg);margin:0;padding:16px 24px}
h1{font-size:20px;margin:0 0 4px}h2{font-size:15px;margin:22px 0 8px}
.muted{color:var(--muted)}.warn{color:#a1261b;font-weight:600}
#filters{display:flex;flex-wrap:wrap;gap:14px 28px;background:var(--panel);padding:10px 14px;border-radius:6px;position:sticky;top:0;z-index:2;border:1px solid var(--line)}
#filters .grp{display:flex;flex-direction:column;gap:4px}
#filters label.chip{display:inline-flex;align-items:center;gap:3px;margin-right:8px;white-space:nowrap}
table{border-collapse:collapse;font-variant-numeric:tabular-nums}
th,td{padding:3px 8px;border-bottom:1px solid var(--line);text-align:right;white-space:nowrap}
th:first-child,td:first-child{text-align:left}
td.click{cursor:pointer}td.click:hover{background:#e8eefc}
td.ok{color:#1b7f3b}td.bad{color:#a1261b;font-weight:600}
.bar{display:flex;height:12px;width:260px;background:#eee}
.bar span{display:block;height:100%}
.legend span{display:inline-flex;align-items:center;margin-right:12px}
.legend i{display:inline-block;width:10px;height:10px;margin-right:4px}
#drill table td{text-align:left;max-width:520px;overflow:hidden;text-overflow:ellipsis}
code{background:var(--panel);padding:2px 4px;border-radius:3px}
svg text{font-size:11px;fill:var(--fg)}
button{font:inherit;padding:3px 10px}
.tabs button.on{background:var(--accent);color:#fff;border-color:var(--accent)}
.flow{max-width:1100px}
.step{display:flex;flex-wrap:wrap;gap:10px;align-items:stretch}
.box{border:1px solid var(--line);border-radius:6px;padding:8px 12px;background:var(--bg)}
.box.main{flex:0 1 260px;min-width:220px;border-left:5px solid #3b4a66;cursor:pointer}
.box.main:hover,.box li:hover{background:#e8eefc}
.box.excl{flex:1 1 320px;background:var(--panel)}
.box .big{font-size:20px;font-weight:600}
.box ul{list-style:none;margin:4px 0;padding:0}
.box li{display:flex;flex-wrap:wrap;gap:0 8px;padding:2px 0;cursor:pointer;border-bottom:1px dotted var(--line)}
.box li .n{margin-left:auto;font-variant-numeric:tabular-nums;font-weight:600}
.box li .mean{flex-basis:100%;color:var(--muted);font-size:12px}
.box li.sub{display:block;cursor:default}.box li.sub ul{margin-left:12px}
span.click{cursor:pointer;text-decoration:underline dotted}
.arrow{margin:4px 0 4px 40px;color:#2e9d57;font-weight:600}
.ok{color:#1b7f3b}.bad{color:#a1261b;font-weight:600}
#trace table td{text-align:left;white-space:normal;vertical-align:top;max-width:none}
#trace pre.detail{white-space:pre-wrap;font-size:11px;margin:2px 0;max-width:60ch}
#trace .path{font-family:monospace;font-size:11px;word-break:break-all}
#filters .grp>div{display:flex;flex-wrap:wrap;gap:2px 0}
.box li>span{min-width:0;overflow-wrap:anywhere}
.scroll{overflow-x:auto;max-width:100%}
@media (max-width:700px){body{padding:8px}.box.main{flex-basis:100%;min-width:0}#filters{position:static}th,td{padding:2px 4px}}
</style></head><body>
<h1>Ingestion funnel: downloaded to uploaded</h1>
<div id="meta" class="muted"></div>
<div id="problems"></div>
<div id="filters"></div>
<div id="filter-help" class="muted" style="margin:6px 0"></div>
<h2>Flow through every stage, in run order</h2>
<div class="tabs"><button id="tab-flow" class="on">Flow</button> <button id="tab-sankey">Sankey</button></div>
<div id="view-flow"><div class="muted" style="margin:6px 0">Top to bottom, one box per stage that gates articles, in the order the pipeline runs them. <b>In</b> is every article whose input passed (for triage: whose metadata was attempted). Each one then passed, was left out, or is waiting: beside the box, by state and reason, with what each means in plain words. A reason joined from several sources is listed under its primary part, with the combinations underneath. Records made before the current stage graph sit outside the flow, as stale. Click a box, a count or a reason to list its articles; click an article for its trace.</div><div id="flow" class="flow"></div></div>
<div id="view-sankey" style="display:none"><div class="muted">Each column is a stage on the path to upload. The band to the right is what passed on; the drops below are what stayed behind, split by state and reason. Click a drop to list its articles.</div>
<div style="overflow-x:auto"><svg id="sankey"></svg></div>
<div class="legend" id="legend"></div></div>
<h2>Every stage</h2>
<div class="muted">Every article has one state at every stage, so each row adds up to the total. <b>entered</b> = total &minus; blocked &minus; stale. Click a number to list its articles; click a stage for its reasons.</div>
<div class="scroll"><table id="stages"></table></div>
<h2 id="reasons-title">Reasons</h2>
<div class="scroll"><table id="reasons"></table></div>
<h2>Kept with notes</h2>
<div class="muted">Records that passed though a field is missing, or was left out by an error. A transient error (a provider busy or down) is retried by the next run after the retry delay; any other is redone with <code>--select errors:&lt;field&gt; --refresh &lt;stage&gt;</code>.</div>
<div class="scroll"><table id="noted"></table></div>
<h2>Sources</h2>
<div class="muted">Download and extraction per source (one row per article and source, so an article counts once under every source it was tried at), the source whose text triage judged, and how many articles with that source reached upload. A bad page is a download that served something other than the article (a paywall, an error page, an empty shell, a citation stub).</div>
<div class="scroll"><table id="sources"></table></div>
<h2 id="drill-title">Drill-down</h2>
<div id="drill"><div class="muted">Click a number above.</div></div>
<h2 id="trace-title">Article trace</h2>
<div><label for="trace-id">Trace an article id:</label> <input id="trace-id" size="16" placeholder="e.g. 222lv3326a2c"> <button id="trace-go">Trace</button></div>
<div id="trace" class="scroll"><div class="muted">Click an article in the drill-down.</div></div>
<script id="data" type="application/json">__DATA__</script>
<script>
"use strict";
const D=typeof FUNNEL_DATA!=="undefined"?FUNNEL_DATA:JSON.parse(document.getElementById("data").textContent);
const S=D.strings, STATES=S.state, STAGES=D.stages.map(s=>s.name), GROUP=D.groups;
const COLOR={passed:"#2e9d57",filtered:"#8a9bb3",failed:"#d0453a",permanent:"#8c1d18",taken_back:"#8a5cc7",refused:"#b5482a",blocked:"#c9ced8",pending:"#e6c229",not_attempted:"#f2e6a6",stale:"#a0703c",unaccounted:"#e3249b",rerun:"#d9a05b",held:"#c8b06a",spent:"#6e2a24",ok:"#2e9d57",skipped:"#8a9bb3"};
// Text in each state's colour, dark enough for 4.5:1 on the page's light panels.
const INK={passed:"#1b6e34",filtered:"#4f5b6e",failed:"#a1261b",permanent:"#7a1914",taken_back:"#6a3fa3",refused:"#9a3412",blocked:"#4f5b6e",pending:"#6b5300",not_attempted:"#5e4f00",stale:"#7a4a1c",unaccounted:"#a3155f",rerun:"#7d5317",held:"#5e4f00",spent:"#5a1f1a",ok:"#1b6e34",skipped:"#4f5b6e"};
const DROPS=["filtered","failed","spent","permanent","taken_back","refused","pending","not_attempted","rerun","held","unaccounted"];
const fmt=n=>n.toLocaleString("en-US");
const esc=s=>String(s).replace(/[&<>"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));
const F={dl:new Set(),tried:new Set(),text:new Set(),from:"",to:"",state:new Set(),reason:""};
const dlSources=[...new Set(S.dl.flatMap(d=>d.split("+")))].sort();
const triedSources=[...new Set(S.tried.flatMap(d=>d.split("+")))].sort();
function articleOk(dl,text,day,tried){
  if(F.dl.size&&!S.dl[dl].split("+").some(s=>F.dl.has(s)))return false;
  if(F.tried.size&&!S.tried[tried].split("+").some(s=>F.tried.has(s)))return false;
  if(F.text.size&&!F.text.has(S.text[text]))return false;
  const d=S.day[day];if(F.from&&d<F.from)return false;if(F.to&&d>F.to)return false;return true}
function bucketOk(state,reason){
  if(F.state.size&&!F.state.has(STATES[state]))return false;
  return !F.reason||S.reason[reason].toLowerCase().includes(F.reason.toLowerCase())}
function cells(){return D.cells.filter(c=>articleOk(c[3],c[4],c[5],c[8]))}
function chips(name,values,set){return `<div class="grp"><b>${name}</b><div>`+values.map(v=>`<label class="chip"><input type="checkbox" data-set="${set}" value="${esc(v)}">${esc(v)}</label>`).join("")+`</div></div>`}
function filters(){
  const days=[...S.day].sort();
  document.getElementById("filters").innerHTML=
    chips("Downloaded from (any of)",[...dlSources],"dl")+chips("Download tried at (any of)",[...triedSources],"tried")+chips("Judged text source",[...S.text].sort(),"text")+
    `<div class="grp"><b>Registered in catalog</b><div><select id="from"><option value="">from</option>${days.map(d=>`<option>${d}</option>`).join("")}</select> <select id="to"><option value="">to</option>${days.map(d=>`<option>${d}</option>`).join("")}</select></div></div>`+
    chips("State (tables, drill-down)",STATES,"state")+
    `<div class="grp"><b>Reason contains</b><input id="reason" size="22" placeholder="e.g. open access"></div><div class="grp"><b>&nbsp;</b><button id="reset">Reset</button></div>`;
  document.querySelectorAll("#filters input[type=checkbox]").forEach(el=>el.onchange=()=>{const s=F[el.dataset.set];el.checked?s.add(el.value):s.delete(el.value);render()});
  document.getElementById("from").onchange=e=>{F.from=e.target.value;render()};
  document.getElementById("to").onchange=e=>{F.to=e.target.value;render()};
  document.getElementById("reason").oninput=e=>{F.reason=e.target.value;render()};
  document.getElementById("reset").onclick=()=>{F.dl.clear();F.tried.clear();F.text.clear();F.state.clear();F.from=F.to=F.reason="";filters();render()};
}
function filtered(){return !!(F.dl.size||F.tried.size||F.text.size||F.from||F.to)}
const cmp=(a,b)=>a<b?-1:a>b?1:0, sumOf=gs=>gs.reduce((a,g)=>a+g[2],0);
// The browser's recount, used only under an article filter; without one the page
// draws D.model, which page_model() counted in Python. Both group as _grouped does.
function grouped(pairs,states){const g={};
  Object.entries(pairs).forEach(([k,n])=>{const [x,r]=k.split("|").map(Number),st=STATES[x];if(!states.includes(st))return;
    const key=st+"|"+GROUP[r];(g[key]=g[key]||{st,group:GROUP[r],m:[]}).m.push([S.reason[r],n])});
  return Object.values(g).map(o=>{const m=o.m.sort((a,b)=>b[1]-a[1]||cmp(a[0],b[0]));return [o.st,o.group,m.reduce((a,x)=>a+x[1],0),m]})
    .sort((a,b)=>b[2]-a[2]||cmp(a[0],b[0])||cmp(a[1],b[1]))}
function model(cs){
  const by={},totals={},up={};STAGES.forEach(s=>{by[s]={};totals[s]={};STATES.forEach(x=>totals[s][x]=0)});D.stages.forEach(s=>up[s.name]=s.upstream);
  cs.forEach(c=>{const st=S.stage[c[0]],k=c[1]+"|"+c[2];by[st][k]=(by[st][k]||0)+c[6];totals[st][STATES[c[1]]]+=c[6]});
  const total=STATES.reduce((a,x)=>a+totals[STAGES[0]][x],0);
  const flow=D.flow_stages.map(st=>{const t=totals[st],u=up[st],came=total-t.blocked-t.stale,excluded=grouped(by[st],D.excluded),waiting=grouped(by[st],D.waiting);
    return {stage:st,input:u,in:came,passed:t.passed,excluded,waiting,stale:grouped(by[st],["stale"]),adds_up:came===t.passed+sumOf(excluded)+sumOf(waiting)}});
  const reasons={};STAGES.forEach(s=>reasons[s]=grouped(by[s],STATES));
  return {total,totals,flow,reasons}}
function M(){return filtered()?model(cells()):D.model}
function sankey(m){
  const t=m.totals,all=m.total,chain=D.trunk,svg=document.getElementById("sankey");
  const colW=150,barW=14,top=24,trunkH=220,scale=all?trunkH/all:0,W=chain.length*colW+260;
  const drops=chain.map(st=>{let e=m.reasons[st].filter(g=>DROPS.includes(g[0]));
    if(e.length>6){const rest=sumOf(e.slice(5));e=e.slice(0,5).concat([["other","(more reasons, see table)",rest,[]]])}return e});
  const maxDrops=Math.max(1,...drops.map(d=>d.length));const H=top+trunkH+40+maxDrops*30+20;
  let g=`<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}">`;
  const entered=st=>all-t[st].blocked-t[st].stale;
  chain.forEach((st,i)=>{const x=20+i*colW,inn=entered(st),h=Math.max(1,inn*scale),y=top;
    g+=`<rect x="${x}" y="${y}" width="${barW}" height="${h}" fill="#3b4a66"><title>${st}: ${fmt(inn)} entered</title></rect>`;
    g+=`<text x="${x}" y="${top-8}" font-weight="600">${st}</text><text x="${x+barW+4}" y="${top+12}">${fmt(inn)}</text>`;
    if(t[st].stale)g+=`<text x="${x+barW+4}" y="${top+26}" fill="${INK.stale}">+${fmt(t[st].stale)} stale</text>`;
    const pass=t[st].passed,ph=pass*scale;
    if(i<chain.length-1){const nx=20+(i+1)*colW,nh=Math.max(1,entered(chain[i+1])*scale);
      g+=`<path d="M${x+barW},${y} C${x+barW+60},${y} ${nx-60},${y} ${nx},${y} L${nx},${y+nh} C${nx-60},${y+nh} ${x+barW+60},${y+ph} ${x+barW},${y+ph} Z" fill="#2e9d57" opacity=".35"><title>${fmt(pass)} passed ${st}</title></path>`}
    else g+=`<text x="${x+barW+4}" y="${top+40}" fill="${INK.passed}" font-weight="600">${fmt(pass)} passed</text>`;
    let yy=y+ph,dy=top+trunkH+40;
    drops[i].forEach(([state,group,n])=>{const dh=Math.max(1.5,n*scale);
      g+=`<path d="M${x+barW},${yy} C${x+barW+40},${yy} ${x+20},${dy} ${x+40},${dy} L${x+40},${dy+dh} C${x+20},${dy+dh} ${x+barW+40},${yy+n*scale} ${x+barW},${yy+n*scale} Z" fill="${COLOR[state]||"#999"}" opacity=".5"/>`;
      g+=`<g style="cursor:pointer" data-stage="${esc(st)}" data-state="${esc(state)}" data-group="${esc(group)}"><rect x="${x+40}" y="${dy}" width="6" height="${dh}" fill="${COLOR[state]||"#999"}"/><text x="${x+50}" y="${dy+9}">${fmt(n)} ${esc(state)}</text><text x="${x+50}" y="${dy+21}" class="muted">${esc(group.length>24?group.slice(0,23)+"…":group)}</text><title>${esc(st)} ${esc(state)}: ${esc(group)} (${fmt(n)})</title></g>`;
      yy+=n*scale;dy+=Math.max(30,dh+18)});
  });
  svg.outerHTML=g.replace("<svg ",`<svg id="sankey" `)+"</svg>";
  document.querySelectorAll("#sankey g[data-stage]").forEach(el=>el.onclick=()=>drill(el.dataset.state==="other"?{stage:el.dataset.stage,states:DROPS}:{stage:el.dataset.stage,state:el.dataset.state,group:el.dataset.group}));
  document.getElementById("legend").innerHTML=STATES.map(s=>`<span><i style="background:${COLOR[s]}"></i>${s}</span>`).join("");
}
function meaning(state,text){const g=D.glossary.find(([k])=>text.includes(k));return g?`${g[1]} (${D.meanings[state]})`:D.meanings[state]}
const ENTERED=STATES.filter(s=>!["blocked","stale"].includes(s));
function groupList(st,groups){return groups.map(([state,group,n,members])=>
  `<li data-stage="${esc(st)}" data-state="${esc(state)}" data-group="${esc(group)}"><span style="color:${INK[state]}">${esc(state)}</span><span>${esc(group||"(no reason)")}</span><span class="n">${fmt(n)}</span><span class="mean">${esc(meaning(state,group))}</span></li>`+
  (members.length>1?`<li class="sub"><details><summary class="muted">${members.length} combinations of the sources' reasons</summary><ul>${members.map(([r,k])=>`<li data-stage="${esc(st)}" data-state="${esc(state)}" data-reason="${esc(r)}"><span>${esc(r)}</span><span class="n">${fmt(k)}</span></li>`).join("")}</ul></details></li>`:"")).join("")}
function folded(st,groups){return groups.length>8?groupList(st,groups.slice(0,8))+`<li class="sub"><details><summary class="muted">${groups.length-8} more reasons</summary><ul>${groupList(st,groups.slice(8))}</ul></details></li>`:groupList(st,groups)}
function flowView(m){
  const feeds={};D.stages.forEach(s=>{if(s.upstream)(feeds[s.upstream]=feeds[s.upstream]||[]).push(s.name)});
  Object.entries(D.side_inputs).forEach(([st,ins])=>ins.forEach(i=>(feeds[i]=feeds[i]||[]).includes(st)||feeds[i].push(st)));
  let h=filtered()?`<div class="muted">Filtered: recounted in this browser for the selected articles. Without a filter the page shows the numbers <code>ingest report funnel</code> counted.</div>`:`<div class="ok">✓ These are the numbers <code>ingest report funnel</code> counted and prints.</div>`;
  m.flow.forEach((x,i)=>{const ex=sumOf(x.excluded),wt=sumOf(x.waiting),st=sumOf(x.stale);
    h+=`<div class="step"><div class="box main" data-stage="${esc(x.stage)}" role="button" tabindex="0"><div><b>${esc(x.stage)}</b> <span class="muted">${x.input?"input: "+esc(x.input)+(D.planned?", taken as its plan takes it":", taken when it passed"):"every live article"}</span></div><div class="big">${fmt(x.in)} in</div>`+
      `<div><span class="click" data-passed="${esc(x.stage)}">${fmt(x.passed)} passed</span> &middot; ${fmt(ex)} left out &middot; ${fmt(wt)} waiting</div>`+
      `<div class="${x.adds_up?"ok":"bad"}">in = passed + left out + waiting: ${fmt(x.in)} = ${fmt(x.passed)} + ${fmt(ex)} + ${fmt(wt)} ${x.adds_up?"✓":"✗"}</div>`+
      (feeds[x.stage]?`<div class="muted">feeds ${feeds[x.stage].map(esc).join(", ")}</div>`:"")+`</div>`+
      `<div class="box excl"><b>Left out: ${fmt(ex)}</b><ul>${folded(x.stage,x.excluded)}</ul>${wt?`<b>Waiting: ${fmt(wt)}</b><ul>${folded(x.stage,x.waiting)}</ul>`:""}`+
      (st?`<b>Records outside the flow: ${fmt(st)} stale</b><ul>${folded(x.stage,x.stale)}</ul>`:"")+`</div></div>`;
    if(i<m.flow.length-1)h+=`<div class="arrow" aria-hidden="true">&darr;</div>`});
  const el=document.getElementById("flow");el.innerHTML=h;
  el.querySelectorAll(".box.main").forEach(b=>{b.onclick=()=>drill({stage:b.dataset.stage,states:ENTERED});b.onkeydown=e=>{if(e.key==="Enter")b.onclick()}});
  el.querySelectorAll("[data-passed]").forEach(e=>e.onclick=ev=>{ev.stopPropagation();drill({stage:e.dataset.passed,state:"passed"})});
  el.querySelectorAll("li[data-group]").forEach(e=>e.onclick=()=>drill({stage:e.dataset.stage,state:e.dataset.state,group:e.dataset.group}));
  el.querySelectorAll("li[data-reason]").forEach(e=>e.onclick=()=>drill({stage:e.dataset.stage,state:e.dataset.state,reason:e.dataset.reason}));
}
function stageTable(m){
  const t=m.totals,all=m.total;const show=STATES.filter(s=>STAGES.some(st=>t[st][s]));
  let h=`<tr><th>stage</th><th>input</th><th>entered</th>${show.map(s=>`<th style="color:${INK[s]}">${s}</th>`).join("")}<th>sum</th><th>share of entered</th></tr>`;
  D.stages.forEach(st=>{const r=t[st.name],sum=STATES.reduce((a,s)=>a+r[s],0),inn=all-r.blocked-r.stale;
    const bar=inn?`<div class="bar">${STATES.filter(s=>!["blocked","stale"].includes(s)).map(s=>`<span title="${s} ${fmt(r[s])}" style="width:${100*r[s]/inn}%;background:${COLOR[s]}"></span>`).join("")}</div>`:"";
    h+=`<tr><td class="click" data-reasons="${st.name}"><b>${st.name}</b>${D.trunk.includes(st.name)?"":' <span class="muted">(side)</span>'}</td><td class="muted">${st.upstream||"&mdash;"}</td><td>${fmt(inn)}</td>`+
      show.map(s=>`<td class="click${s==="unaccounted"&&r[s]?" bad":""}" data-stage="${st.name}" data-state="${s}" style="${!F.state.size||F.state.has(s)?"":"opacity:.3"}">${r[s]?fmt(r[s]):""}</td>`).join("")+
      `<td class="${sum===all?"ok":"bad"}" title="${sum===all?"reconciles":"does not add up"}">${fmt(sum)} ${sum===all?"✓":"✗"}</td><td>${bar}</td></tr>`});
  const el=document.getElementById("stages");el.innerHTML=h;
  el.querySelectorAll("td[data-state]").forEach(td=>td.onclick=()=>drill({stage:td.dataset.stage,state:td.dataset.state}));
  el.querySelectorAll("td[data-reasons]").forEach(td=>td.onclick=()=>{CUR=td.dataset.reasons;reasons(M())});
}
let CUR=null;
function reasons(m){
  const st=CUR||D.trunk[0],needle=F.reason.toLowerCase();document.getElementById("reasons-title").textContent=`Reasons at ${st}`;
  const rows=m.reasons[st].filter(g=>!F.state.size||F.state.has(g[0])).map(([s,g,n,mem])=>{const k=mem.filter(([r])=>r.toLowerCase().includes(needle));return [s,g,needle?k.reduce((a,x)=>a+x[1],0):n,k]}).filter(r=>r[2]);
  document.getElementById("reasons").innerHTML=`<tr><th>state</th><th>reason</th><th>articles</th></tr>`+rows.map(([s,g,n,mem])=>
    `<tr><td style="color:${INK[s]}">${esc(s)}</td><td class="click" style="text-align:left" data-s="${esc(s)}" data-g="${esc(g)}">${esc(g||"—")}${mem.length>1?` <span class="muted">(${mem.length} combinations)</span>`:""}</td><td>${fmt(n)}</td></tr>`).join("");
  document.querySelectorAll("#reasons td[data-g]").forEach(td=>td.onclick=()=>drill({stage:st,state:td.dataset.s,group:td.dataset.g}));
}
function sourceTable(cs){
  const src=D.sources.filter(c=>articleOk(c[4],c[5],c[6],c[9]));const T={};
  const add=(s,k,n)=>{T[s]=T[s]||{};T[s][k]=(T[s][k]||0)+n};
  src.forEach(c=>add(S.source[c[1]],S.stage[c[0]]+":"+S.status[c[2]],c[7]));
  const up=D.trunk[D.trunk.length-1];
  cs.forEach(c=>{const st=S.stage[c[0]];if(st==="triage"&&STATES[c[1]]!=="blocked")add(S.text[c[4]],"judged",c[6]);
    if(st===up&&STATES[c[1]]==="passed"){S.dl[c[3]].split("+").forEach(s=>add(s,"reached",c[6]));add(S.text[c[4]],"reached_judged",c[6])}});
  const cols=[["download:ok","downloaded"],["download:failed","dl failed"],["download:permanent","dl permanent"],["download:skipped","dl skipped"],["extract:ok","extracted"],["extract:failed","extract failed"],["extract:permanent","extract permanent"],["extract:skipped","extract skipped"],["extract:taken_back","extract no text"],["judged","text triage judged"],["reached","has a download, reached "+up],["reached_judged","judged text, reached "+up]];
  const names=Object.keys(T).filter(s=>s!=="none").sort().concat(T.none?["none"]:[]);
  document.getElementById("sources").innerHTML=`<tr><th>source</th>${cols.map(c=>`<th>${c[1]}</th>`).join("")}</tr>`+
    names.map(s=>`<tr><td>${esc(s)}</td>${cols.map(([k])=>{const n=(T[s]||{})[k]||0;const [stg,sta]=k.split(":");return `<td class="${sta?"click":""}" data-src="${esc(s)}" data-k="${k}">${n?fmt(n):""}</td>`}).join("")}</tr>`).join("");
  document.querySelectorAll("#sources td.click").forEach(td=>td.onclick=()=>{const [stg,sta]=td.dataset.k.split(":");drillSource(stg,td.dataset.src,sta)});
}
const q=s=>"'"+String(s).replace(/'/g,"'\\''")+"'";
function articleArgs(){let c=` --catalog ${q(D.catalog)} ${D.prose?"--prose":"--no-prose"}`;
  [["dl","--source"],["tried","--tried"],["text","--judged"]].forEach(([k,flag])=>[...F[k]].sort().forEach(v=>c+=` ${flag} ${q(v)}`));
  if(F.from)c+=` --from ${F.from}`;if(F.to)c+=` --to ${F.to}`;return c}
function wanted(o){const w=o.states||(o.state?[o.state]:null);return w?(F.state.size?w.filter(s=>F.state.has(s)):w):[...F.state]}
function cliCmd(o){
  const states=wanted(o);
  if((o.states||o.state)&&!states.length)return "(none: the state filter leaves this bucket out)";
  let c=`ingest report funnel-ids --stage ${o.stage}`+articleArgs();states.forEach(s=>c+=` --state ${s}`);
  if(o.reason!=null)c+=` --exact-reason ${q(o.reason)}`;if(o.group!=null)c+=` --reason-group ${q(o.group)}`;if(F.reason)c+=` --reason ${q(F.reason)}`;return c}
function showDrill(title,rows,total,cmd){
  const ids=rows.map(r=>r[0]);
  document.getElementById("drill-title").textContent="Drill-down: "+title;
  document.getElementById("drill").innerHTML=`<div>${fmt(total)} articles; ${fmt(rows.length)} sampled here (up to ${D.samples} per bucket). <button id="copy">Copy ${fmt(ids.length)} ids</button> <input id="drill-search" size="24" placeholder="search id, reason, raw text"></div>`+
   `<div class="muted" style="margin:6px 0">Every id in this bucket: <code>${esc(cmd)}</code></div><div style="overflow-x:auto"><table id="drill-rows"></table></div>`;
  const draw=needle=>{const n=needle.toLowerCase(),shown=rows.filter(r=>!n||r.some(v=>String(v).toLowerCase().includes(n)));
    document.getElementById("drill-rows").innerHTML=`<tr><th>article</th><th>stage</th><th>state</th><th>reason</th><th>raw</th><th>downloaded from</th><th>judged text</th><th>registered</th></tr>`+
    shown.slice(0,1000).map(r=>`<tr>${r.map((v,i)=>`<td title="${esc(v)}"${i?"":` class="click" data-id="${esc(v)}"`}>${esc(i===4&&v.length>140?v.slice(0,139)+"…":v)}</td>`).join("")}</tr>`).join("");
    document.querySelectorAll("#drill-rows td[data-id]").forEach(td=>td.onclick=()=>trace(td.dataset.id))};
  draw("");document.getElementById("drill-search").oninput=e=>draw(e.target.value);
  document.getElementById("copy").onclick=()=>{const txt=ids.join("\n");(navigator.clipboard?navigator.clipboard.writeText(txt):Promise.reject()).catch(()=>{const a=document.createElement("textarea");a.value=txt;document.body.appendChild(a);a.select();document.execCommand("copy");a.remove()})};
  document.getElementById("drill-title").scrollIntoView({behavior:"smooth"});
}
function bucket(o){
  let total=0;const rows=[],want=o.states||(o.state?[o.state]:null);
  cells().forEach(c=>{if(S.stage[c[0]]!==o.stage||want&&!want.includes(STATES[c[1]]))return;if(o.reason!=null&&S.reason[c[2]]!==o.reason)return;if(o.group!=null&&GROUP[c[2]]!==o.group)return;if(!bucketOk(c[1],c[2]))return;
    total+=c[6];c[7].forEach(([a,r])=>rows.push([S.id[a],o.stage,STATES[c[1]],S.reason[c[2]],S.raw[r],S.dl[c[3]],S.text[c[4]],S.day[c[5]]]))});
  return {total,rows}}
function drill(o){
  const {total,rows}=bucket(o);
  showDrill(`${o.stage} / ${o.states?"in":o.state||"any"}${o.reason?" / "+o.reason:o.group?" / "+o.group:""}`,rows,total,cliCmd(o));
}
function drillSource(stage,source,state){
  let total=0;const rows=[];
  D.sources.forEach(c=>{if(!articleOk(c[4],c[5],c[6],c[9]))return;if(S.stage[c[0]]!==stage||S.source[c[1]]!==source||S.status[c[2]]!==state)return;
    total+=c[7];c[8].forEach(([a,r])=>rows.push([S.id[a],stage+":"+source,state,S.reason[c[3]]||"",S.raw[r],S.dl[c[4]],S.text[c[5]],S.day[c[6]]]))});
  showDrill(`${stage} from ${source} / ${state}`,rows,total,`ingest report funnel-ids --stage ${stage} --by-source ${q(source)} --state ${state}`+articleArgs());
}
function trace(id){
  const el=document.getElementById("trace"),ix=S.id.indexOf(id);
  document.getElementById("trace-title").textContent="Article trace: "+id;
  const cmd=`ingest report trace ${q(id)} --catalog ${q(D.catalog)} -c <pipeline config>`;
  const known=STAGES.map(st=>{const c=D.cells.find(c=>S.stage[c[0]]===st&&c[7].some(([a])=>a===ix));if(!c)return [st,"","",""];const r=c[7].find(([a])=>a===ix);return [st,STATES[c[1]],S.reason[c[2]],S.raw[r[1]]]});
  el.innerHTML=`<div class="muted">The state at every stage, as this page counted it. Full trace (every record's path, status, fingerprint, freshness and inputs): <code>${esc(cmd)}</code>, or open this page with <code>ingest report serve</code> and click again.</div>`+
    `<table><tr><th>stage</th><th>state</th><th>reason</th></tr>${known.map(([st,s,r,raw])=>`<tr><td>${esc(st)}</td><td style="color:${INK[s]||"inherit"}">${esc(s||"(not sampled)")}</td><td>${esc(r.replace(/other: [^+;]*?(?= ?(?:[+;]|$))/g,"the stage raised an error").trimEnd())}${raw?`<details><summary class="muted">technical detail</summary><pre class="detail">${esc(raw)}</pre></details>`:""}</td></tr>`).join("")}</table>`;
  document.getElementById("trace-title").scrollIntoView();
  if(!location.protocol.startsWith("http"))return;
  fetch("trace?id="+encodeURIComponent(id)).then(r=>r.json()).then(t=>{
    if(t.error){el.innerHTML=`<div class="bad">${esc(t.error)}</div>`;return}
    const kv=o=>Object.entries(o||{}).map(([k,v])=>`${esc(k)}=${esc(typeof v==="object"?JSON.stringify(v):v)}`).join(", ");
    el.innerHTML=`<div class="muted">Registered ${esc(t.registered)}. From the catalog rows only; no payload is opened.</div><table><tr><th>stage</th><th>state</th><th>records</th><th>built from</th><th>freshness</th></tr>`+
      t.stages.map(x=>`<tr><td><b>${esc(x.stage)}</b><div class="muted">current code: ${kv(x.version)||"&mdash;"} (a record's own version is folded into its fingerprint; see freshness)</div></td><td><span style="color:${INK[x.state]}">${esc(x.state)}</span> ${esc(x.reason)}<div class="muted">${esc(x.meaning)}</div></td>`+
        `<td>${x.records.map(r=>`<div><b>${esc(r.source||"(one)")}</b> ${esc(r.status)}${r.reason?": "+esc(r.reason):""}; written ${esc(r.written)}; ${r.attempts} attempt(s)${r.last?", last "+esc(r.last):""}<div class="muted">fingerprint ${esc(r.fingerprint||"(none)")}; ${kv(r.summary)}</div>${r.path?`<div class="path">${esc(r.path)} ${r.on_disk?"":"(missing)"}</div>`:""}${r.error_detail?`<details><summary class="muted">technical detail</summary><pre class="detail">${esc(r.error_detail)}</pre></details>`:""}</div>`).join("")||"&mdash;"}</td>`+
        `<td>${x.built_from.map(u=>`<div>${esc(u.stage)}${u.source?":"+esc(u.source):""} ${esc(u.status)}, ${esc(u.fingerprint.slice(0,12)||"(none)")}, written ${esc(u.written)}${u.newer?' <span class="bad">rewritten after this record</span>':""}</div>`).join("")||"&mdash;"}</td>`+
        `<td>${esc(x.plan||"")}${x.records.filter(r=>r.freshness&&r.freshness!==x.plan).map(r=>`<div>${esc(r.source)}: ${esc(r.freshness)}</div>`).join("")}</td></tr>`).join("")+`</table>`;document.getElementById("trace-title").scrollIntoView()});
}
function notedTable(){const rows=Object.entries(D.noted||{}).flatMap(([st,by])=>Object.entries(by).map(([f,[e,t,m]])=>[st,f,e,t,m]));
  document.getElementById("noted").innerHTML=rows.length?`<tr><th>stage</th><th>field</th><th>errors</th><th>of them transient</th><th>missing</th></tr>`+rows.map(([st,f,e,t,m])=>`<tr><td>${esc(st)}</td><td>${esc(f)}</td><td class="n">${fmt(e)}</td><td class="n">${fmt(t)}</td><td class="n">${fmt(m)}</td></tr>`).join(""):`<tr><td class="muted">None noted.</td></tr>`}
function render(){const m=M();flowView(m);sankey(m);stageTable(m);reasons(m);sourceTable(cells());notedTable()}
if(typeof document!=="undefined"&&document.getElementById("flow")){
document.getElementById("trace-go").onclick=()=>{const v=document.getElementById("trace-id").value.trim();if(v)trace(v)};
document.getElementById("trace-id").onkeydown=e=>{if(e.key==="Enter")document.getElementById("trace-go").onclick()};
document.getElementById("tab-flow").onclick=()=>{document.getElementById("view-flow").style.display="";document.getElementById("view-sankey").style.display="none";document.getElementById("tab-flow").className="on";document.getElementById("tab-sankey").className=""};
document.getElementById("tab-sankey").onclick=()=>{document.getElementById("view-flow").style.display="none";document.getElementById("view-sankey").style.display="";document.getElementById("tab-sankey").className="on";document.getElementById("tab-flow").className=""};
document.getElementById("meta").innerHTML=`${fmt(D.total)} live articles in <code>${esc(D.catalog)}</code>; built ${D.generated} in ${D.seconds}s; stage graph ${D.prose?"with":"without"} the prose stages.`;
document.getElementById("filter-help").innerHTML=`<b>Filters.</b> These select articles, so every stage still adds up: <b>Downloaded from</b> keeps articles with an ok download from any ticked source (an article whose every download failed has source <i>none</i>, so ticking a source hides its download failures); <b>Download tried at</b> keeps articles with any download recorded from a ticked source, failed or not (use it for losses at download by source); <b>Judged text source</b> is the source whose text triage, or else passages, read; <b>Registered</b> is the day the article entered the catalog. These narrow the tables and the drill-down only: <b>State</b>, and <b>Reason contains</b> (a case-insensitive substring; a clicked reason matches exactly). The source table always lists every source tried; its numbers follow the article filters.`;
document.getElementById("problems").innerHTML=(D.planned?"":`<div class="warn">Records only: no stage was planned, so nothing here says what a run would do.</div>`)+(D.problems.length?`<div class="warn">Does not reconcile:</div><ul>${D.problems.map(p=>`<li>${esc(p)}</li>`).join("")}</ul>`:`<div class="muted">Reconciles: every stage accounts for every article, and every article with a record at a stage was placed from that record.</div>`);
filters();render();
// A link can open one article's trace: <page>#trace=<article id>.
if(location.hash.startsWith("#trace="))trace(decodeURIComponent(location.hash.slice(7)));
}
</script></body></html>
"""  # noqa: E501 -- the page is minified HTML and JS, not Python
