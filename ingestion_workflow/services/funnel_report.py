"""Where every article stands at every stage, counted from the catalog.

Each live article gets exactly one state per stage, so a stage's states always
add up to the catalog's article count. The state comes from the article's own
artifact when it has one, and otherwise from its input:

passed        ok, and the next stage takes it as input
filtered      recorded, but holding nothing the next stage takes (no tables, ...)
failed        a failure the scheduler retries
permanent     a failure it does not retry
taken_back    every extraction found no text, so what was made from it was withdrawn
held          upload kept it because a studyset holds the study
refused       its input passed, but this stage will not read it: the text was
              made from a downloaded page that is not the article
blocked       never ran here because its input did not pass; the reason names
              the stage where the article dropped out and why. A failed or empty
              record left here from an earlier run counts as blocked too, and its
              reason adds that record's own status and reason ("leftover ...")
pending       its input passed and nothing is recorded here yet; the reason says
              when the input holds nothing for this stage to read
not_attempted its input passed and this stage has never run on the catalog
stale         an ok artifact exists although its input did not pass, so it was
              made from an input that has since gone or under an older stage graph
unaccounted   an attempt is recorded but no outcome, or a status the catalog does
              not define

Only small summary fields are read, through `json_extract`, and no blob is
opened. Rows are streamed in article order, so memory does not grow with the
catalog.
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

from ingestion_workflow.catalog import Catalog
from ingestion_workflow.catalog.blobs import BlobStore
from ingestion_workflow.pipeline.stage import HELD_FOR_REVIEW, NO_TEXT
from ingestion_workflow.pipeline.stages.extract import READ_FROM_BAD_PAGE

PASSED = "passed"
FILTERED = "filtered"
FAILED = "failed"
PERMANENT = "permanent"
TAKEN_BACK = "taken_back"
HELD = "held"
REFUSED = "refused"
BLOCKED = "blocked"
PENDING = "pending"
NOT_ATTEMPTED = "not_attempted"
STALE = "stale"
UNACCOUNTED = "unaccounted"

STATES = (PASSED, FILTERED, FAILED, PERMANENT, TAKEN_BACK, HELD, REFUSED, BLOCKED, PENDING,
          NOT_ATTEMPTED, STALE, UNACCOUNTED)

#: What each state means, in plain words, for the page.
MEANINGS = {
    PASSED: "done, and handed on to the next stage",
    FILTERED: "done, but it holds nothing the next stage reads",
    FAILED: "failed; the pipeline will try again",
    PERMANENT: "failed in a way the pipeline does not retry",
    TAKEN_BACK: "no extraction found any text, so what was made from it was withdrawn",
    HELD: "upload kept the study already there, because a studyset holds it",
    REFUSED: "its input passed, but that text came from a downloaded page that is not the article",
    BLOCKED: "never reached this stage: it dropped out at an earlier one",
    PENDING: "ready for this stage, which has not run on it yet",
    NOT_ATTEMPTED: "ready, but this stage has never run on the catalog",
    STALE: "a record made from an input that has since gone or changed",
    UNACCOUNTED: "an attempt with no outcome, or a status the catalog does not know: a bug",
}

#: Of the articles a stage on the upload path takes in, those it leaves out and
#: those still waiting; the rest passed. The flow diagram sums these.
EXCLUDED = (FILTERED, FAILED, PERMANENT, TAKEN_BACK, HELD, REFUSED, UNACCOUNTED)
WAITING = (PENDING, NOT_ATTEMPTED)

#: Plain words for common reasons, matched as a substring, first match wins.
GLOSSARY: Tuple[Tuple[str, str], ...] = (
    ("leftover", "a failed or empty record left from an earlier run; nothing downstream "
                 "reads it"),
    ("tables wait behind it", "the article has coordinate tables, which reach resolve only "
                              "after this free step runs"),
    ("nothing to read (passages = 0)", "no passage to send to the model; running it costs "
                                       "nothing and records an empty answer"),
    (READ_FROM_BAD_PAGE, "the text it would read came from a page that is not the article"),
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
    (HELD_FOR_REVIEW, "the study is in a studyset, so the copy already uploaded stays"),
    (NO_TEXT, "no extractor found text in the download"),
)

#: A stage that runs per source keeps one artifact per source; the article's
#: state is the best any source reached, as `Catalog.status_counts` counts it.
_RANK = {"ok": 0, "failed": 1, "permanent": 2, "skipped": 3}

#: Triage waits for metadata to have been attempted, not to have succeeded
#: (`TriageStage.plan`): plenty of articles have no metadata to find.
_INPUT_ON_ATTEMPT = {"triage"}

#: Stages that refuse text made from a downloaded page that is not the article,
#: and the stage whose artifact names that text's source (`read_from_bad_page`
#: in the prose, resolve and sync plans).
_READS_TEXT = {"prose": "passages", "resolve": "prose", "sync": "prose"}

#: A stage whose input can pass while holding nothing for it to read, and the
#: count on the input's summary that says so. Prose answers an article with no
#: passage without a model call (`ProseStage.plan`), so such an article is
#: pending for free, unlike one waiting on the model.
_EMPTY_INPUT = {"prose": "passages"}

#: Free-text errors folded into a few categories; the raw text stays for the
#: drill-down. First match wins, so the specific patterns come first.
_CATEGORIES: Tuple[Tuple[str, str], ...] = (
    (r"held for review", "held for review"),
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


def categorize(error: Optional[str]) -> str:
    """The category of a recorded error; unmatched text keeps its own shape."""
    if not error:
        return "no reason recorded"
    for pattern, category in _COMPILED:
        if pattern.search(error):
            return category
    text = re.sub(r"https?://\S+", "<url>", error.strip())
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
    from ingestion_workflow.pipeline.stages import PROSE_STAGES, STAGE_ORDER, STAGE_TYPES

    settings = SimpleNamespace(prose_model="on" if prose else None)
    names = [n for n in STAGE_ORDER if prose or n not in PROSE_STAGES]
    upstream: Dict[str, Optional[str]] = {}
    flags: Dict[str, str] = {}
    for name in names:
        kind = STAGE_TYPES[name]
        if hasattr(kind, "upstream_for"):
            up, flag = kind.upstream_for(settings)
        else:
            up, flag = kind.requires, getattr(kind, "requires_flag", None)
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


def _failure_reason(row: _Row) -> str:
    if row.bad_page:
        return f"bad page: {row.bad_page}"
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
                      best.reason or best.error or "skipped: the source cannot handle it")
    failures = [r for r in rows if r.status != "ok" and r.error]
    if failures and all(r.error == NO_TEXT for r in failures):
        return Placed(TAKEN_BACK, NO_TEXT)
    if best.error and best.error.startswith(HELD_FOR_REVIEW):
        return Placed(HELD, HELD_FOR_REVIEW, best.error)
    reason = " + ".join(sorted({_failure_reason(r) for r in failures})) or "no reason recorded"
    raw = "; ".join((f"{r.source}: " if r.source else "") + (r.error or "")[:200]
                    for r in failures)
    return Placed(FAILED if best.status == "failed" else PERMANENT, reason, raw[:600])


def _origin(stage: str, placed: Placed) -> Placed:
    """What a stage downstream of `placed` reports as the reason it was blocked."""
    if placed.state == BLOCKED:
        return Placed(BLOCKED, placed.carry or placed.reason, placed.raw)
    why = f"{stage} {placed.state}" + (f": {placed.reason}" if placed.reason else "")
    return Placed(BLOCKED, why, placed.raw)


def place_article(graph: Sequence[StageNode], rows: Sequence[_Row], *, ran: set,
                  attempted: set) -> Dict[str, Placed]:
    """Every stage's state for one article. `attempted` holds the stages with an
    attempt but no artifact; `ran` the stages with any artifact in the catalog."""
    by_stage: Dict[str, List[_Row]] = defaultdict(list)
    for row in rows:
        by_stage[row.stage].append(row)
    bad = _bad_page_sources(rows)
    out: Dict[str, Placed] = {}
    for node in graph:
        own = by_stage.get(node.name)
        if node.upstream is None:
            entered = True
        else:
            before = out[node.upstream]
            entered = before.state == PASSED or (
                node.name in _INPUT_ON_ATTEMPT and before.state not in
                (BLOCKED, PENDING, NOT_ATTEMPTED, STALE, UNACCOUNTED))
            reads = _READS_TEXT.get(node.name)
            if (entered and reads and _from_bad_page(by_stage.get(reads), bad)
                    and not (node.name == "sync" and _retracted(by_stage.get("upload")))):
                # The stage blocks it and leaves any record it made before as it is.
                out[node.name] = Placed(REFUSED, f"{reads} {READ_FROM_BAD_PAGE}",
                                        recorded=bool(own))
                continue
        if own:
            out[node.name] = _recorded(node, own, before if not entered else None)
        elif not entered:
            out[node.name] = _origin(node.upstream, before)
        elif node.name == "resolve" and not _resolve_has_input(by_stage):
            # ResolveStage.plan skips these without recording anything.
            out[node.name] = Placed(FILTERED, "neither the tables nor the prose hold an analysis")
        elif node.name in attempted:
            out[node.name] = Placed(UNACCOUNTED, "an attempt is recorded but no outcome")
        elif node.name not in ran:
            out[node.name] = Placed(NOT_ATTEMPTED, "this stage has never run on the catalog")
        else:
            out[node.name] = Placed(PENDING, _pending_reason(node, by_stage))
    return out


def _recorded(node: StageNode, own: List[_Row], before: Optional[Placed]) -> Placed:
    """The state of an article with an artifact here; `before` is set when its input
    did not pass."""
    if before is None:
        placed = _own_state(node, own)
    elif all(r.status != "ok" and r.error == NO_TEXT for r in own):
        # The record a stage leaves when its input was taken back: the
        # article dropped out upstream, so it counts as blocked from there.
        placed = _origin(node.upstream, before)
    else:
        mine = _own_state(node, own)
        why = f"input {node.upstream} is {before.state}"
        if mine.state in (PASSED, HELD):
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


def _bad_page_sources(rows: Sequence[_Row]) -> Dict[str, str]:
    """Each source whose download or extraction failed as a page that is not the article."""
    return {r.source: r.bad_page for r in rows
            if r.stage in ("download", "extract") and r.status != "ok" and r.bad_page}


def _from_bad_page(rows: Optional[List[_Row]], bad: Dict[str, str]) -> bool:
    """Whether the artifact a stage reads was made from a source's bad page."""
    return bool(rows) and (rows[0].text_source or "") in bad


def _retracted(rows: Optional[List[_Row]]) -> bool:
    return bool(rows) and _truthy(rows[0].retracted)


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


def _resolve_has_input(by_stage: Dict[str, List[_Row]]) -> bool:
    prose = [r for r in by_stage.get("prose", []) if r.status == "ok"]
    tables = [r for r in by_stage.get("analyses", []) if r.status == "ok"]
    read = prose and (_truthy(prose[0].coordinates) or _truthy(prose[0].analyses))
    return bool(read or (tables and _truthy(tables[0].tables)))


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
        return ""
    if row.status == "skipped" and not row.bad_page:
        return row.reason or categorize(row.error)
    return _failure_reason(row)


_ROW_COLUMNS = """
SELECT a.article_id, a.stage, a.source, a.status, substr(a.error, 1, 400),
       json_extract(a.summary, '$.passed'), json_extract(a.summary, '$.tables'),
       json_extract(a.summary, '$.reason'), json_extract(a.summary, '$.source'),
       json_extract(a.summary, '$.coordinates'), json_extract(a.summary, '$.analyses'),
       json_extract(a.summary, '$.retracted'), json_extract(a.summary, '$.bad_page'),
       json_extract(a.summary, '$.passages')
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
    seconds: float = 0.0
    samples: int = 0

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
        return problems


def flow(funnel: Funnel) -> List[Dict[str, Any]]:
    """The upload path as a flow diagram, one step per stage: what came in (its input
    passed), what passed on, what was left out and what waits, each by state and
    reason, and the two checks the page shows. The CLI prints this; the page draws it,
    and recounts it in the browser only to apply its filters."""
    by: Dict[str, Dict[Tuple[str, str], int]] = defaultdict(lambda: defaultdict(int))
    for (stage, state, reason, *_), (count, _) in funnel.cells.items():
        by[stage][(state, reason)] += count
    steps: List[Dict[str, Any]] = []
    before: Optional[int] = None
    for stage in trunk(funnel.graph):
        cells = by[stage]

        def picked(states: Sequence[str]) -> List[list]:
            return sorted(([state, reason, n] for (state, reason), n in cells.items()
                           if state in states), key=lambda item: (-item[2], item[0], item[1]))

        came = funnel.total - sum(n for (state, _), n in cells.items()
                                  if state in (BLOCKED, STALE))
        passed = sum(n for (state, _), n in cells.items() if state == PASSED)
        excluded, waiting = picked(EXCLUDED), picked(WAITING)
        steps.append({
            "stage": stage, "in": came, "passed": passed,
            "excluded": excluded, "waiting": waiting,
            "stale": sum(n for (state, _), n in cells.items() if state == STALE),
            "adds_up": came == passed + sum(i[2] for i in excluded) + sum(i[2] for i in waiting),
            "follows": before is None or came == before,
        })
        before = passed
    return steps


def _add(table: Dict[tuple, list], key: tuple, article_id: str, raw: str, samples: int) -> None:
    cell = table.get(key)
    if cell is None:
        cell = table[key] = [0, []]
    cell[0] += 1
    if len(cell[1]) < samples:
        cell[1].append((article_id, raw))


def build_funnel(conn: sqlite3.Connection, *, prose: Optional[bool] = None, samples: int = 5,
                 collect: Optional[Callable[[str, tuple], bool]] = None,
                 ) -> Funnel:
    """Count every article's state at every stage.

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
    funnel = Funnel(graph=graph, samples=samples)
    names = {node.name for node in graph}
    funnel.expected = {stage: n for stage, n in conn.execute(_RECORDED_SQL) if stage in names}
    attempted: Dict[str, set] = defaultdict(set)
    for article_id, stage in conn.execute(_ATTEMPTED_SQL):
        attempted[article_id].add(stage)

    groups = _artifact_groups(conn)
    pending_group = next(groups, None)
    for article_id, day in _articles(conn):
        while pending_group is not None and pending_group[0] < article_id:
            funnel.stranded += len(pending_group[1])
            pending_group = next(groups, None)
        rows: List[_Row] = []
        if pending_group is not None and pending_group[0] == article_id:
            rows = pending_group[1]
            pending_group = next(groups, None)
        funnel.total += 1
        downloaded, judged, tried = _dimensions(rows)
        placed = place_article(graph, rows, ran=ran, attempted=attempted.get(article_id, set()))
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
                _add(funnel.sources, key, article_id, (row.error or "")[:300], samples)
                if collect and collect("source", key):
                    funnel.collected.append(
                        (article_id, f"{row.stage}:{row.source}", status, reason))
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
    return {
        "catalog": catalog,
        "generated": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "seconds": round(funnel.seconds, 1),
        "total": funnel.total,
        "samples": funnel.samples,
        "stages": [{"name": n.name, "upstream": n.upstream, "flag": n.flag} for n in funnel.graph],
        "prose": any(n.name == "prose" for n in funnel.graph),
        "flow": flow(funnel),
        "meanings": MEANINGS,
        "glossary": GLOSSARY,
        "excluded": EXCLUDED,
        "waiting": WAITING,
        "trunk": trunk(funnel.graph),
        "problems": funnel.reconcile(),
        "stranded": funnel.stranded,
        "strings": {kind: list(table) for kind, table in tables.items()},
        "cells": cells,
        "sources": sources,
    }


def render_html(funnel: Funnel, catalog: str) -> str:
    """One self-contained page: the data as JSON and the code inline, so it opens offline."""
    data = json.dumps(_encode(funnel, catalog), separators=(",", ":"))
    # A "</" inside the JSON would end the script element early.
    return _PAGE.replace("__DATA__", data.replace("</", "<\\/"))


def write_report(catalog_root: Path | str, out: Path | str, *, samples: int = 20,
                 prose: Optional[bool] = None) -> Funnel:
    conn = open_readonly(catalog_root)
    try:
        funnel = build_funnel(conn, prose=prose, samples=samples)
    finally:
        conn.close()
    Path(out).write_text(render_html(funnel, str(catalog_root)), encoding="utf-8")
    return funnel


def matching_ids(catalog_root: Path | str, *, stage: str, states: Sequence[str] = (),
                 reason: Optional[str] = None, exact_reason: Optional[str] = None,
                 sources: Sequence[str] = (), tried: Sequence[str] = (),
                 judged: Sequence[str] = (), day_from: Optional[str] = None,
                 day_to: Optional[str] = None, prose: Optional[bool] = None,
                 by_source: Optional[str] = None) -> List[Tuple[str, str, str, str]]:
    """Every article in one bucket, chosen as the page chooses it:
    (article_id, stage, state, reason).

    `reason` matches as a case-insensitive substring, as the reason box does,
    and `exact_reason` exactly, as a clicked reason does. `sources` (an ok
    download from), `tried` (any download recorded from) and `judged` each match
    any of their values; `day_from`/`day_to` bound the registration day. With
    `by_source`, the rows are that source's own download or extraction rows at
    `stage`, and `states` matches their status (ok, failed, permanent, skipped,
    taken_back), as the page's source table does."""
    needle = (reason or "").lower()

    def article_ok(dl: str, txt: str, day: str, tr: str) -> bool:
        return ((not sources or any(s in dl.split("+") for s in sources))
                and (not tried or any(s in tr.split("+") for s in tried))
                and (not judged or txt in judged)
                and (not day_from or day >= day_from) and (not day_to or day <= day_to))

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
                and article_ok(dl, txt, day, tr))

    conn = open_readonly(catalog_root)
    try:
        return build_funnel(conn, prose=prose, samples=0, collect=wanted).collected
    finally:
        conn.close()


class _NoPayloads(BlobStore):
    def get(self, digest: Optional[str]) -> Optional[Any]:
        raise PermissionError("the trace reads catalog metadata only, never a payload")


class _MetadataCatalog(Catalog):
    """The catalog over a read-only connection, refusing to open a payload."""

    def __init__(self, connection: sqlite3.Connection, root: Path) -> None:
        super().__init__(connection, root)
        self.blobs = _NoPayloads(self.root / "blobs")

    def payload(self, artifact):
        return self.blobs.get(artifact.blob if artifact else None)


def _versions(stage: str) -> Dict[str, Any]:
    """The version constants of the stage's code as it is now."""
    from ingestion_workflow.pipeline.stages import STAGE_TYPES

    module = sys.modules[STAGE_TYPES[stage].__module__]
    return {k: v for k, v in sorted(vars(module).items())
            if k.isupper() and k.endswith("VERSION") and isinstance(v, (str, int))}


def _brief(artifact) -> Optional[Dict[str, str]]:
    if artifact is None:
        return None
    return {"stage": artifact.stage, "source": artifact.source,
            "fingerprint": artifact.fingerprint, "written": artifact.updated_at}


def _read_plan(plan, existing: Dict[str, Any]) -> Dict[str, Any]:
    """What one article's plan says about freshness, per source where it can tell."""
    sources: Dict[str, Dict[str, Any]] = {}
    for work in plan.pending:
        old = existing.get(work.source)
        if old is None:
            why = "due: no record yet, so the stage would run now"
        elif old.status.value != "ok":
            why = "due: the stage would try again"
        elif old.fingerprint != work.fingerprint:
            why = (f"stale: built from fingerprint {old.fingerprint[:12] or '(none)'}; "
                   f"its inputs or version now give {work.fingerprint[:12]}, so it would rebuild")
        else:
            why = "stale: a refresh was asked for, or its stored payload is gone"
        sources[work.source] = {"freshness": why, "would_read": _brief(work.upstream)}
    said = []
    if plan.fresh:
        said.append("fresh: its fingerprint matches what the stage would build from its "
                    "inputs and code now")
    if plan.blocked:
        said.append("blocked: " + ("; ".join(plan.reasons) or "its input is not ready"))
    if plan.skipped:
        said.append("skipped: the plan has nothing for it to do")
    if plan.permanent:
        said.append("not retried: a permanent failure, or out of attempts")
    if plan.pending:
        said.append(f"would run for {len(plan.pending)} source(s)")
    return {"plan": "; ".join(said), "sources": sources}


def _plan_one(catalog_root: Path | str, article_id: str, names: Sequence[str],
              settings) -> Dict[str, Dict[str, Any]]:
    """Each stage's own `plan()` for one article, run against the catalog read-only.

    This is the scheduler's freshness test itself, so it cannot drift from it."""
    from ingestion_workflow.pipeline.scheduler import _plan_batch
    from ingestion_workflow.pipeline.stage import Context
    from ingestion_workflow.pipeline.stages import build

    conn = open_readonly(catalog_root)
    conn.row_factory = sqlite3.Row
    out: Dict[str, Dict[str, Any]] = {}
    try:
        catalog = _MetadataCatalog(conn, Path(catalog_root).resolve())
        ctx = Context(settings, catalog)
        ref = catalog.ref(article_id)
        runs = {stage.name: stage for stage in build(None, settings)}
        for name in names:
            stage = runs.get(name)
            if stage is None:
                out[name] = {"plan": "not planned: these settings do not run this stage"}
                continue
            try:
                plan = _plan_batch(ctx, stage, [ref])
                existing = catalog.artifacts([article_id], name).get(article_id, {})
                out[name] = _read_plan(plan, existing)
            except Exception as exc:  # report it, so one stage cannot hide the rest
                out[name] = {"plan": f"not known: {type(exc).__name__}: {exc}"[:300]}
    finally:
        conn.close()
    return out


def trace_article(catalog_root: Path | str, article_id: str, *, settings=None,
                  prose: Optional[bool] = None) -> Dict[str, Any]:
    """How one article got where it is: per stage its state (the same placement the
    funnel counts), and every artifact's path, status, reason, summary, when it was
    written, its fingerprint and attempts, and what it was built from. With the
    pipeline's `settings`, each stage's plan says whether it is fresh. Reads catalog
    rows only, never a payload."""
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
        placed = place_article(graph, rows, ran=ran, attempted=attempted)
        by_key = {(r.stage, r.source): r for r in rows}
        records: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for stage, source, status, fp, blob, summary, error, written in conn.execute(
                "SELECT stage, source, status, fingerprint, blob, summary, error, updated_at "
                "FROM artifacts WHERE article_id = ? ORDER BY stage, source", (article_id,)):
            path = Path(catalog_root).resolve() / "blobs" / blob[:2] / f"{blob}.json.gz" \
                if blob else None
            records[stage].append({
                "source": source, "status": status, "error": (error or "")[:600],
                "reason": _row_reason(by_key[(stage, source)]),
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
    names = [node.name for node in graph]
    plans = _plan_one(catalog_root, article_id, names, settings) if settings is not None else {}
    stages = []
    for node in graph:
        where = placed[node.name]
        mine = records.get(node.name, [])
        plan = plans.get(node.name, {})
        for rec in mine:
            rec.update(tries.get((node.name, rec["source"]), {"attempts": 0}))
            if plan:
                rec["freshness"] = plan.get("sources", {}).get(rec["source"], {}).get(
                    "freshness") or plan.get("plan", "")
        upstream = records.get(node.upstream or "", [])
        stages.append({
            "stage": node.name, "input": node.upstream, "state": where.state,
            "reason": where.reason, "meaning": MEANINGS[where.state], "records": mine,
            "built_from": [{"stage": node.upstream, "source": u["source"],
                            "status": u["status"], "fingerprint": u["fingerprint"],
                            "written": u["written"],
                            "newer": bool(mine) and u["written"] > max(r["written"] for r in mine)}
                           for u in upstream],
            "version": _versions(node.name),
            "plan": plan.get("plan", "") if plan else (
                "" if settings is not None else "pass the pipeline's --config to check freshness"),
        })
    return {"article_id": article_id, "registered": found[0], "stages": stages,
            "attempts_without_outcome": sorted(attempted)}


def serve(catalog_root: Path | str, *, host: str = "127.0.0.1", port: int = 8765,
          samples: int = 20, prose: Optional[bool] = None, settings=None) -> ThreadingHTTPServer:
    """The page, plus each article's trace on click, served from this machine. The
    caller runs `serve_forever()`."""
    conn = open_readonly(catalog_root)
    try:
        funnel = build_funnel(conn, prose=prose, samples=samples)
    finally:
        conn.close()
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
.arrow{margin:4px 0 4px 40px;color:#2e9d57;font-weight:600}
.ok{color:#1b7f3b}.bad{color:#a1261b;font-weight:600}
#trace table td{text-align:left;white-space:normal;vertical-align:top;max-width:none}
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
<h2>Flow along the upload path</h2>
<div class="tabs"><button id="tab-flow" class="on">Flow</button> <button id="tab-sankey">Sankey</button></div>
<div id="view-flow"><div class="muted" style="margin:6px 0">Top to bottom, one box per stage. <b>In</b> is every article whose input passed; each one then passed on, was left out (beside the box, by state and reason, in plain words), or is waiting. Click a box or a reason to list its articles; click an article for its trace.</div><div id="flow" class="flow"></div></div>
<div id="view-sankey" style="display:none"><div class="muted">Each column is a stage on the path to upload. The band to the right is what passed on; the drops below are what stayed behind, split by state and reason. Click a drop to list its articles.</div>
<div style="overflow-x:auto"><svg id="sankey"></svg></div>
<div class="legend" id="legend"></div></div>
<h2>Every stage</h2>
<div class="muted">Every article has one state at every stage, so each row adds up to the total. <b>entered</b> = total &minus; blocked &minus; stale. Click a number to list its articles; click a stage for its reasons.</div>
<div class="scroll"><table id="stages"></table></div>
<h2 id="reasons-title">Reasons</h2>
<div class="scroll"><table id="reasons"></table></div>
<h2>Sources</h2>
<div class="muted">Download and extraction per source (one row per article and source, so an article counts once under every source it was tried at), the source whose text triage judged, and how many articles with that source reached upload. A bad page is a download that served something other than the article (a paywall, an error page, an empty shell, a citation stub).</div>
<div class="scroll"><table id="sources"></table></div>
<h2 id="drill-title">Drill-down</h2>
<div id="drill"><div class="muted">Click a number above.</div></div>
<h2 id="trace-title">Article trace</h2>
<div id="trace" class="scroll"><div class="muted">Click an article in the drill-down.</div></div>
<script id="data" type="application/json">__DATA__</script>
<script>
"use strict";
const D=JSON.parse(document.getElementById("data").textContent);
const S=D.strings, STATES=S.state, STAGES=D.stages.map(s=>s.name);
const COLOR={passed:"#2e9d57",filtered:"#8a9bb3",failed:"#d0453a",permanent:"#8c1d18",taken_back:"#8a5cc7",held:"#e08a1e",refused:"#b5482a",blocked:"#c9ced8",pending:"#e6c229",not_attempted:"#f2e6a6",stale:"#a0703c",unaccounted:"#e3249b",ok:"#2e9d57",skipped:"#8a9bb3"};
const DROPS=["filtered","failed","permanent","taken_back","held","refused","pending","not_attempted","unaccounted"];
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
function totals(cs){const t={};STAGES.forEach(s=>{t[s]={};STATES.forEach(x=>t[s][x]=0)});let all=0;
  cs.forEach(c=>{t[S.stage[c[0]]][STATES[c[1]]]+=c[6]});
  const first=STAGES[0];STATES.forEach(x=>all+=t[first][x]);return [t,all]}
function sankey(cs){
  const [t,all]=totals(cs),chain=D.trunk,svg=document.getElementById("sankey");
  const colW=150,barW=14,top=24,trunkH=220,scale=all?trunkH/all:0,W=chain.length*colW+260;
  const drops=chain.map(st=>{const m={};cs.forEach(c=>{if(S.stage[c[0]]!==st)return;const s=STATES[c[1]];if(!DROPS.includes(s))return;const k=s+"\u0001"+S.reason[c[2]];m[k]=(m[k]||0)+c[6]});
    let e=Object.entries(m).sort((a,b)=>b[1]-a[1]);if(e.length>6){const rest=e.slice(5).reduce((a,b)=>a+b[1],0);e=e.slice(0,5).concat([["other\u0001(more reasons, see table)",rest]])}return e});
  const maxDrops=Math.max(1,...drops.map(d=>d.length));const H=top+trunkH+40+maxDrops*30+20;
  let g=`<svg xmlns="http://www.w3.org/2000/svg" width="${W}" height="${H}">`;
  const entered=st=>all-t[st].blocked-t[st].stale;
  chain.forEach((st,i)=>{const x=20+i*colW,inn=entered(st),h=Math.max(1,inn*scale),y=top;
    g+=`<rect x="${x}" y="${y}" width="${barW}" height="${h}" fill="#3b4a66"><title>${st}: ${fmt(inn)} entered</title></rect>`;
    g+=`<text x="${x}" y="${top-8}" font-weight="600">${st}</text><text x="${x+barW+4}" y="${top+12}">${fmt(inn)}</text>`;
    if(t[st].stale)g+=`<text x="${x+barW+4}" y="${top+26}" fill="#a0703c">+${fmt(t[st].stale)} stale</text>`;
    const pass=t[st].passed,ph=pass*scale;
    if(i<chain.length-1){const nx=20+(i+1)*colW,nh=Math.max(1,entered(chain[i+1])*scale);
      g+=`<path d="M${x+barW},${y} C${x+barW+60},${y} ${nx-60},${y} ${nx},${y} L${nx},${y+nh} C${nx-60},${y+nh} ${x+barW+60},${y+ph} ${x+barW},${y+ph} Z" fill="#2e9d57" opacity=".35"><title>${fmt(pass)} passed ${st}</title></path>`}
    else g+=`<text x="${x+barW+4}" y="${top+40}" fill="#2e9d57" font-weight="600">${fmt(pass)} passed</text>`;
    let yy=y+ph,dy=top+trunkH+40;
    drops[i].forEach(([k,n])=>{const [state,reason]=k.split("\u0001"),dh=Math.max(1.5,n*scale);
      g+=`<path d="M${x+barW},${yy} C${x+barW+40},${yy} ${x+20},${dy} ${x+40},${dy} L${x+40},${dy+dh} C${x+20},${dy+dh} ${x+barW+40},${yy+n*scale} ${x+barW},${yy+n*scale} Z" fill="${COLOR[state]||"#999"}" opacity=".5"/>`;
      g+=`<g style="cursor:pointer" data-stage="${esc(st)}" data-state="${esc(state)}" data-reason="${esc(reason)}"><rect x="${x+40}" y="${dy}" width="6" height="${dh}" fill="${COLOR[state]||"#999"}"/><text x="${x+50}" y="${dy+9}">${fmt(n)} ${esc(state)}</text><text x="${x+50}" y="${dy+21}" class="muted">${esc(reason.length>24?reason.slice(0,23)+"…":reason)}</text><title>${esc(st)} ${esc(state)}: ${esc(reason)} (${fmt(n)})</title></g>`;
      yy+=n*scale;dy+=Math.max(30,dh+18)});
  });
  svg.outerHTML=g.replace("<svg ",`<svg id="sankey" `)+"</svg>";
  document.querySelectorAll("#sankey g[data-stage]").forEach(el=>el.onclick=()=>{const r=el.dataset.reason;drill({stage:el.dataset.stage,state:el.dataset.state,reason:r.startsWith("(more reasons")?null:r})});
  document.getElementById("legend").innerHTML=STATES.map(s=>`<span><i style="background:${COLOR[s]}"></i>${s}</span>`).join("");
}
function flowOf(cs){
  const by={};cs.forEach(c=>{const st=S.stage[c[0]],k=c[1]+"|"+c[2];(by[st]=by[st]||{})[k]=((by[st]||{})[k]||0)+c[6]});
  const [,all]=totals(cs);let before=null;
  return D.trunk.map(st=>{const m=by[st]||{},E=Object.entries(m).map(([k,v])=>{const [x,r]=k.split("|").map(Number);return [STATES[x],S.reason[r],v]});
    const sum=xs=>xs.reduce((a,i)=>a+i[2],0),pick=ss=>E.filter(i=>ss.includes(i[0])).sort((a,b)=>b[2]-a[2]||(a[0]+a[1]<b[0]+b[1]?-1:1));
    const came=all-sum(pick(["blocked","stale"])),passed=sum(pick(["passed"])),excluded=pick(D.excluded),waiting=pick(D.waiting);
    const step={stage:st,in:came,passed,excluded,waiting,stale:sum(pick(["stale"])),adds_up:came===passed+sum(excluded)+sum(waiting),follows:before===null||came===before};
    before=passed;return step})}
function meaning(state,reason){const g=D.glossary.find(([k])=>reason.includes(k));return g?g[1]:D.meanings[state]}
function filtered(){return F.dl.size||F.tried.size||F.text.size||F.from||F.to}
function flowView(cs){
  const steps=flowOf(cs),sum=xs=>xs.reduce((a,i)=>a+i[2],0);
  const same=!filtered()&&JSON.stringify(steps.map(x=>[x.in,x.passed,sum(x.excluded),sum(x.waiting)]))===JSON.stringify(D.flow.map(x=>[x.in,x.passed,sum(x.excluded),sum(x.waiting)]));
  const li=(st,[state,reason,n])=>`<li data-stage="${esc(st)}" data-state="${esc(state)}" data-reason="${esc(reason)}"><span style="color:${COLOR[state]}">${esc(state)}</span><span>${esc(reason||"(no reason)")}</span><span class="n">${fmt(n)}</span><span class="mean">${esc(meaning(state,reason))}</span></li>`;
  const list=(st,items)=>items.slice(0,8).map(i=>li(st,i)).join("")+(items.length>8?`<details><summary class="muted">${items.length-8} more reasons</summary><ul>${items.slice(8).map(i=>li(st,i)).join("")}</ul></details>`:"");
  let h=filtered()?`<div class="muted">Filtered: the counts are recounted in the browser for the selected articles.</div>`:`<div class="${same?"ok":"bad"}">${same?"✓ the same numbers as <code>ingest report funnel</code> prints":"✗ differs from what <code>ingest report funnel</code> counted"}</div>`;
  steps.forEach((x,i)=>{const ex=sum(x.excluded),wt=sum(x.waiting);
    h+=`<div class="step"><div class="box main" data-stage="${esc(x.stage)}"><div><b>${esc(x.stage)}</b> <span class="muted">${i?"input: "+esc(D.trunk[i-1])+" passed":"every live article"}</span></div><div class="big">${fmt(x.in)} in</div>`+
      `<div>${fmt(x.passed)} passed &middot; ${fmt(ex)} left out &middot; ${fmt(wt)} waiting</div>`+
      `<div class="${x.adds_up?"ok":"bad"}">in = passed + left out + waiting: ${fmt(x.in)} = ${fmt(x.passed)} + ${fmt(ex)} + ${fmt(wt)} ${x.adds_up?"✓":"✗"}</div>`+
      (i?`<div class="${x.follows?"ok":"bad"}">in = ${esc(D.trunk[i-1])} passed ${x.follows?"✓":"✗"}</div>`:"")+
      (x.stale?`<div class="muted" title="${esc(D.meanings.stale)}">+${fmt(x.stale)} stale records (not counted in)</div>`:"")+`</div>`+
      `<div class="box excl"><b>Left out: ${fmt(ex)}</b><ul>${list(x.stage,x.excluded)}</ul>${wt?`<b>Waiting: ${fmt(wt)}</b><ul>${list(x.stage,x.waiting)}</ul>`:""}</div></div>`;
    if(i<steps.length-1)h+=`<div class="arrow">&darr; ${fmt(x.passed)} passed on to ${esc(D.trunk[i+1])}</div>`});
  const el=document.getElementById("flow");el.innerHTML=h;
  el.querySelectorAll(".box.main").forEach(b=>b.onclick=()=>drill({stage:b.dataset.stage,states:STATES.filter(s=>!["blocked","stale"].includes(s))}));
  el.querySelectorAll("li[data-state]").forEach(e=>e.onclick=ev=>{ev.stopPropagation();drill({stage:e.dataset.stage,state:e.dataset.state,reason:e.dataset.reason})});
}
function stageTable(cs){
  const [t,all]=totals(cs);const show=STATES.filter(s=>STAGES.some(st=>t[st][s]));
  let h=`<tr><th>stage</th><th>input</th><th>entered</th>${show.map(s=>`<th style="color:${COLOR[s]}">${s}</th>`).join("")}<th>sum</th><th>share of entered</th></tr>`;
  D.stages.forEach(st=>{const r=t[st.name],sum=STATES.reduce((a,s)=>a+r[s],0),inn=all-r.blocked-r.stale;
    const bar=inn?`<div class="bar">${STATES.filter(s=>!["blocked","stale"].includes(s)).map(s=>`<span title="${s} ${fmt(r[s])}" style="width:${100*r[s]/inn}%;background:${COLOR[s]}"></span>`).join("")}</div>`:"";
    h+=`<tr><td class="click" data-reasons="${st.name}"><b>${st.name}</b>${D.trunk.includes(st.name)?"":' <span class="muted">(side)</span>'}</td><td class="muted">${st.upstream||"&mdash;"}${st.name==="triage"?" (attempted)":""}</td><td>${fmt(inn)}</td>`+
      show.map(s=>`<td class="click${s==="unaccounted"&&r[s]?" bad":""}" data-stage="${st.name}" data-state="${s}" style="${!F.state.size||F.state.has(s)?"":"opacity:.3"}">${r[s]?fmt(r[s]):""}</td>`).join("")+
      `<td class="${sum===all?"ok":"bad"}" title="${sum===all?"reconciles":"does not add up"}">${fmt(sum)} ${sum===all?"✓":"✗"}</td><td>${bar}</td></tr>`});
  const el=document.getElementById("stages");el.innerHTML=h;
  el.querySelectorAll("td[data-state]").forEach(td=>td.onclick=()=>drill({stage:td.dataset.stage,state:td.dataset.state}));
  el.querySelectorAll("td[data-reasons]").forEach(td=>td.onclick=()=>{CUR=td.dataset.reasons;reasons(cells())});
}
let CUR=null;
function reasons(cs){
  const st=CUR||D.trunk[0];document.getElementById("reasons-title").textContent=`Reasons at ${st}`;
  const m={};cs.forEach(c=>{if(S.stage[c[0]]!==st||!bucketOk(c[1],c[2]))return;const k=c[1]+"|"+c[2];m[k]=(m[k]||0)+c[6]});
  const rows=Object.entries(m).sort((a,b)=>b[1]-a[1]);
  document.getElementById("reasons").innerHTML=`<tr><th>state</th><th>reason</th><th>articles</th></tr>`+rows.map(([k,n])=>{const [s,r]=k.split("|").map(Number);
    return `<tr><td style="color:${COLOR[STATES[s]]}">${STATES[s]}</td><td class="click" style="text-align:left" data-s="${s}" data-r="${r}">${esc(S.reason[r]||"—")}</td><td>${fmt(n)}</td></tr>`}).join("");
  document.querySelectorAll("#reasons td[data-r]").forEach(td=>td.onclick=()=>drill({stage:st,state:STATES[+td.dataset.s],reason:S.reason[+td.dataset.r]}));
}
function sourceTable(cs){
  const src=D.sources.filter(c=>articleOk(c[4],c[5],c[6],c[9]));const T={};
  const add=(s,k,n)=>{T[s]=T[s]||{};T[s][k]=(T[s][k]||0)+n};
  src.forEach(c=>add(S.source[c[1]],S.stage[c[0]]+":"+S.status[c[2]],c[7]));
  const [t]=totals(cs);const up=D.trunk[D.trunk.length-1]==="sync"?"upload":D.trunk[D.trunk.length-1];
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
  if(o.reason!=null)c+=` --exact-reason ${q(o.reason)}`;if(F.reason)c+=` --reason ${q(F.reason)}`;return c}
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
function drill(o){
  let total=0;const rows=[],want=o.states||(o.state?[o.state]:null);
  cells().forEach(c=>{if(S.stage[c[0]]!==o.stage||want&&!want.includes(STATES[c[1]]))return;if(o.reason!=null&&S.reason[c[2]]!==o.reason)return;if(!bucketOk(c[1],c[2]))return;
    total+=c[6];c[7].forEach(([a,r])=>rows.push([S.id[a],o.stage,STATES[c[1]],S.reason[c[2]],S.raw[r],S.dl[c[3]],S.text[c[4]],S.day[c[5]]]))});
  showDrill(`${o.stage} / ${o.states?"in":o.state||"any"}${o.reason?" / "+o.reason:""}`,rows,total,cliCmd(o));
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
    `<table><tr><th>stage</th><th>state</th><th>reason</th><th>raw</th></tr>${known.map(([st,s,r,raw])=>`<tr><td>${esc(st)}</td><td style="color:${COLOR[s]||"inherit"}">${esc(s||"(not sampled)")}</td><td>${esc(r)}</td><td>${esc(raw)}</td></tr>`).join("")}</table>`;
  document.getElementById("trace-title").scrollIntoView({behavior:"smooth"});
  if(!location.protocol.startsWith("http"))return;
  fetch("trace?id="+encodeURIComponent(id)).then(r=>r.json()).then(t=>{
    if(t.error){el.innerHTML=`<div class="bad">${esc(t.error)}</div>`;return}
    const kv=o=>Object.entries(o||{}).map(([k,v])=>`${esc(k)}=${esc(typeof v==="object"?JSON.stringify(v):v)}`).join(", ");
    el.innerHTML=`<div class="muted">Registered ${esc(t.registered)}. From the catalog rows only; no payload is opened.</div><table><tr><th>stage</th><th>state</th><th>records</th><th>built from</th><th>freshness</th></tr>`+
      t.stages.map(x=>`<tr><td><b>${esc(x.stage)}</b><div class="muted">code: ${kv(x.version)||"&mdash;"}</div></td><td><span style="color:${COLOR[x.state]}">${esc(x.state)}</span> ${esc(x.reason)}<div class="muted">${esc(x.meaning)}</div></td>`+
        `<td>${x.records.map(r=>`<div><b>${esc(r.source||"(one)")}</b> ${esc(r.status)}${r.reason?": "+esc(r.reason):""}; written ${esc(r.written)}; ${r.attempts} attempt(s)${r.last?", last "+esc(r.last):""}<div class="muted">fingerprint ${esc(r.fingerprint||"(none)")}; ${kv(r.summary)}</div>${r.path?`<div class="path">${esc(r.path)} ${r.on_disk?"":"(missing)"}</div>`:""}${r.error?`<div class="muted">${esc(r.error)}</div>`:""}</div>`).join("")||"&mdash;"}</td>`+
        `<td>${x.built_from.map(u=>`<div>${esc(u.stage)}${u.source?":"+esc(u.source):""} ${esc(u.status)}, ${esc(u.fingerprint.slice(0,12)||"(none)")}, written ${esc(u.written)}${u.newer?' <span class="bad">rewritten after this record</span>':""}</div>`).join("")||"&mdash;"}</td>`+
        `<td>${esc(x.plan||"")}${x.records.filter(r=>r.freshness&&r.freshness!==x.plan).map(r=>`<div>${esc(r.source)}: ${esc(r.freshness)}</div>`).join("")}</td></tr>`).join("")+`</table>`});
}
function render(){const cs=cells();flowView(cs);sankey(cs);stageTable(cs);reasons(cs);sourceTable(cs)}
document.getElementById("tab-flow").onclick=()=>{document.getElementById("view-flow").style.display="";document.getElementById("view-sankey").style.display="none";document.getElementById("tab-flow").className="on";document.getElementById("tab-sankey").className=""};
document.getElementById("tab-sankey").onclick=()=>{document.getElementById("view-flow").style.display="none";document.getElementById("view-sankey").style.display="";document.getElementById("tab-sankey").className="on";document.getElementById("tab-flow").className=""};
document.getElementById("meta").innerHTML=`${fmt(D.total)} live articles in <code>${esc(D.catalog)}</code>; built ${D.generated} in ${D.seconds}s; stage graph ${D.prose?"with":"without"} the prose stages.`;
document.getElementById("filter-help").innerHTML=`<b>Filters.</b> These select articles, so every stage still adds up: <b>Downloaded from</b> keeps articles with an ok download from any ticked source (an article whose every download failed has source <i>none</i>, so ticking a source hides its download failures); <b>Download tried at</b> keeps articles with any download recorded from a ticked source, failed or not (use it for losses at download by source); <b>Judged text source</b> is the source whose text triage, or else passages, read; <b>Registered</b> is the day the article entered the catalog. These narrow the tables and the drill-down only: <b>State</b>, and <b>Reason contains</b> (a case-insensitive substring; a clicked reason matches exactly). The source table always lists every source tried; its numbers follow the article filters.`;
document.getElementById("problems").innerHTML=D.problems.length?`<div class="warn">Does not reconcile:</div><ul>${D.problems.map(p=>`<li>${esc(p)}</li>`).join("")}</ul>`:`<div class="muted">Reconciles: every stage accounts for every article, and every article with a record at a stage was placed from that record.</div>`;
filters();render();
</script></body></html>
"""  # noqa: E501 -- the page is minified HTML and JS, not Python
