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
blocked       never ran here because its input did not pass; the reason names
              the stage where the article dropped out and why
pending       its input passed and nothing is recorded here yet
not_attempted its input passed and this stage has never run on the catalog
stale         an artifact exists although its input did not pass, so it was made
              from an input that has since gone or under an older stage graph
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
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Callable, Dict, Iterator, List, Optional, Sequence, Tuple

from ingestion_workflow.pipeline.stage import HELD_FOR_REVIEW, NO_TEXT

PASSED = "passed"
FILTERED = "filtered"
FAILED = "failed"
PERMANENT = "permanent"
TAKEN_BACK = "taken_back"
HELD = "held"
BLOCKED = "blocked"
PENDING = "pending"
NOT_ATTEMPTED = "not_attempted"
STALE = "stale"
UNACCOUNTED = "unaccounted"

STATES = (PASSED, FILTERED, FAILED, PERMANENT, TAKEN_BACK, HELD, BLOCKED, PENDING,
          NOT_ATTEMPTED, STALE, UNACCOUNTED)

#: States in which the stage did not see the article: its input never passed.
NOT_ENTERED = (BLOCKED, STALE)

#: A stage that runs per source keeps one artifact per source; the article's
#: state is the best any source reached, as `Catalog.status_counts` counts it.
_RANK = {"ok": 0, "failed": 1, "permanent": 2, "skipped": 3}

#: Triage waits for metadata to have been attempted, not to have succeeded
#: (`TriageStage.plan`): plenty of articles have no metadata to find.
_INPUT_ON_ATTEMPT = {"triage"}

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


@dataclass
class Placed:
    """One article's state at one stage."""

    state: str
    reason: str = ""
    raw: str = ""


def _truthy(value: object) -> bool:
    return value not in (None, 0, "0", "", False, "false")


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
    reason = " + ".join(sorted({categorize(r.error) for r in failures})) or "no reason recorded"
    raw = "; ".join((f"{r.source}: " if r.source else "") + (r.error or "")[:200]
                    for r in failures)
    return Placed(FAILED if best.status == "failed" else PERMANENT, reason, raw[:600])


def _origin(stage: str, placed: Placed) -> Placed:
    """What a stage downstream of `placed` reports as the reason it was blocked."""
    if placed.state == BLOCKED:
        return Placed(BLOCKED, placed.reason, placed.raw)
    why = f"{stage} {placed.state}" + (f": {placed.reason}" if placed.reason else "")
    return Placed(BLOCKED, why, placed.raw)


def place_article(graph: Sequence[StageNode], rows: Sequence[_Row], *, ran: set,
                  attempted: set) -> Dict[str, Placed]:
    """Every stage's state for one article. `attempted` holds the stages with an
    attempt but no artifact; `ran` the stages with any artifact in the catalog."""
    by_stage: Dict[str, List[_Row]] = defaultdict(list)
    for row in rows:
        by_stage[row.stage].append(row)
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
        if own:
            if entered:
                out[node.name] = _own_state(node, own)
            elif all(r.status != "ok" and r.error == NO_TEXT for r in own):
                # The record a stage leaves when its input was taken back: the
                # article dropped out upstream, so it counts as blocked from there.
                out[node.name] = _origin(node.upstream, before)
            else:
                out[node.name] = Placed(STALE, f"input {node.upstream} is {before.state}",
                                        before.reason)
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
            out[node.name] = Placed(PENDING, "input ready, not run yet")
    return out


def _resolve_has_input(by_stage: Dict[str, List[_Row]]) -> bool:
    prose = [r for r in by_stage.get("prose", []) if r.status == "ok"]
    tables = [r for r in by_stage.get("analyses", []) if r.status == "ok"]
    read = prose and (_truthy(prose[0].coordinates) or _truthy(prose[0].analyses))
    return bool(read or (tables and _truthy(tables[0].tables)))


def _dimensions(rows: Sequence[_Row]) -> Tuple[str, str]:
    """(sources with a download, source of the text triage or passages judged)."""
    downloaded = sorted({r.source for r in rows if r.stage == "download" and r.status == "ok"})
    judged = ([r.text_source for r in rows if r.stage == "triage" and r.text_source]
              or [r.text_source for r in rows if r.stage == "passages" and r.text_source])
    return "+".join(downloaded) or "none", (judged[0] if judged else "none")


_ROWS_SQL = """
SELECT a.article_id, a.stage, a.source, a.status, substr(a.error, 1, 400),
       json_extract(a.summary, '$.passed'), json_extract(a.summary, '$.tables'),
       json_extract(a.summary, '$.reason'), json_extract(a.summary, '$.source'),
       json_extract(a.summary, '$.coordinates'), json_extract(a.summary, '$.analyses'),
       json_extract(a.summary, '$.retracted')
FROM artifacts a ORDER BY a.article_id
"""

_ATTEMPTED_SQL = """
SELECT DISTINCT t.article_id, t.stage FROM attempts t
WHERE NOT EXISTS (SELECT 1 FROM artifacts a
                  WHERE a.article_id = t.article_id AND a.stage = t.stage)
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
    #: (stage, state, reason, downloaded, judged, day) -> [count, [(article_id, raw)]]
    cells: Dict[tuple, list] = field(default_factory=dict)
    #: (stage, source, status, reason, downloaded, judged, day) -> [count, [(article_id, raw)]]
    sources: Dict[tuple, list] = field(default_factory=dict)
    #: Artifacts whose article is not live (merged away or never registered).
    stranded: int = 0
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
        and every trunk stage enters exactly what the stage before it passed."""
        problems = []
        totals = self.stage_totals()
        for stage, counts in totals.items():
            if sum(counts.values()) != self.total:
                problems.append(f"{stage}: states sum to {sum(counts.values()):,}, "
                                f"not {self.total:,}")
            if counts[UNACCOUNTED]:
                problems.append(f"{stage}: {counts[UNACCOUNTED]:,} unaccounted")
        chain = trunk(self.graph)
        for before, after in zip(chain, chain[1:]):
            entered = self.total - sum(totals[after][s] for s in NOT_ENTERED)
            if after not in _INPUT_ON_ATTEMPT and entered != totals[before][PASSED]:
                problems.append(f"{after} entered {entered:,} but {before} passed "
                                f"{totals[before][PASSED]:,}")
        if self.stranded:
            problems.append(f"{self.stranded:,} artifacts belong to no live article")
        return problems


def _add(table: Dict[tuple, list], key: tuple, article_id: str, raw: str, samples: int) -> None:
    cell = table.get(key)
    if cell is None:
        cell = table[key] = [0, []]
    cell[0] += 1
    if len(cell[1]) < samples:
        cell[1].append((article_id, raw))


def build_funnel(conn: sqlite3.Connection, *, prose: Optional[bool] = None, samples: int = 5,
                 collect: Optional[Callable[[str, str, str, str, str, str], bool]] = None,
                 ) -> Funnel:
    """Count every article's state at every stage.

    `prose` defaults to whether the catalog holds any prose-stage artifact.
    `collect(stage, state, reason, downloaded, judged, day)` picks articles to
    list in full, beyond the `samples` kept per cell.
    """
    started = time.monotonic()
    ran = {r[0] for r in conn.execute("SELECT DISTINCT stage FROM artifacts")}
    if prose is None:
        prose = bool(ran & {"passages", "prose", "resolve"})
    graph = stage_graph(prose)
    funnel = Funnel(graph=graph, samples=samples)
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
        downloaded, judged = _dimensions(rows)
        placed = place_article(graph, rows, ran=ran, attempted=attempted.get(article_id, set()))
        for stage, where in placed.items():
            _add(funnel.cells, (stage, where.state, where.reason, downloaded, judged, day),
                 article_id, where.raw, samples)
            if collect and collect(stage, where.state, where.reason, downloaded, judged, day):
                funnel.collected.append((article_id, stage, where.state, where.reason))
        for row in rows:
            if row.stage in ("download", "extract"):
                status = TAKEN_BACK if row.error == NO_TEXT else row.status
                reason = "" if row.status == "ok" else (
                    row.reason or categorize(row.error) if row.status == "skipped"
                    else categorize(row.error))
                key = (row.stage, row.source, status, reason, downloaded, judged, day)
                _add(funnel.sources, key, article_id, (row.error or "")[:300], samples)
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
        out = []
        for key, (count, sample) in source.items():
            out.append([ix(k, v) for k, v in zip(kinds, key)] + [
                count, [[ix("id", a), ix("raw", raw)] for a, raw in sample]])
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


def matching_ids(catalog_root: Path | str, *, stage: str, state: Optional[str] = None,
                 reason: Optional[str] = None, source: Optional[str] = None,
                 judged: Optional[str] = None) -> List[Tuple[str, str, str, str]]:
    """Every article in one bucket: (article_id, stage, state, reason).

    `reason` matches as a case-insensitive substring; `source` matches when the
    article has a download from that source."""
    needle = (reason or "").lower()

    def wanted(st, sta, rea, dl, txt, _day) -> bool:
        return (st == stage and (state is None or sta == state)
                and needle in rea.lower()
                and (source is None or source in dl.split("+"))
                and (judged is None or txt == judged))

    conn = open_readonly(catalog_root)
    try:
        return build_funnel(conn, samples=0, collect=wanted).collected
    finally:
        conn.close()


_PAGE = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
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
</style></head><body>
<h1>Ingestion funnel: downloaded to uploaded</h1>
<div id="meta" class="muted"></div>
<div id="problems"></div>
<div id="filters"></div>
<h2>Funnel along the upload path</h2>
<div class="muted">Each column is a stage on the path to upload. The band to the right is what passed on; the drops below are what stayed behind, split by state and reason. Click a drop to list its articles.</div>
<div style="overflow-x:auto"><svg id="sankey"></svg></div>
<div class="legend" id="legend"></div>
<h2>Every stage</h2>
<div class="muted">Every article has one state at every stage, so each row adds up to the total. <b>entered</b> = total &minus; blocked &minus; stale. Click a number to list its articles; click a stage for its reasons.</div>
<table id="stages"></table>
<h2 id="reasons-title">Reasons</h2>
<table id="reasons"></table>
<h2>Sources</h2>
<div class="muted">Download and extraction per source (one row per article and source), the source whose text triage judged, and how many articles with that source reached upload.</div>
<table id="sources"></table>
<h2 id="drill-title">Drill-down</h2>
<div id="drill"><div class="muted">Click a number above.</div></div>
<script id="data" type="application/json">__DATA__</script>
<script>
"use strict";
const D=JSON.parse(document.getElementById("data").textContent);
const S=D.strings, STATES=S.state, STAGES=D.stages.map(s=>s.name);
const COLOR={passed:"#2e9d57",filtered:"#8a9bb3",failed:"#d0453a",permanent:"#8c1d18",taken_back:"#8a5cc7",held:"#e08a1e",blocked:"#c9ced8",pending:"#e6c229",not_attempted:"#f2e6a6",stale:"#a0703c",unaccounted:"#e3249b",ok:"#2e9d57",skipped:"#8a9bb3"};
const DROPS=["filtered","failed","permanent","taken_back","held","pending","not_attempted","unaccounted"];
const fmt=n=>n.toLocaleString("en-US");
const esc=s=>String(s).replace(/[&<>"]/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));
const F={dl:new Set(),text:new Set(),from:"",to:"",state:new Set(),reason:""};
const dlSources=[...new Set(S.dl.flatMap(d=>d.split("+")))].sort();
function articleOk(dl,text,day){
  if(F.dl.size&&!S.dl[dl].split("+").some(s=>F.dl.has(s)))return false;
  if(F.text.size&&!F.text.has(S.text[text]))return false;
  const d=S.day[day];if(F.from&&d<F.from)return false;if(F.to&&d>F.to)return false;return true}
function bucketOk(state,reason){
  if(F.state.size&&!F.state.has(STATES[state]))return false;
  return !F.reason||S.reason[reason].toLowerCase().includes(F.reason.toLowerCase())}
function cells(){return D.cells.filter(c=>articleOk(c[3],c[4],c[5]))}
function chips(name,values,set){return `<div class="grp"><b>${name}</b><div>`+values.map(v=>`<label class="chip"><input type="checkbox" data-set="${set}" value="${esc(v)}">${esc(v)}</label>`).join("")+`</div></div>`}
function filters(){
  const days=[...S.day].sort();
  document.getElementById("filters").innerHTML=
    chips("Downloaded from (any of)",[...dlSources],"dl")+chips("Judged text source",[...S.text].sort(),"text")+
    `<div class="grp"><b>Registered in catalog</b><div><select id="from"><option value="">from</option>${days.map(d=>`<option>${d}</option>`).join("")}</select> <select id="to"><option value="">to</option>${days.map(d=>`<option>${d}</option>`).join("")}</select></div></div>`+
    chips("State (tables, drill-down)",STATES,"state")+
    `<div class="grp"><b>Reason contains</b><input id="reason" size="22" placeholder="e.g. open access"></div><div class="grp"><b>&nbsp;</b><button id="reset">Reset</button></div>`;
  document.querySelectorAll("#filters input[type=checkbox]").forEach(el=>el.onchange=()=>{const s=F[el.dataset.set];el.checked?s.add(el.value):s.delete(el.value);render()});
  document.getElementById("from").onchange=e=>{F.from=e.target.value;render()};
  document.getElementById("to").onchange=e=>{F.to=e.target.value;render()};
  document.getElementById("reason").oninput=e=>{F.reason=e.target.value;render()};
  document.getElementById("reset").onclick=()=>{F.dl.clear();F.text.clear();F.state.clear();F.from=F.to=F.reason="";filters();render()};
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
  const src=D.sources.filter(c=>articleOk(c[4],c[5],c[6]));const T={};
  const add=(s,k,n)=>{T[s]=T[s]||{};T[s][k]=(T[s][k]||0)+n};
  src.forEach(c=>add(S.source[c[1]],S.stage[c[0]]+":"+S.status[c[2]],c[7]));
  const [t]=totals(cs);const up=D.trunk[D.trunk.length-1]==="sync"?"upload":D.trunk[D.trunk.length-1];
  cs.forEach(c=>{const st=S.stage[c[0]];if(st==="triage"&&STATES[c[1]]!=="blocked")add(S.text[c[4]],"judged",c[6]);
    if(st===up&&STATES[c[1]]==="passed"){S.dl[c[3]].split("+").forEach(s=>add(s,"reached",c[6]));add(S.text[c[4]],"reached_judged",c[6])}});
  const cols=[["download:ok","downloaded"],["download:failed","dl failed"],["download:permanent","dl permanent"],["download:skipped","dl skipped"],["extract:ok","extracted"],["extract:failed","extract failed"],["extract:taken_back","extract no text"],["judged","text triage judged"],["reached","has a download, reached "+up],["reached_judged","judged text, reached "+up]];
  const names=Object.keys(T).filter(s=>s!=="none").sort().concat(T.none?["none"]:[]);
  document.getElementById("sources").innerHTML=`<tr><th>source</th>${cols.map(c=>`<th>${c[1]}</th>`).join("")}</tr>`+
    names.map(s=>`<tr><td>${esc(s)}</td>${cols.map(([k])=>{const n=(T[s]||{})[k]||0;const [stg,sta]=k.split(":");return `<td class="${sta?"click":""}" data-src="${esc(s)}" data-k="${k}">${n?fmt(n):""}</td>`}).join("")}</tr>`).join("");
  document.querySelectorAll("#sources td.click").forEach(td=>td.onclick=()=>{const [stg,sta]=td.dataset.k.split(":");drillSource(stg,td.dataset.src,sta)});
}
function cliCmd(o){let c=`ingest report funnel-ids --catalog ${JSON.stringify(D.catalog)} --stage ${o.stage}`;if(o.state)c+=` --state ${o.state}`;if(o.reason)c+=` --reason ${JSON.stringify(o.reason)}`;if(F.dl.size===1)c+=` --source ${[...F.dl][0]}`;if(F.text.size===1)c+=` --judged ${[...F.text][0]}`;return c}
function showDrill(title,rows,total,cmd){
  const ids=rows.map(r=>r[0]);
  document.getElementById("drill-title").textContent="Drill-down: "+title;
  document.getElementById("drill").innerHTML=`<div>${fmt(total)} articles; ${fmt(rows.length)} sampled here (up to ${D.samples} per bucket). <button id="copy">Copy ${fmt(ids.length)} ids</button></div>`+
   `<div class="muted" style="margin:6px 0">Every id: <code>${esc(cmd)}</code></div>`+
   `<table><tr><th>article</th><th>stage</th><th>state</th><th>reason</th><th>raw</th><th>downloaded from</th><th>judged text</th><th>registered</th></tr>`+
   rows.slice(0,1000).map(r=>`<tr>${r.map((v,i)=>`<td title="${esc(v)}">${esc(i===4&&v.length>140?v.slice(0,139)+"…":v)}</td>`).join("")}</tr>`).join("")+`</table>`;
  document.getElementById("copy").onclick=()=>{const txt=ids.join("\n");(navigator.clipboard?navigator.clipboard.writeText(txt):Promise.reject()).catch(()=>{const a=document.createElement("textarea");a.value=txt;document.body.appendChild(a);a.select();document.execCommand("copy");a.remove()})};
  document.getElementById("drill-title").scrollIntoView({behavior:"smooth"});
}
function drill(o){
  let total=0;const rows=[];
  cells().forEach(c=>{if(S.stage[c[0]]!==o.stage||STATES[c[1]]!==o.state&&o.state)return;if(o.reason!=null&&S.reason[c[2]]!==o.reason)return;if(!bucketOk(c[1],c[2]))return;
    total+=c[6];c[7].forEach(([a,r])=>rows.push([S.id[a],o.stage,STATES[c[1]],S.reason[c[2]],S.raw[r],S.dl[c[3]],S.text[c[4]],S.day[c[5]]]))});
  showDrill(`${o.stage} / ${o.state||"any"}${o.reason?" / "+o.reason:""}`,rows,total,cliCmd(o));
}
function drillSource(stage,source,state){
  let total=0;const rows=[];
  D.sources.forEach(c=>{if(!articleOk(c[4],c[5],c[6]))return;if(S.stage[c[0]]!==stage||S.source[c[1]]!==source||S.status[c[2]]!==state)return;
    total+=c[7];c[8].forEach(([a,r])=>rows.push([S.id[a],stage+":"+source,state,S.reason[c[3]]||"",S.raw[r],S.dl[c[4]],S.text[c[5]],S.day[c[6]]]))});
  showDrill(`${stage} from ${source} / ${state}`,rows,total,`ingest report funnel-ids --catalog ${JSON.stringify(D.catalog)} --stage ${stage} --source ${source}`);
}
function render(){const cs=cells();sankey(cs);stageTable(cs);reasons(cs);sourceTable(cs)}
document.getElementById("meta").innerHTML=`${fmt(D.total)} live articles in <code>${esc(D.catalog)}</code>; built ${D.generated} in ${D.seconds}s. Source, judged-text and registration-date filters select articles, so every stage still adds up; state and reason filters narrow the tables and drill-down.`;
document.getElementById("problems").innerHTML=D.problems.length?`<div class="warn">Does not reconcile:</div><ul>${D.problems.map(p=>`<li>${esc(p)}</li>`).join("")}</ul>`:`<div class="muted">Reconciles: every stage accounts for every article, and each stage on the upload path entered what the one before it passed.</div>`;
filters();render();
</script></body></html>
"""
