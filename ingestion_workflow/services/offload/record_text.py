"""The paper text a record request carries: the parse's text, less what is certain to hold
nothing for the record model.

Two things are left out, each only where that is certain:

- a paragraph that repeats an earlier one word for word. ACE writes some tables out
  several times (27% of ACE tables 3 or more times, 2026-11-20); the first copy stays.
- a reference list: from its heading to the next heading the paper keeps (figure legends,
  supporting information, an appendix, acknowledgements, the appended supplementary
  material...), and only when the lines between are citations. Only ACE and PDF texts
  hold their list (pubget, Elsevier and PMC texts leave it out); in both, the figure
  legends often follow it, so the list is never cut to the end of the text.

The text the parse and the record's `text_sha256` name is unchanged: this is what the
model is shown of it. A spec asks for it with `record_text` (`RECORD_TEXT`).

`trimmed-2` (`RECORD_TEXT_FIT`) is for the papers trimmed-1 leaves over a spec's input limit
(62 of the 2026-11-27 corpus over 108,000 tokens). In turn, until the request fits:
- the parts of a long thesis or book outside its reviewed ranges, never an analysis's own
  paragraph (`ranges_of`); a reviewed text with no record of its own is not sent;
- numeric supplementary data no analysis came from (`numeric_data_spans`): source-data
  sheets, per-subject values, matrices, which tokenize at four times prose;
- the paragraphs farthest from the analyses (`fit_text`).
Only the text the model reads is cut. The parse, every coordinate in it and every analysis the
prompt lists stay as they are; the listing carries each analysis's points and caption.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from functools import lru_cache
from pathlib import Path
from typing import List, Optional, Tuple

#: `RunSpec.record_text` for this text; "full" is the parse's text as it is.
RECORD_TEXT = "trimmed-1"

#: A copied block: `MIN_RUN` or more repeated paragraphs of `MIN_PARAGRAPH` characters or
#: more, `MIN_BLOCK` in all. A table body this long is left out when it repeats on its own.
MIN_RUN = 3
MIN_PARAGRAPH = 40
MIN_BLOCK = 300
MIN_TABLE_REPEAT = 200

_REF_HEADING = re.compile(
    r"^[#*\s]*(?:references?(?:\s+and\s+notes)?|bibliography|literature\s+cited|works\s+cited|"
    r"reference\s+list|cited\s+literature|supplementary\s+references)[*:\s]*$",
    re.I,
)

#: A heading after a reference list that starts what the paper keeps.
_KEEP_HEADING = re.compile(
    r"^[#*\s]*(?:figure\s+legends?|figure\s+captions?|figures?(?:\s+and\s+tables?)?|legends?|"
    r"tables?|supporting\s+information|supplementa\w*(?:\s+\S+){0,6}|electronic\s+supplementary"
    r"\s+material|appendix(?:\s+\S+){0,6}|appendices|acknowledge?ments?|funding(?:\s+\S+){0,3}|"
    r"author\s+contributions?|contributors|conflicts?\s+of\s+interests?|competing\s+interests?|"
    r"declarations?(?:\s+\S+){0,4}|disclosures?|data(?:\s+and\s+\S+)?\s+availability(?:\s+\S+)?|"
    r"additional\s+information|abbreviations|footnotes?|notes|ethics(?:\s+\S+){0,3}|"
    r"online\s+methods|extended\s+data(?:\s+\S+){0,4})[*:.\s]*$",
    re.I,
)

#: The first line of a caption, where a list runs straight into the legends.
_CAPTION_START = re.compile(
    r"^[#*\s]*(?:fig(?:ure)?\.?|table|supplementa\w+\s+(?:fig\w*|table))"
    r"\s*s?\d+[a-z]?\b",
    re.I,
)

_YEAR = re.compile(r"\b(?:1[89]\d\d|20\d\d)[a-z]?\b")

#: The link labels ACE keeps of a publisher's reference list.
_LINK_LABEL = re.compile(
    r"^(?:article|pubmed(?:\s+central)?|cas|google\s+scholar|crossref|cross\s*ref|"
    r"web\s+of\s+science|"
    r"isi|scopus|medline|pmc(?:\s+free\s+article)?|full\s+text|abstract|view\s+article|"
    r"find\s+this\s+article\s+online|download\s+references|math\s*scinet|\[?\d+\]?\.?)$",
    re.I,
)

#: Of a reference list's non-blank lines, the share that must look like citations, and the
#: share that must name a year; and the fewest lines it may have.
CITATION_SHARE = 0.85
YEAR_SHARE = 0.4
MIN_LINES = 5
#: A list split into fields (ACE: each author on a line) names few years for its lines: one
#: naming at least `LONG_LIST_YEARS` passes on `LONG_LIST_SHARE` of its lines like citations.
LONG_LIST_YEARS = 20
LONG_LIST_SHARE = 0.9


@dataclass
class Trim:
    """What was left out of one text."""

    repeats: int = 0
    repeat_chars: int = 0
    #: (start, end, lines naming a year) of each reference list left out, in the text it was
    #: cut from (the text after `drop_repeats`).
    lists: List[Tuple[int, int, int]] = field(default_factory=list)


def _heading(para: str) -> bool:
    return para.lstrip().startswith("#")


def _table_body(para: str) -> bool:
    lines = [line for line in para.split("\n") if line.strip()]
    return (
        len(para) >= MIN_TABLE_REPEAT
        and len(lines) >= 2
        and sum("\t" in line or line.lstrip().startswith("|") for line in lines)
        >= 0.8 * len(lines)
    )


def drop_repeats(text: str, trim: Trim) -> str:
    """`text` without the copies of what it holds twice: a table body (rows of cells) that
    repeats an earlier one, or a block of `MIN_RUN` or more paragraphs of `MIN_PARAGRAPH`
    characters or more, `MIN_BLOCK` in all, each repeating an earlier one (a table copied
    whole: its caption, rows and footnotes). Short paragraphs are kept and do not end a
    block: a label ("Table 1"), or a citation a text broke out of its sentence ("Smith et al.,
    2011"), reads wrongly without its neighbours. A heading is never left out, nor a lone
    repeated paragraph: a subsection title or a footnote repeated under another table says
    something where it stands."""
    parts = re.split(r"(\n[ \t]*\n)", text)
    paras = parts[0::2]
    gaps = parts[1::2] + [""]
    keys = [" ".join(para.split()) for para in paras]
    seen: set = set()
    repeated = []
    for para, key in zip(paras, keys):
        repeated.append(len(key) >= MIN_PARAGRAPH and key in seen and not _heading(para))
        seen.add(key)
    drop = [False] * len(paras)
    i = 0
    while i < len(paras):
        if not repeated[i]:
            i += 1
            continue
        j = i
        while j < len(paras) and (
            repeated[j] or len(keys[j]) < MIN_PARAGRAPH and not _heading(paras[j])
        ):
            j += 1
        block = [k for k in range(i, j) if repeated[k]]
        whole = len(block) >= MIN_RUN and sum(len(keys[k]) for k in block) >= MIN_BLOCK
        for k in block:
            drop[k] = whole or _table_body(paras[k])
        i = j
    out = []
    for para, gap, gone in zip(paras, gaps, drop):
        if gone:
            trim.repeats += 1
            trim.repeat_chars += len(para) + len(gap)
        else:
            out.append(para + gap)
    return "".join(out)


def _prose(line: str) -> bool:
    """A paragraph, not an entry: long and naming no year, or longer than any entry runs."""
    s = line.strip()
    return (len(s) > 200 and not _YEAR.search(s)) or len(s) > 600


def _looks_cited(body: List[str]) -> Tuple[bool, int]:
    """Whether the lines between a reference heading and the list's end are a reference list,
    and how many name a year. A line is like a citation when it or the line after names a
    year (an entry whose title has a line of its own), is a link label, or is short (an
    author or journal on its own line); at least `CITATION_SHARE` must be. The link labels
    aside, `YEAR_SHARE` of the lines name a year, or the list is long and all but a few
    lines are short (a list split into fields)."""
    years = [bool(_YEAR.search(line)) for line in body]
    labels = [bool(_LINK_LABEL.match(line.strip())) for line in body]
    cited = sum(
        years[k] or labels[k] or len(body[k].strip()) <= 60 or (k + 1 < len(body) and years[k + 1])
        for k in range(len(body))
    )
    entries = len(body) - sum(labels)
    dated = sum(years)
    ok = (
        len(body) >= MIN_LINES
        and cited >= CITATION_SHARE * len(body)
        and entries > 0
        and (
            dated >= YEAR_SHARE * entries
            or (dated >= LONG_LIST_YEARS and cited >= LONG_LIST_SHARE * len(body))
        )
    )
    return ok, dated


def _list_end(lines: List[str], start: int) -> int:
    """The index of the first line after the list whose heading is `lines[start]`: the next
    heading the paper keeps, the first line of a caption, or two paragraphs of prose."""
    filled = [k for k in range(start + 1, len(lines)) if lines[k].strip()]
    for n, k in enumerate(filled):
        s = lines[k].strip()
        if _KEEP_HEADING.match(s) or _CAPTION_START.match(s) or _REF_HEADING.match(s):
            return k
        if _prose(s) and n + 1 < len(filled) and _prose(lines[filled[n + 1]]):
            return k
    return len(lines)


def drop_reference_lists(text: str, trim: Trim) -> str:
    """`text` with each reference list in it replaced by a line saying it is left out. A list
    is cut only when its lines are citations (`_looks_cited`); otherwise it is kept whole."""
    lines = text.split("\n")
    starts = [0]
    for line in lines:
        starts.append(starts[-1] + len(line) + 1)
    keep: List[str] = []
    i = 0
    while i < len(lines):
        if _REF_HEADING.match(lines[i].strip()):
            end = _list_end(lines, i)
            body = [line for line in lines[i + 1 : end] if line.strip()]
            ok, years = _looks_cited(body)
            if ok:
                trim.lists.append((starts[i], min(starts[end], len(text)), years))
                keep += [
                    lines[i].strip(),
                    f"[The reference list ({len(body):,} lines) is left out of this text.]",
                    "",
                ]
                i = end
                continue
        keep.append(lines[i])
        i += 1
    return "\n".join(keep)


def record_text(text: str) -> Tuple[str, Trim]:
    """The text a `trimmed-1` record request carries, and what was left out of it."""
    trim = Trim()
    return drop_reference_lists(drop_repeats(text, trim), trim), trim


# -- trimmed-2: a request over its limit, cut to what its analyses need ---------------------

#: `RunSpec.record_text` for trimmed-1, and, where that request is still over the spec's
#: `max_input_tokens`: the reviewed ranges of a thesis or book (`ranges_of`), its numeric
#: supplementary data left out (`numeric_data_spans`), and as a last resort the paragraphs
#: nearest its analyses (`fit_text`). The parse, and so every coordinate, is unchanged.
RECORD_TEXT_FIT = "trimmed-2"


def omit(text: str, spans: List[Tuple[int, int, str]]) -> str:
    """`text` with each (start, end, what) span replaced by a line saying `what` of how many
    characters is left out there."""
    out, last = [], 0
    for start, end, what in sorted(_merged(spans)):
        out.append(text[last:start])
        out.append(f"\n[{end - start:,} characters of {what} are left out here.]\n")
        last = end
    out.append(text[last:])
    return "".join(out)


def _merged(spans: List[Tuple[int, int, str]]) -> List[Tuple[int, int, str]]:
    out: List[Tuple[int, int, str]] = []
    for start, end, what in sorted(spans):
        if out and start <= out[-1][1]:
            s0, e0, w0 = out[-1]
            out[-1] = (s0, max(e0, end), w0 if w0 == what else "the text")
        else:
            out.append((start, end, what))
    return out


# -- numeric supplementary data --------------------------------------------------------------

#: A supplementary unit (a file's block of the text, or one sheet of it) holding only data
#: (eLife source data, per-subject values, time series, matrices) is left out of a request when
#: over `NUMERIC_SHARE` of its characters are digits and number punctuation, it is
#: `MIN_DATA_CHARS` or longer, its opening names no coordinates (`_COORD_HEADER`), and no
#: analysis of the parse came from it. Measured 2026-11-27 (PR description): such data
#: tokenizes at 0.97 tokens a character, four times prose; prose and coordinate tables scored
#: 0.14-0.28, the data 0.89-0.99. 38% of the corpus's analyses with points come from
#: supplementary tables, which this never touches.
NUMERIC_SHARE = 0.85
MIN_DATA_CHARS = 2000
COORD_HEADER_CHARS = 3000
_COORD_HEADER = re.compile(
    r"(?i)\b(mni|talairach|tal\b|coordinates?|peaks?|clusters?|brodmann|BA\b)"
    r"|\bx\s*[,/\t]\s*y\s*[,/\t]\s*z\b")
_NUMBER_CHARS = frozenset("0123456789.,;:-+−eE%()[]/|")
_SHEET_TABLE = re.compile(r"^s\d+-")
_SUPPLEMENT_CAPTION = re.compile(r"(?i)supplement|\bS\d+\b|appendix|dataset")
DATA = "numeric supplementary data"


def numeric_share(text: str) -> float:
    body = [ch for ch in text if not ch.isspace()]
    return sum(ch in _NUMBER_CHARS for ch in body) / max(1, len(body))


def _supplementary(table: dict) -> bool:
    """A table read from a supplementary file: a sheet of one, or a caption naming one."""
    caption = f"{table.get('caption') or ''} {table.get('label') or ''}"
    return bool(_SHEET_TABLE.match(table.get("table_id") or "")
                or _SUPPLEMENT_CAPTION.search(caption))


@dataclass
class Unit:
    """One supplementary file's block of a text, or one sheet of it."""

    start: int
    end: int
    label: str
    tables: List[str]
    protected: bool
    numeric: float
    coord_header: bool

    @property
    def data(self) -> bool:
        return (not self.protected and not self.coord_header and self.numeric > NUMERIC_SHARE
                and self.end - self.start >= MIN_DATA_CHARS)


def supplement_units(text: str, parsed: dict, entries: List[dict]) -> Tuple[List[Unit], List[str]]:
    """The supplementary units of a text, and the tables of its analyses that are
    supplementary but sit in no unit and have no place in the text (`unmapped`).

    A unit is a file's block under its heading in a "supplement" section of the parse; a block
    whose tables are sheets ("Supplementary file 1 (x.xls): <sheet>") is cut at each sheet's
    name line. A unit is protected when an analysis came from one of its tables, or one of
    the analyses' text spans (or its table's span) falls inside it."""
    tables = parsed.get("tables") or []
    used = {e.get("table_id") for e in entries if e.get("table_id")}
    spans = [(sp["start_char"], sp.get("end_char") or sp["start_char"] + 1)
             for e in entries for sp in (e.get("text_spans") or [])
             if sp.get("start_char") is not None]
    for t in tables:
        sp = t.get("text_span") or {}
        if t.get("table_id") in used and sp.get("start_char") is not None:
            spans.append((sp["start_char"], sp.get("end_char") or sp["start_char"] + 1))
    units: List[Unit] = []
    claimed: set = set()
    for sec in parsed.get("sections") or []:
        if sec.get("kind") != "supplement":
            continue
        a, b = sec["start_char"], sec["end_char"]
        cuts = sorted({a} | {a + m.start() for m in re.finditer(r"(?m)^#{1,6}[^\n]*$", text[a:b])})
        for x, y in zip(cuts, cuts[1:] + [b]):
            line_end = text.find("\n", x, y)
            head = text[x:line_end if line_end > 0 else y].lstrip("# ").strip()
            prefix = head.split("):")[0] + ")" if "):" in head else head
            mine = ([t for t in tables if (t.get("caption") or "").startswith(prefix)]
                    if "(" in prefix else [])
            sheets, pos = [], x
            for t in mine:
                name = (t.get("caption") or "")[len(prefix):].lstrip(": ").strip()
                at = text.find("\n" + name + "\n", pos, y) if name else -1
                if at >= 0:
                    sheets.append((at + 1, t))
                    pos = at + 1
            if sheets:
                bounds = [x] + [p for p, _ in sheets] + [y]
                pieces = [(bounds[0], bounds[1], [])] + [
                    (bounds[j + 1], bounds[j + 2], [sheets[j][1]]) for j in range(len(sheets))]
            else:
                pieces = [(x, y, mine)]
            for p, q, owned in pieces:
                ids = [t.get("table_id") for t in owned]
                claimed.update(ids)
                seg = text[p:q]
                units.append(Unit(p, q, head[:100], ids,
                                  protected=bool(set(ids) & used)
                                  or any(s < q and e > p for s, e in spans),
                                  numeric=numeric_share(seg),
                                  coord_header=bool(_COORD_HEADER.search(seg[:COORD_HEADER_CHARS]))))
    by_id = {t.get("table_id"): t for t in tables}
    unmapped = sorted(tid for tid in used if tid not in claimed and tid in by_id
                      and _supplementary(by_id[tid])
                      and (by_id[tid].get("text_span") or {}).get("start_char") is None)
    return units, unmapped


def numeric_data_spans(text: str, parsed: dict, entries: List[dict]) -> List[Tuple[int, int, str]]:
    """The spans of a text's supplementary data units (`Unit.data`), each less its first line
    (the file's caption or the sheet's name). None when a supplementary table of an analysis
    is unmapped: what holds its rows cannot be told from what does not."""
    units, unmapped = supplement_units(text, parsed, entries)
    if unmapped:
        return []
    out = []
    for u in units:
        if u.data:
            line_end = text.find("\n", u.start, u.end)
            out.append((line_end + 1 if line_end > 0 else u.start, u.end, DATA))
    return out


# -- reviewed ranges of theses and books -----------------------------------------------------

#: Per article, read 2026-11-27 (PR description): what of a long thesis or book its record is
#: drawn from (`keep`, character ranges of its text), or that it holds no record of its own
#: (`skip`, why: a book on others' studies, a meta-analysis chapter, an atlas). Each is bound
#: to the text it was read from by `text_sha256`; a text since changed is not cut by it.
RANGES = Path(__file__).with_name("record_text_ranges.json")


@lru_cache(maxsize=None)
def _ranges() -> dict:
    return json.loads(RANGES.read_text("utf-8")) if RANGES.exists() else {}


def ranges_of(article_id: str, text_sha256: str) -> Optional[dict]:
    held = _ranges().get(article_id)
    return held if held and held.get("text_sha256") == text_sha256 else None


def outside(keep: List[List[int]], length: int) -> List[Tuple[int, int, str]]:
    """The spans of a text of `length` characters outside the `keep` ranges."""
    out, last = [], 0
    for start, end in sorted(keep):
        if start > last:
            out.append((last, start, "the text"))
        last = max(last, end)
    if last < length:
        out.append((last, length, "the text"))
    return out


# -- the last resort: the paragraphs nearest the analyses ------------------------------------

#: A unit of the text is a paragraph, cut at line ends (else anywhere) into pieces of at most
#: `MAX_UNIT` characters: an unbroken block (a PDF page, an abstract book's one paragraph)
#: must not stand or fall whole.
MAX_UNIT = 4000
#: The opening of the text that is kept before anything else (title, authors, abstract), in
#: tokens; an abstract book's opening holds nothing of its analyses' abstracts, so it is short.
FRONT_TOKENS = 1500
#: The section kinds of the parse (`parsed_paper.json` "sections") a record is drawn from
#: most: after the analyses' own paragraphs, these are kept first, nearest first.
CORE_KINDS = frozenset({"methods", "results"})


@dataclass
class Fit:
    """What `fit_text` kept of one text."""

    units: int = 0
    kept: int = 0
    anchored: int = 0
    gaps: int = 0
    gap_chars: int = 0


def _units(text: str) -> List[Tuple[int, int]]:
    """(start, end) of each paragraph of `text`, long ones cut into pieces of `MAX_UNIT`."""
    out = []
    for m in re.finditer(r"[^\n]+(?:\n(?![ \t]*\n)[^\n]*)*", text):
        start, end = m.start(), m.end()
        while end - start > MAX_UNIT:
            cut = text.rfind("\n", start + MAX_UNIT // 2, start + MAX_UNIT)
            if cut <= start:
                cut = text.rfind(" ", start + MAX_UNIT // 2, start + MAX_UNIT)
            cut = cut + 1 if cut > start else start + MAX_UNIT
            out.append((start, cut))
            start = cut
        if text[start:end].strip():
            out.append((start, end))
    return out


def fit_text(text: str, anchors: List[Tuple[int, int]], budget: int, ntok,
             sections: List[dict] = (), exclude: List[Tuple[int, int, str]] = ()
             ) -> Tuple[str, Fit]:
    """`text`, or as much of it as `budget` tokens hold (`ntok` counts them), kept in order.

    The units overlapping an anchor (an analysis's text spans, the place of its table) are
    kept first, with the heading above each; then the opening of the text (`FRONT_TOKENS`);
    then the units of the methods and results (`CORE_KINDS`), nearest an anchor first; then
    the rest, nearest first. A unit whose middle is in an `exclude` span is never kept. Each
    run of units left out becomes one line saying how much is left out there. Anchors and
    spans are character offsets into `text`."""
    fit = Fit()
    units = _units(text)
    fit.units = len(units)
    barred = [any(a <= (s + e) // 2 < b for a, b, _ in exclude) for s, e in units]
    cost = [0 if barred[k] else ntok(text[s:e]) for k, (s, e) in enumerate(units)]
    if not any(barred) and sum(cost) <= budget:
        fit.kept = len(units)
        return text, fit
    spans = sorted((max(0, a), max(a + 1, b)) for a, b in anchors)
    anchored = [not barred[k] and any(a < e and b > s for a, b in spans)
                for k, (s, e) in enumerate(units)]
    heading = [text[s:e].lstrip().startswith("#") for s, e in units]
    kinds = []
    for s, _ in units:
        kind = next((sec.get("kind") for sec in sections
                     if sec.get("start_char", 0) <= s < sec.get("end_char", 0)), None)
        kinds.append(kind)

    def distance(k):
        s, e = units[k]
        return min((max(a - e, s - b, 0) for a, b in spans), default=len(text))

    order: List[int] = []
    for k in range(len(units)):
        if anchored[k]:
            above = next((j for j in range(k - 1, -1, -1) if heading[j]), None)
            if above is not None:
                order.append(above)
            order.append(k)
    front, n = [], 0
    for k in range(len(units)):
        if n + cost[k] > FRONT_TOKENS:
            break
        front.append(k)
        n += cost[k]
    order += front
    rest = [k for k in range(len(units)) if not anchored[k]]
    order += sorted((k for k in rest if kinds[k] in CORE_KINDS), key=distance)
    order += sorted((k for k in rest if kinds[k] not in CORE_KINDS), key=distance)
    keep, used = set(), 0
    for k in order:
        if k in keep or barred[k] or used + cost[k] > budget:
            continue
        keep.add(k)
        used += cost[k]
    fit.kept = len(keep)
    fit.anchored = sum(anchored[k] for k in keep)
    out: List[str] = []
    gap = 0
    for k, (s, e) in enumerate(units):
        if k in keep:
            if gap:
                out.append(f"[{gap:,} characters of the text are left out here.]")
                fit.gaps += 1
                fit.gap_chars += gap
                gap = 0
            out.append(text[s:e])
        else:
            gap += e - s
    if gap:
        out.append(f"[{gap:,} characters of the text are left out here.]")
        fit.gaps += 1
        fit.gap_chars += gap
    return "\n\n".join(out), fit


#: A line of a list of tables or figures: dot leaders, then a page number. A table's label
#: found there is not where the table is (one thesis of the 62 had all 30 anchors there).
_LIST_LINE = re.compile(r"(?:\.\s?){4,}\s*\d{1,4}\s*$|…+\s*\d{1,4}\s*$")


def _in_list_line(text: str, at: int) -> bool:
    start = text.rfind("\n", 0, at) + 1
    end = text.find("\n", at)
    return bool(_LIST_LINE.search(text[start:end if end >= 0 else len(text)]))


def anchors_of(text: str, entries: List[dict], tables: List[dict]) -> List[Tuple[int, int]]:
    """Where each analysis of a parse is in its text: its text spans, and its table's span,
    else the first place the table's label (or the start of its caption) is printed outside
    a list of tables or figures."""
    by_id = {t.get("table_id"): t for t in tables or []}
    out = []
    for e in entries:
        for sp in e.get("text_spans") or []:
            if sp.get("start_char") is not None:
                out.append((sp["start_char"], sp.get("end_char") or sp["start_char"] + 1))
        t = by_id.get(e.get("table_id"))
        if t is None:
            continue
        sp = t.get("text_span") or {}
        if sp.get("start_char") is not None:
            out.append((sp["start_char"], sp.get("end_char") or sp["start_char"] + 1))
            continue
        for probe in (t.get("label"), (t.get("caption") or "")[:80]):
            if not probe or probe == "None":
                continue
            at = text.find(probe)
            while at >= 0 and _in_list_line(text, at):
                at = text.find(probe, at + 1)
            if at >= 0:
                out.append((at, at + len(probe)))
                break
        else:
            found = _row_in_text(text, t, e)
            if found is not None:
                out.append(found)
    return out


#: A table row is found in a text when its first `ROW_CELLS` non-empty cells are printed
#: within `ROW_REACH` characters of each other, the first of them at least `MIN_CELL` long.
ROW_CELLS = 3
ROW_REACH = 300
MIN_CELL = 3


def _row_in_text(text: str, table: dict, entry: dict) -> Optional[Tuple[int, int]]:
    """Where a row an analysis's points came from is printed: the place of a table the text
    holds without a span or a label (a thesis's tables, a supplement's sheets)."""
    rows = {p.get("row") for p in entry.get("points") or [] if p.get("row") is not None}
    for row in table.get("rows") or []:
        if row.get("row") not in rows:
            continue
        cells = [str(c).strip() for c in row.get("cells") or [] if str(c).strip()][:ROW_CELLS]
        if len(cells) < 2 or len(cells[0]) < MIN_CELL:
            continue
        at = text.find(cells[0])
        while at >= 0:
            window = text[at:at + ROW_REACH]
            if all(c in window for c in cells[1:]):
                return at, at + len(cells[0])
            at = text.find(cells[0], at + 1)
    return None


def paragraph_of(text: str, at: int) -> Tuple[int, int]:
    """The paragraph of `text` holding offset `at`: between blank lines."""
    start = text.rfind("\n\n", 0, at)
    end = text.find("\n\n", at)
    return (start + 2 if start >= 0 else 0), (end if end >= 0 else len(text))


def carve(spans: List[Tuple[int, int, str]], keep: List[Tuple[int, int]]
          ) -> List[Tuple[int, int, str]]:
    """`spans` less every part of them inside one of the `keep` ranges."""
    out = []
    for start, end, what in spans:
        pieces = [(start, end)]
        for a, b in sorted(keep):
            nxt = []
            for x, y in pieces:
                if b <= x or a >= y:
                    nxt.append((x, y))
                    continue
                if x < a:
                    nxt.append((x, a))
                if b < y:
                    nxt.append((b, y))
            pieces = nxt
        out += [(x, y, what) for x, y in pieces if y > x]
    return out
