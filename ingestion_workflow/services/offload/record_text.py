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
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import List, Tuple

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
