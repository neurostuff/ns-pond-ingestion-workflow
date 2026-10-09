"""In-text citations found in plain text and tied to a reference list (roadmap L3).

For a source that marks nothing (a PDF) or links nothing: numbered markers ("[12]",
"(3, 4)", a superscript whose formatting was lost: "in OCD, 7,9 we") and author-year
ones ("Smith et al., 2001"), matched to a list that gives each entry's names and year.

Scored against exact XML links on 620 ns-pond papers, and with Crossref's list on 110:
see the wiki and `experiments/references/`.
"""

from __future__ import annotations

import re
import unicodedata
from collections import defaultdict
from typing import Dict, List, Sequence, Tuple

from ingestion_workflow.services.citations import Sentences

#: How sure each kind of marker is, matched against a Crossref list: author-year markers
#: were 0.97 precise, numbered ones 0.87 (Crossref's order differs from the printed
#: numbering for ~8% of entries). Of 15 loose numbers read by hand in PDFs, 13 were right.
CONFIDENCE = {"author_year": 0.95, "author_year_ambiguous": 0.6,
              "bracket": 0.85, "paren": 0.8, "bare": 0.75}

_NUM = r"\d{1,3}(?:\s*[-–—]\s*\d{1,3})?"
_NUM_LIST = rf"{_NUM}(?:\s*[,;]\s*{_NUM}|\s+{_NUM})*"
_BRACKET = re.compile(rf"\[\s*({_NUM_LIST})\s*\]")
_PAREN = re.compile(rf"\(\s*({_NUM_LIST})\s*\)")
_BARE = re.compile(r"(?<=[A-Za-z)\]][.,;:])\s?([1-9]\d{0,2}(?:\s?[-–,]\s?[1-9]\d{0,2})*)(?=\s+[A-Za-z(]|\s*\n)")
_NOT_BEFORE_BARE = re.compile(
    r"(?:\b(?:fig|figs|figure|table|tab|eq|version|v|no|step|study|experiment|exp|session|run|block)\.?)"
    r"\s*[.,;:]?\s*$", re.I)
_YEAR = re.compile(r"\b((?:19|20)\d\d)([a-z])?\b")
_ALLOWED_GAP = {"et", "al", "and", "&", "s", "in", "press", "see", "also", "e", "g", "cf", "for",
                "review", "reviews", "a", "b", "c", "d"}


def _fold_char(c: str) -> str:
    d = "".join(x for x in unicodedata.normalize("NFKD", c) if not unicodedata.combining(x)).lower()
    return d[:1] if d else " "


def _fold(s: str) -> str:
    """Accents off and lower case, one character per character, so offsets carry over."""
    return "".join(_fold_char(c) for c in s or "")


def _table_line(text: str, pos: int) -> bool:
    """A row of a TSV table (pubget, Elsevier) or of a Docling markdown table."""
    a = text.rfind("\n", 0, pos) + 1
    b = text.find("\n", pos)
    line = text[a : b if b >= 0 else None]
    return "\t" in line or line.lstrip().startswith("|")


def _expand(group: str) -> List[int]:
    out: List[int] = []
    for part in re.split(r"\s*[,;]\s*|\s+(?![-–—])(?<![-–—]\s)", group):
        m = re.fullmatch(r"(\d+)\s*[-–—]\s*(\d+)", part)
        if m:
            a, b = int(m[1]), int(m[2])
            out.extend(range(a, b + 1) if 0 < b - a < 100 else [a, b])
        elif part.strip().isdigit():
            out.append(int(part))
    return out


def _numbered_index(refs) -> Dict[int, str]:
    by_label = {}
    for r in refs:
        digits = re.sub(r"\D", "", r.get("label") or "")
        if digits:
            by_label.setdefault(int(digits), r["id"])
    if len(by_label) >= 0.8 * len(refs):
        return by_label
    return {r.get("position") or i + 1: r["id"] for i, r in enumerate(refs)}


def _not_a_citation(text: str, m) -> bool:
    """Statistics "F (1, 22)", labels "SMN(41)", enumerations "(1) ... (2)", ranges "57 [50–63]"."""
    before = text[max(0, m.start() - 12) : m.start()]
    if re.search(r"[^\W_]$", before) or re.search(r"\d\s*$", before):
        return True
    if re.search(r"(?:^|[\s(])(?:[A-Za-zχηβ]|df|SD|SE|CI|M|n|N)\s*[²2]?\s*$", before):
        return True
    nums = _expand(m[1])
    if len(nums) == 1 and m[0].startswith("("):
        if (re.search(rf"\(\s*{nums[0] + 1}\s*\)", text[m.end() : m.end() + 400])
                or re.search(rf"\(\s*{nums[0] - 1}\s*\)", text[max(0, m.start() - 400) : m.start()])):
            return True
    return False


def _numbered(text: str, refs, loose: bool) -> List[dict]:
    index = _numbered_index(refs)
    out = []
    patterns = ((_BRACKET, "bracket"), (_PAREN, "paren")) + (((_BARE, "bare"),) if loose else ())
    for pattern, kind in patterns:
        for m in pattern.finditer(text):
            if _table_line(text, m.start()):
                continue
            if kind != "bare" and _not_a_citation(text, m):
                continue
            if kind == "bare" and _NOT_BEFORE_BARE.search(text[max(0, m.start() - 15) : m.start()]):
                continue
            nums = _expand(m[1])
            ids = [index[n] for n in nums if n in index]
            if not ids or len(ids) < len(nums):
                continue
            start = m.start(1) if kind == "bare" else m.start()
            out.append({"start": start, "end": m.end(), "refs": ids, "kind": kind})
    return out


def _first_author(text: str) -> List[str]:
    """First author of an unstructured entry: 'Smith, J.' / 'Smith J' at its start."""
    t = re.sub(r"^\W*\d+\W*", "", text or "")
    m = re.match(r"((?:(?:van|von|de|der|den|di|da|le|la|du)\s+)*[A-ZÀ-Þ][^\W\d_]+(?:[-'’][A-ZÀ-Þ]?[^\W\d_]+)*)", t)
    return [m[1]] if m else []


def _author_year_entries(refs) -> Dict[str, List[dict]]:
    by_year: Dict[str, List[dict]] = defaultdict(list)
    for r in refs:
        authors = r.get("authors") or _first_author(r.get("text"))
        year = str(r.get("year") or "") or next(iter(re.findall(r"\b((?:19|20)\d\d)[a-z]?\b", r.get("text") or "")), "")
        if not authors or not year:
            continue
        m = re.search(rf"\b{year}([a-z])\b", (r.get("label") or "") + " " + (r.get("text") or ""))
        by_year[year].append({"id": r["id"], "authors": authors, "suffix": m[1] if m else ""})
    return by_year


def _author_year(text: str, refs) -> List[dict]:
    by_year = _author_year_entries(refs)
    folded = _fold(text)
    out = []
    for m in _YEAR.finditer(text):
        candidates = by_year.get(m[1], [])
        if not candidates or _table_line(text, m.start()):
            continue
        line_start = text.rfind("\n", 0, m.start()) + 1
        lo = max(line_start, m.start() - 120, text.rfind(";", 0, m.start()) + 1)
        best = None
        for r in candidates:
            first = _fold(r["authors"][0])
            if len(first) < 2:
                continue
            coauthors = {w for x in r["authors"] for w in re.findall(r"[^\W\d_]+", _fold(x))}
            # a list naming only the first author (Crossref) cannot vouch for "and Raichle"
            unknown_ok = len(r["authors"]) == 1
            for hit in re.finditer(rf"(?<![^\W\d_]){re.escape(first)}(?![^\W\d_])", folded[lo : m.start()]):
                s = lo + hit.start()
                gap = text[s + len(first) : m.start()]
                ok, prev = len(gap) <= 80, ""
                for tok in re.findall(r"[^\W\d_]+|\d+|\S", gap):
                    t = _fold(tok)
                    if t in _ALLOWED_GAP or tok.isdigit() or tok in ",;()[]&.'’-–" or t in coauthors:
                        prev = t
                        continue
                    if unknown_ok and prev in ("and", "&") and tok[:1].isupper():
                        prev = t
                        continue
                    ok = False
                    break
                if ok and (best is None or s < best[0]):  # the longest citation that reads cleanly
                    best = (s, r)
        if best is None:
            continue
        s, r = best
        same = [x for x in candidates if _fold(x["authors"][0]) == _fold(r["authors"][0])]
        if m[2]:
            same = [x for x in same if x["suffix"] == m[2]] or same
        out.append({"start": s, "end": m.end(), "refs": [same[0]["id"]], "kind": "author_year",
                    "ambiguous": len(same) > 1, "_entry": same[0], "_year": m[1]})
    # "Toepper et al., 2010a, b": the bare letter continues the last match
    for c in list(out):
        if re.search(r"\d[a-z]$", text[c["start"] : c["end"]]):
            tail = re.match(r"\s*,\s*([a-z])\b", text[c["end"] : c["end"] + 6])
            if tail:
                sib = [x for x in by_year.get(c["_year"], [])
                       if _fold(x["authors"][0]) == _fold(c["_entry"]["authors"][0]) and x["suffix"] == tail[1]]
                if sib:
                    e = c["end"] + tail.end()
                    out.append({"start": e - 1, "end": e, "refs": [sib[0]["id"]], "kind": "author_year",
                                "ambiguous": False})
    return out


def find(text: str, refs: Sequence[dict], *, loose_numbers: bool = True) -> Tuple[List[dict], str]:
    """Citations in `text` tied to `refs`, in the citation shape, and the style the paper cites in.

    `loose_numbers=False` for text built without superscripts, where a loose number is
    never a marker.
    """
    num, ay = _numbered(text, refs, loose_numbers), _author_year(text, refs)
    style = "numbered" if len(num) > len(ay) else "author_year"
    if style == "author_year":
        num = []  # "(2)" in an author-year paper is a list item or an equation
    elif sum(c["kind"] == "bracket" for c in num) >= 0.5 * len(num):
        num = [c for c in num if c["kind"] == "bracket"]
    elif sum(c["kind"] == "bare" for c in num) < 0.3 * len(num):
        num = [c for c in num if c["kind"] != "bare"]
    sentences = Sentences(text)
    out = []
    for c in sorted(num + ay, key=lambda c: c["start"]):
        sentence = sentences.of(c["start"])
        kind = "author_year_ambiguous" if c.get("ambiguous") else c["kind"]
        out.append({
            "text_span": {"start_char": c["start"], "end_char": c["end"], "text": text[c["start"] : c["end"]]},
            "sentence": {"start_char": sentence[0], "end_char": sentence[1]} if sentence else None,
            "references": c["refs"],
            "method": "author_year" if c["kind"] == "author_year" else "numbered",
            "confidence": CONFIDENCE[kind],
            "marker_in_text": True,
        })
    return out, style
