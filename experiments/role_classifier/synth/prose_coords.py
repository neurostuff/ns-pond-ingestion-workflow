"""Find stereotactic coordinates written in an article's prose, outside its tables.

Experiment, not pipeline code. For each article: strip table blocks from the
extracted text, split into sentences, match coordinate triplets with a few
patterns, and build a context of the matching sentence plus its neighbours --
extended past any neighbour that also holds coordinates.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

MINUS = "−–—‐‑‒﹣－"
NUM = rf"(?:\u00b1|[+{MINUS}-])?\s?\d{{1,3}}(?:\.\d+)?"
SEP = r"\s*[,;/]\s*|\s+"  # comma, semicolon, slash, or whitespace between values

# x = -42, y = 18, z = 6   (also x: -42 / y -18 ...)
LABELLED = re.compile(
    rf"\bx\s*[=:]?\s*(?P<x>{NUM})\s*[,;]?\s*(?:and\s+)?\by\s*[=:]?\s*(?P<y>{NUM})\s*[,;]?\s*(?:and\s+)?\bz\s*[=:]?\s*(?P<z>{NUM})",
    re.I,
)
# (x, y, z) = (-42, 18, 6)   /   x, y, z = -42, 18, 6
HEADED = re.compile(
    rf"\(?\s*x\s*,\s*y\s*,\s*z\s*\)?\s*[=:]\s*[\(\[]?\s*(?P<x>{NUM})(?:{SEP})(?P<y>{NUM})(?:{SEP})(?P<z>{NUM})\s*[\)\]]?",
    re.I,
)
# (-42, 18, 6)   [42 -18 6]   (-42/18/6)
BRACKETED = re.compile(
    rf"[\(\[]\s*(?P<x>{NUM})(?:{SEP})(?P<y>{NUM})(?:{SEP})(?P<z>{NUM})\s*[\)\]]"
)
# MNI: 4, 14, -2   /   Talairach coordinates 12, -56, 34
PREFIXED = re.compile(
    rf"\b(?:MNI|Talairach|TAL)(?:\s+coordinates?)?\s*[:=]?\s*(?P<x>{NUM})\s*,\s*(?P<y>{NUM})\s*,\s*(?P<z>{NUM})(?![\d.])"
)
PATTERNS = (("labelled", LABELLED), ("headed", HEADED), ("bracketed", BRACKETED), ("prefixed", PREFIXED))

CUE = re.compile(
    r"\b(MNI|Talairach|stereotax\w*|coordinates?|peak\w*|local maxim\w*|maxima|"
    r"voxels?|clusters?|gyrus|gyri|cortex|cortices|sulcus|lobule|nucleus|"
    r"amygdala|hippocamp\w*|insula\w*|thalam\w*|striatum|putamen|caudate|cerebell\w*|"
    r"precuneus|cuneus|BA\s?\d+|Brodmann|ROI|seed|sphere|activation|x\s*,\s*y\s*,\s*z)\b",
    re.I,
)
#: What a bare bracketed triplet must sit beside. Anatomy words are not
#: enough: citation lists ("[ 29 , 33 , 37 ]") turn up in sentences about the
#: hippocampus and cortex.
COORD_CUE = re.compile(
    r"\b(MNI|Talairach|stereotax\w*|coordinates?|co-ordinates?|peak\w*|local maxim\w*|maxima|"
    r"voxels?|seeds?|spheres?|cent(?:er|re)d?|x\s*,\s*y\s*,\s*z|xyz)\b",
    re.I,
)
SPACE = re.compile(r"\b(MNI|Montreal Neurological|Talairach|Tournoux)\b", re.I)

# Sentence ends at . ! ? followed by space and an upper-case letter or bracket,
# except after common abbreviations and inside "et al." and decimals.
ABBREV = re.compile(r"(?:\b(?:e\.g|i\.e|et al|Fig|Figs|Tab|vs|approx|ca|cf|resp|No|Eq|Ref|Dr|Mr|Ms|Inc|Ltd|al)\.|\b[A-Z]\.)$")
CANDIDATE_END = re.compile(r"[.!?](?:[\"')\]]*)\s+(?=[A-Z(\[\"'])")


def table_free(text: str) -> str:
    """Drop the lines of tables the extractors inserted: tab-separated rows
    (pubget, elsevier, ACE) and pipe-separated rows (docling PDF)."""
    kept = []
    for line in text.splitlines():
        if line.count("\t") >= 2 or line.count("|") >= 3:
            continue
        kept.append(line)
    return "\n".join(kept)


def sentences(text: str) -> List[str]:
    text = re.sub(r"[ \t]*\n[ \t]*", " \n", text)
    out, start = [], 0
    for m in CANDIDATE_END.finditer(text):
        end = m.start() + 1
        if ABBREV.search(text[start:end]):
            continue
        piece = text[start:end].strip()
        if piece:
            out.append(piece)
        start = m.end()
    tail = text[start:].strip()
    if tail:
        out.append(tail)
    # Paragraph breaks are sentence breaks too.
    split = []
    for s in out:
        split.extend(p.strip() for p in re.split(r"\s*\n\s*\n\s*|\s\n(?=[A-Z#])", s) if p.strip())
    return split


def _num(s: str) -> float:
    s = re.sub(r"\s", "", re.sub(rf"[{MINUS}]", "-", s)).replace("+", "").replace("\u00b1", "")
    return float(s)


def plausible(x: float, y: float, z: float) -> bool:
    if not (-90 <= x <= 90 and -125 <= y <= 90 and -75 <= z <= 95):
        return False
    if x == y == z == 0:
        return False
    return True


@dataclass
class Hit:
    pattern: str
    x: float
    y: float
    z: float
    span: Tuple[int, int]


def find(sentence: str) -> List[Hit]:
    hits, pending, taken = [], [], []
    for name, rx in PATTERNS:
        for m in rx.finditer(sentence):
            if any(a < m.end() and m.start() < b for a, b in taken):
                continue
            try:
                x, y, z = (_num(m.group(k)) for k in "xyz")
            except ValueError:
                continue
            if not plausible(x, y, z):
                continue
            # Where a figure is cut ("slices at x = 30, y = 50, z = 16"), not a result.
            if re.search(r"\bslices?\b[^.;]{0,40}$", sentence[:m.start()], re.I):
                continue
            hit = Hit(name, x, y, z, m.span())
            taken.append(m.span())
            if name != "bracketed":
                hits.append(hit)
                continue
            # "[ 24 , 32 – 34 ]": an en dash between two numbers is a range.
            if re.search(r"\d\s*[\u2013\u2014]\s*\d", m.group(0)):
                continue
            if all(v == int(v) for v in (x, y, z)) and y == x + 1 and z == y + 1:
                continue
            pending.append((hit, m))
    # A bare bracketed triplet is as often a citation list, a range or three
    # percentages. It counts beside a coordinate word; as one item of a list
    # whose other items qualify; or, holding a negative value -- which a
    # citation list never does -- beside an anatomical word.
    def qualifies(hit, m):
        near = sentence[max(m.start() - 80, 0):m.end() + 80]
        if COORD_CUE.search(near):
            return True
        return min(hit.x, hit.y, hit.z) < 0 and bool(CUE.search(sentence[max(m.start() - 60, 0):m.start()]))
    good = [h for h, m in pending if qualifies(h, m)]
    if good or hits:
        good = [h for h, _ in pending]
    return sorted(hits + good, key=lambda h: h.span)


@dataclass
class Context:
    first: int
    last: int
    text: str
    hits: List[Hit] = field(default_factory=list)
    cued: bool = False
    space: Optional[str] = None


def contexts(sents: List[str], max_chars: int = 4000) -> List[Context]:
    """Each run of coordinate sentences plus one coordinate-free sentence on
    either side -- the rule: the matching sentence, the one before and after,
    and past any neighbour that also holds coordinates."""
    found = [find(s) for s in sents]
    out, i = [], 0
    while i < len(sents):
        if not found[i]:
            i += 1
            continue
        j = i
        while j + 1 < len(sents) and found[j + 1]:
            j += 1
        first, last = max(i - 1, 0), min(j + 1, len(sents) - 1)
        text = " ".join(sents[first:last + 1])
        while len(text) > max_chars and last > j:
            last -= 1
            text = " ".join(sents[first:last + 1])
        hits = [h for k in range(i, j + 1) for h in found[k]]
        ctx = Context(first, last, text, hits)
        ctx.cued = bool(CUE.search(text))
        sp = SPACE.search(text)
        ctx.space = ("MNI" if sp and sp.group(1).lower().startswith(("mni", "montreal")) else "TAL") if sp else None
        out.append(ctx)
        i = j + 1
    return out
