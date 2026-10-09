"""Find the passages of an article's text that hold stereotactic coordinates.

A passage is the run of sentences holding coordinates plus one sentence on
either side, with the nearest section heading and a little surrounding text
for naming. Table rows the extractors wrote into the text are dropped first.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import List, Optional, Tuple

MINUS = "−–—‐‑‒﹣－"
#: The space after a sign only: a space before one is left to the patterns,
#: whose own `\s*` would otherwise overlap it and backtrack without end on a
#: long run of whitespace.
NUM = rf"(?:(?:±|[+{MINUS}-])\s?)?\d{{1,3}}(?:\.\d+)?"
#: Comma, semicolon, slash or whitespace between values, or nothing before a
#: minus: "MNI -6–8 22" is (-6, -8, 22). "(−48, −34, and 42)" too.
SEP = rf"\s*[,;/]\s*(?:and\s+)?|\s+|(?=[{MINUS}-])"
#: Runs of spaces are closed up before any pattern runs: tag-stripped XML has
#: hundreds of them between a bare "x" and the next number.
SPACES = re.compile(r"[ \t\u00a0]{2,}")

LABELLED = re.compile(
    rf"\bx\s*(?:[=:]\s*)?(?P<x>{NUM})\s*(?:[,;]\s*)?(?:and\s+)?\by\s*(?:[=:]\s*)?(?P<y>{NUM})"
    rf"\s*(?:[,;]\s*)?(?:and\s+)?\bz\s*(?:[=:]\s*)?(?P<z>{NUM})",
    re.I,
)
HEADED = re.compile(
    rf"(?:\(\s*)?\bx\s*,\s*y\s*,\s*z\s*(?:\)\s*)?[=:]\s*(?:[\(\[]\s*)?(?P<x>{NUM})(?:{SEP})(?P<y>{NUM})(?:{SEP})"
    rf"(?P<z>{NUM})\s*[\)\]]?",
    re.I,
)
BRACKETED = re.compile(rf"[\(\[]\s*(?P<x>{NUM})(?:{SEP})(?P<y>{NUM})(?:{SEP})(?P<z>{NUM})\s*[\)\]]")
PREFIXED = re.compile(
    rf"\b(?:MNI|Talairach|TAL)(?:\s+coordinates?)?\s*[:=]?\s*(?P<x>{NUM})\s*,\s*(?P<y>{NUM})\s*,"
    rf"\s*(?P<z>{NUM})(?![\d.])"
)
#: Three numbers that are not part of a longer run, kept only beside a cue:
#: "(-8, -96, 2, Z = 9.15)", "Left: -30, 22, -8, max z", "peak voxel, -6, 22, -8;".
#: A fourth number after a semicolon is a field ("(-3, 53, -4; 1253; p < .001)").
LOOSE = re.compile(
    rf"(?<![\d.+{MINUS}\u00b1-])(?<!\d[,;/]\s)(?<!\d\s[,;/]\s)(?<!\d\s[,;/])(?<!\d\s)(?<!\d[,;/])"
    rf"(?P<x>{NUM})(?:{SEP})(?P<y>{NUM})(?:{SEP})(?P<z>{NUM})(?![\d.])(?!\s*[,/]?\s*[+{MINUS}-]?\d)"
)
PATTERNS = (("labelled", LABELLED), ("headed", HEADED), ("bracketed", BRACKETED),
            ("prefixed", PREFIXED), ("loose", LOOSE))

#: One bracket holding several triplets, of which the patterns above see only
#: the first: "[−24, −96, 9; 42, −81, −6]", "(−30, −81, 30 and 36, −75, 33)",
#: "(coordinates of −36, 6, 30; −51, 18, 27)". Every item must be a triplet.
GROUP = re.compile(r"[\(\[]([^()\[\]]{10,600})[\)\]]")
ITEM = re.compile(rf"\s*(?:[A-Za-z][^\d{MINUS}+-]{{0,40}}?)?(?P<x>{NUM})(?:{SEP})(?P<y>{NUM})(?:{SEP})(?P<z>{NUM})\s*")
ITEM_SEP = re.compile(r"\s*;\s*|,?\s+and\s+")

#: Anatomy and imaging words: enough for a negative bracketed triplet.
CUE = re.compile(
    r"\b(MNI|Talairach|stereotax\w*|coordinates?|peak\w*|local maxim\w*|maxima|"
    r"voxels?|clusters?|gyrus|gyri|cortex|cortices|sulcus|sulci|lobules?|nucleus|nuclei|"
    r"amygdala|hippocamp\w*|insula\w*|thalam\w*|striatum|putamen|caudate|cerebell\w*|"
    r"precuneus|cuneus|BA\s?\d+|Brodmann|ROI|seed|sphere|activations?|x\s*,\s*y\s*,\s*z)\b",
    re.I,
)
#: Regions papers abbreviate, which count as anatomy as CUE does: "the MPFC (−6 58 24)".
ANATOMY_ABBR = re.compile(
    r"\b[a-z]{0,3}(?:STS|STG|MTG|ITG|IFG|MFG|SFG|IFC|IPL|SPL|IPS|TPJ|PFC|ACC|PCC|SMA|OFC|FEF|TP|PMC|MFC|"
    r"MPFC|PPC|IOG|MOG|LOC|FFA|PPA|EBA|NAcc|VTA|SN|PAG|BA)\b")
#: What a bare triplet must sit beside. Anatomy alone is not enough: citation
#: lists ("[ 29 , 33 , 37 ]") turn up in sentences about the cortex.
COORD_CUE = re.compile(
    r"\b(MNI|Talairach|stereotax\w*|coordinates?|co-ordinates?|peak\w*|local maxim\w*|maxima|"
    r"voxels?|seeds?|spheres?|cent(?:er|re)d?|x\s*,\s*y\s*,\s*z|xyz|clusters?|cluster size|"
    r"Brodmann|BA\s?\d+|ROIs?|crosshairs?|max(?:imum)?\s*[zt])\b|\b[ZTFzt]\s*(?:\(\d+\))?\s*[=:]\s*[-−]?\d",
    re.I,
)
SPACE = re.compile(r"\b(MNI|Montreal Neurological|Talairach|Tournoux)\b", re.I)
HEADING = re.compile(r"^\s*#{1,6}\s+(.+?)\s*$", re.M)
BROKEN_LIST = re.compile(rf"([,;(\[]|\d)[ \t]*\n\s*\n\s*(?=[{MINUS}-]?\d)")
ABBREV = re.compile(
    r"(?:\b(?:e\.g|i\.e|n\.s|et al|Fig|Figs|Tab|vs|approx|ca|cf|resp|No|Eq|Ref|Dr|Mr|Ms|Inc|Ltd|al)\.|\b[A-Z]\.)$")
CANDIDATE_END = re.compile(r"[.!?](?:[\"')\]]*)\s+(?=[A-Z(\[\"'])")


@dataclass
class Hit:
    pattern: str
    x: float
    y: float
    z: float
    span: Tuple[int, int]


@dataclass
class Passage:
    text: str
    hits: List[Hit] = field(default_factory=list)
    before: str = ""
    after: str = ""
    heading: Optional[str] = None
    space: Optional[str] = None


def table_free(text: str) -> str:
    """Drop table rows the extractors wrote into the text (tab- or pipe-separated)."""
    return "\n".join(
        line for line in text.splitlines() if line.count("\t") < 2 and line.count("|") < 3
    )


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
    split = []
    for s in out:
        split.extend(p.strip() for p in re.split(r"\s*\n\s*\n\s*|\s\n(?=[A-Z#])", s) if p.strip())
    return split


def _num(s: str) -> float:
    return float(re.sub(r"\s", "", re.sub(rf"[{MINUS}]", "-", s)).replace("+", "").replace("±", ""))


def plausible(x: float, y: float, z: float) -> bool:
    if not (-90 <= x <= 90 and -125 <= y <= 90 and -75 <= z <= 95) or x == y == z == 0:
        return False
    # "BA 20, 21, 22", "j = 3, 4, 5": no reported peak is three consecutive numbers.
    return not (y == x + 1 and z == y + 1)


#: All positive, ascending, in square brackets: how a citation list looks, and
#: how 1.4% of 14,556 curated peaks do. Kept only after a coordinate word.
CITED = re.compile(r"^\[\s*\d")
SPATIAL_BEFORE = re.compile(r"(?i)(coordinates?|MNI|Talairach|peak\w*|maxim\w*|cent(?:er|re)d?\s+(?:at|on)|seeds?|at)\W{0,12}$")


def _anatomy(text: str) -> bool:
    return bool(CUE.search(text) or ANATOMY_ABBR.search(text))


def _listed(sentence: str) -> Tuple[List[Hit], List[Hit], List[Tuple[int, int]]]:
    """The triplets of brackets that list several -- those beside a cue, those
    not -- and the brackets' spans."""
    hits, unsure, spans = [], [], []
    for g in GROUP.finditer(sentence):
        start, items = g.start(1), []
        for part in ITEM_SEP.split(g.group(1)) + [None]:
            if part is None:
                break
            m = ITEM.fullmatch(part)
            if m is None:
                items = []
                break
            at = sentence.index(part, start)
            items.append((m, at))
            start = at + len(part)
        if len(items) < 2:
            continue
        xyz = [tuple(_num(m.group(k)) for k in "xyz") for m, _ in items]
        raw = [m.group(k) for m, _ in items for k in "xyz"]
        if not all(plausible(*v) for v in xyz) or any(re.match(r"^[^\d]*0\d", r) for r in raw):
            continue
        near = sentence[max(g.start() - 80, 0):g.end() + 80]
        negative = min(min(v) for v in xyz) < 0
        cued = COORD_CUE.search(near) or negative and _anatomy(sentence[max(g.start() - 60, 0):g.start()])
        (hits if cued else unsure).extend(
            Hit("listed", *v, (at + m.start("x"), at + m.end("z"))) for v, (m, at) in zip(xyz, items))
        spans.append(g.span())
    return hits, unsure, spans


def find(sentence: str) -> List[Hit]:
    """The coordinate triplets in one sentence."""
    hits, unsure, taken = _listed(sentence)
    pending, cited = [], []
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
            # Where a figure is cut ("slices at x = 30, y = 50"), not a result.
            if re.search(r"\bslices?\b[^.;]{0,40}$", sentence[:m.start()], re.I):
                continue
            raw = [m.group(k) for k in "xyz"]
            if any(re.match(r"^[^\d]*0\d", r) or "\u00b1" in r for r in raw):
                continue  # "1,000/34.0" is a number with a separator; "0.37 \u00b1 0.01" a statistic
            hit = Hit(name, x, y, z, m.span())
            if (CITED.match(m.group(0)) and x > 0 and 0 < x < y < z and all(v == int(v) for v in (x, y, z))
                    and not SPATIAL_BEFORE.search(sentence[max(m.start() - 40, 0):m.start()])):
                cited.append(hit)
                taken.append(m.span())
                continue
            taken.append(m.span())
            if name == "loose":
                ranged = re.search(r"\d\s+[–—]\s*\d|\d[–—]\s+\d", m.group(0))
                anatomy = min(x, y, z) < 0 and _anatomy(sentence[max(m.start() - 60, 0):m.start()])
                if ranged:
                    continue
                (hits if COORD_CUE.search(sentence) or anatomy else unsure).append(hit)
                continue
            if name != "bracketed":
                hits.append(hit)
                continue
            if re.search(r"\d\s*[–—]\s*\d", m.group(0)):
                continue  # "[ 24 , 32 – 34 ]" is a range
            pending.append((hit, m))

    # A bare bracketed triplet counts beside a coordinate word, as one item of
    # a list whose other items qualify, or holding a negative value -- which a
    # citation list never does -- beside an anatomical word.
    def qualifies(hit, m):
        if COORD_CUE.search(sentence[max(m.start() - 80, 0):m.end() + 80]):
            return True
        return min(hit.x, hit.y, hit.z) < 0 and _anatomy(sentence[max(m.start() - 60, 0):m.start()])

    # A sentence that reports coordinates reports its doubtful-looking ones
    # too: "pre-SMA [3, 9, 63]" among "[−3, −21, −21; 9, −24, −6]".
    good = [h for h, m in pending if qualifies(h, m)]
    if good or hits:
        good = [h for h, _ in pending] + unsure + cited
    return sorted(hits + good, key=lambda h: h.span)


def passages(text: str, *, max_chars: int = 4000, before: int = 3) -> List[Passage]:
    """Every run of coordinate sentences, with a sentence either side.

    `before` sentences preceding the passage, the one after it and the last
    heading above it are kept alongside: they often name the contrast the
    passage only refers to ("This comparison revealed ...").
    """
    # A PDF's column or page break can fall inside a triplet ("[-21, -6,\n\n-27]"),
    # and a paragraph break ends a sentence, so it is closed up first.
    clean = BROKEN_LIST.sub(r"\1 ", SPACES.sub(" ", table_free(text or "")))
    sents = sentences(HEADING.sub(lambda m: f"\n\n#{m.group(1)}\n\n", clean))
    found = [[] if s.startswith("#") else find(s) for s in sents]
    out, i = [], 0
    while i < len(sents):
        if not found[i]:
            i += 1
            continue
        j = i
        while j + 1 < len(sents) and found[j + 1]:
            j += 1
        first, last = max(i - 1, 0), min(j + 1, len(sents) - 1)
        if sents[first].startswith("#"):
            first = i
        if sents[last].startswith("#"):
            last = j
        body = " ".join(sents[first:last + 1])
        while len(body) > max_chars and last > j:
            last -= 1
            body = " ".join(sents[first:last + 1])
        heading = next((s.lstrip("#").strip() for s in reversed(sents[:first]) if s.startswith("#")), None)
        prev = [s for s in sents[max(0, first - before):first] if not s.startswith("#")]
        nxt = next((s for s in sents[last + 1:last + 2] if not s.startswith("#")), "")
        sp = SPACE.search(body)
        out.append(Passage(
            text=body,
            hits=[h for k in range(i, j + 1) for h in found[k]],
            before=" ".join(prev),
            after=nxt,
            heading=heading,
            space=("MNI" if sp.group(1).lower().startswith(("mni", "montreal")) else "TAL") if sp else None,
        ))
        i = j + 1
    return out


__all__ = ["Hit", "Passage", "find", "passages", "plausible", "sentences", "table_free"]
