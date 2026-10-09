"""An article's reference list and in-text citations, as offsets into its extracted text.

XML marks each citation with its entry's id (JATS `xref`, Elsevier `ce:cross-ref`): the
citation elements are wrapped in sentinels, the extractor's own text builder is re-run on
the marked XML, and where the sentinels land are the markers' offsets. Publisher HTML links
its markers to its list too, but readability's text cannot be rebuilt from the page, so a
link is placed by its own text and the words before it.

Measurements behind the choices are in the wiki and `experiments/references/`.
"""

from __future__ import annotations

import logging
import re
from bisect import bisect_right
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from lxml import etree
from lxml import html as lhtml

logger = logging.getLogger(__name__)

JATS_SOURCES = ("pubget", "pmc", "europepmc")

#: How sure a publisher link's placement is: 23 of 25 read by hand were right, and 96%
#: of links are placed by a 40-character context that occurs once in the text.
LINK_CONFIDENCE = 0.9
LINK_CONFIDENCE_WEAK = 0.7  # a shorter or repeated context

_S, _M, _E = "", "", ""
_DIGITS = {str(d): chr(0xE010 + d) for d in range(10)}
_UNDIGITS = {v: k for k, v in _DIGITS.items()}
_SENTINEL = re.compile("([-]+)|")
_CE = "http://www.elsevier.com/xml/common/dtd"
_SB = "http://www.elsevier.com/xml/common/struct-bib/dtd"
_XLINK = "{http://www.w3.org/1999/xlink}href"


@dataclass
class ReadResult:
    """What one source's markup says about the article's references and citations."""

    references: List[dict]
    citations: List[dict]
    #: counts worth reporting: links the text has no place for, markers it dropped
    notes: Dict[str, int] = field(default_factory=dict)


# ------------------------------------------------------------------ text helpers
def _squash(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", s or "").strip()


def _text_of(el) -> str:
    return _squash("".join(el.itertext())) if el is not None else ""


def _local(el) -> str:
    return etree.QName(el).localname if isinstance(el.tag, str) else ""


def _year(value: Optional[str]) -> Optional[int]:
    m = re.search(r"\b(1[5-9]\d\d|20\d\d)", value or "")
    return int(m.group(1)) if m else None


# -------------------------------------------------------------------- sentences
# pondie's splitter (pondie/extraction/prompt/preprocess.py::sentence_spans), so a
# sentence here is the sentence pondie cuts.
_NON_TERMINAL = frozenset("""
et al e.g i.e cf vs etc approx ca resp viz fig figs tab tabs eq ref refs no nos dr prof
mr mrs ms st inc ltd co univ dept min sec ms mm cm ml mg kg vol ed eds pp al s.d s.e
i.v p.o a.m p.m
""".split()) | frozenset(chr(c) for c in range(ord("a"), ord("z") + 1))
_BOUNDARY = re.compile(r"[.!?][\"')\]]?\s+(?=[\"'(\[]?[A-Z0-9])")
_LAST_WORD = re.compile(r"([A-Za-z][A-Za-z.]*)\.?$")
_MARKERISH = re.compile(r"[\s\[\]\(\),;–—\-0-9]*")


def _ends_mid_sentence(text: str) -> bool:
    word = _LAST_WORD.search(text.rstrip(".!?"))
    return bool(word and word.group(1).lower().rstrip(".") in _NON_TERMINAL)


def sentence_spans(text: str) -> List[Tuple[int, int]]:
    """Every sentence of `text` as (start, end) offsets, in order; a heading or a table row is one."""
    spans: List[Tuple[int, int]] = []
    offset = 0
    for line in text.split("\n"):
        start, stripped = 0, line.strip()
        if stripped and not stripped.startswith(("#", "|")):
            guarded = re.sub(r"(\d)\.(\d)", lambda m: f"{m[1]}\x00{m[2]}", line)
            for boundary in _BOUNDARY.finditer(guarded):
                if _ends_mid_sentence(guarded[start : boundary.start() + 1]):
                    continue
                spans.append((offset + start, offset + boundary.start() + 1))
                start = boundary.end()
        spans.append((offset + start, offset + len(line)))
        offset += len(line) + 1
    trimmed = []
    for begin, end in spans:
        piece = text[begin:end]
        begin += len(piece) - len(piece.lstrip())
        end -= len(piece) - len(piece.rstrip())
        if end - begin > 2:
            trimmed.append((begin, end))
    return trimmed


class Sentences:
    """Which sentence a marker cites from.

    A marker right after a sentence's full stop ("population.[1] Further", or a dropped
    superscript there) belongs to the sentence it follows. That rule decides 3-5% of
    citations; the splitter alone would hand them to the next sentence.
    """

    def __init__(self, text: str):
        self.text = text
        self.spans = sentence_spans(text)
        self.starts = [s for s, _ in self.spans]

    def of(self, pos: int) -> Optional[Tuple[int, int]]:
        text, spans = self.text, self.spans
        k = bisect_right(self.starts, pos) - 1
        if k >= 0 and pos <= spans[k][1]:
            if (k > 0 and _MARKERISH.fullmatch(text[spans[k][0] : pos])
                    and not text[spans[k - 1][1] : spans[k][0]].strip()
                    and text[spans[k - 1][1] - 1] in ".!?"):
                return spans[k - 1]
            return spans[k]
        if k >= 0 and not text[spans[k][1] : pos].strip(" \t"):
            return spans[k]
        if k >= 0 and k + 1 < len(spans) and _MARKERISH.fullmatch(text[pos : spans[k + 1][0]]):
            return spans[k]
        return None


# ------------------------------------------------------------------- alignment
class OffsetMap:
    """Offsets in one rendering of a text, carried onto another that differs in places.

    Lines first (cheap and hashable), then characters inside runs of differing lines.
    An offset inside a replaced run maps to None.
    """

    def __init__(self, a: str, b: str):
        self.blocks: List[Tuple[int, int, int]] = []  # (a_start, b_start, length)
        la, lb = a.splitlines(keepends=True), b.splitlines(keepends=True)
        oa, ob = [0], [0]
        for x in la:
            oa.append(oa[-1] + len(x))
        for x in lb:
            ob.append(ob[-1] + len(x))
        for tag, i1, i2, j1, j2 in SequenceMatcher(None, la, lb, autojunk=False).get_opcodes():
            if tag == "equal":
                self.blocks.append((oa[i1], ob[j1], oa[i2] - oa[i1]))
            elif tag == "replace" and (oa[i2] - oa[i1]) * (ob[j2] - ob[j1]) < 4e8:
                ca, cb = a[oa[i1] : oa[i2]], b[ob[j1] : ob[j2]]
                for blk in SequenceMatcher(None, ca, cb, autojunk=False).get_matching_blocks():
                    if blk.size:
                        self.blocks.append((oa[i1] + blk.a, ob[j1] + blk.b, blk.size))
        self.blocks.sort()
        self.starts = [s for s, _, _ in self.blocks]

    def __call__(self, pos: int) -> Optional[int]:
        k = bisect_right(self.starts, pos) - 1
        if k >= 0:
            a0, b0, n = self.blocks[k]
            if pos <= a0 + n:
                return b0 + pos - a0
            # inside a run only `a` has: it sits where b joins the two blocks
            if k + 1 < len(self.blocks) and self.blocks[k + 1][1] == b0 + n:
                return b0 + n
        return None


def _open(i: int) -> str:
    return _S + "".join(_DIGITS[c] for c in str(i)) + _M


def _wrap(el, i: int) -> None:
    el.text = _open(i) + (el.text or "")
    if len(el):
        el[-1].tail = (el[-1].tail or "") + _E
    else:
        el.text += _E


def _strip(marked: str) -> Tuple[str, Dict[int, Tuple[int, int]]]:
    """The text without sentinels, and each wrapped element's (start, end) in it."""
    out, pos, n, stack, spans = [], 0, 0, [], {}
    for m in _SENTINEL.finditer(marked):
        out.append(marked[pos : m.start()])
        n += m.start() - pos
        pos = m.end()
        if m.group(1):
            stack.append((int("".join(_UNDIGITS[c] for c in m.group(1))), n))
        elif stack:
            i, s = stack.pop()
            spans.setdefault(i, (s, n))
    out.append(marked[pos:])
    return "".join(out), spans


def _place(rebuilt: str, spans: Dict[int, Tuple[int, int]], stored: str, notes: Dict[str, int]):
    """Spans in the rebuilt text, carried onto the stored one where the two differ."""
    if rebuilt == stored:
        return spans
    notes["text_rebuilt_differs"] = 1
    mapping = OffsetMap(rebuilt, stored)
    placed = {}
    for i, (s, e) in spans.items():
        ms, me = mapping(s), mapping(e)
        if ms is not None and me is not None and me >= ms:
            placed[i] = (ms, me)
    return placed


def _citations(text: str, links: List[dict], spans: Dict[int, Tuple[int, int]], method: str,
               notes: Dict[str, int]) -> List[dict]:
    sentences = Sentences(text)
    out = []
    for i, link in enumerate(links):
        if i not in spans:
            notes["links_not_in_text"] = notes.get("links_not_in_text", 0) + 1
            continue
        s, e = spans[i]
        marker = text[s:e]
        in_text = bool(re.search(r"[^\W_]", marker))
        if not in_text:
            # zero width, where the marker was: the sentence still holds
            if link.get("has_text", True):
                notes["markers_not_in_text"] = notes.get("markers_not_in_text", 0) + 1
            marker, e = "", s
        sentence = sentences.of(s)
        out.append({
            "text_span": {"start_char": s, "end_char": e, "text": marker},
            "sentence": {"start_char": sentence[0], "end_char": sentence[1]} if sentence else None,
            "references": link["references"],
            "method": method,
            "confidence": link.get("confidence"),
            "marker_in_text": in_text,
        })
    return out


def _join_ranges(citations: List[dict], references: List[dict], text: str) -> List[dict]:
    """"[3]–[5]" links only its ends: one citation of 3, 4 and 5."""
    number = {}
    for r in references:
        digits = re.sub(r"\D", "", r.get("label") or "")
        if digits:
            number[r["id"]] = int(digits)
    by_number = {v: k for k, v in number.items()}
    out: List[dict] = []
    for c in citations:
        prev = out[-1] if out else None
        if (prev and prev["marker_in_text"] and c["marker_in_text"]
                and len(prev["references"]) == len(c["references"]) == 1
                and re.fullmatch(r"\s*[-–—]\s*", text[prev["text_span"]["end_char"] : c["text_span"]["start_char"]])):
            a, b = number.get(prev["references"][0]), number.get(c["references"][0])
            if a is not None and b is not None and 0 < b - a < 100:
                s, e = prev["text_span"]["start_char"], c["text_span"]["end_char"]
                prev["text_span"] = {"start_char": s, "end_char": e, "text": text[s:e]}
                prev["references"] = [by_number[n] for n in range(a, b + 1) if n in by_number]
                continue
        out.append(c)
    return out


# ------------------------------------------------------------------------ JATS
def _jats_references(root) -> List[dict]:
    refs = []
    for el in root.iter("ref"):
        if not any(_local(a) == "ref-list" for a in el.iterancestors()):
            continue
        cits = [c for c in el if _local(c) in ("mixed-citation", "element-citation", "citation", "nlm-citation")]
        cit = cits[0] if cits else el
        r = {
            "id": el.get("id") or "",
            "position": len(refs) + 1,
            "provider": "source_xml",
            "label": _text_of(el.find("label")) or None,
            "text": _squash(" ".join(_text_of(c) for c in cits)) if cits else _text_of(el),
            "doi": None, "pmid": None, "pmcid": None,
            "title": _text_of(cit.find(".//article-title")) or _text_of(cit.find(".//chapter-title")) or None,
            "year": _year(_text_of(cit.find(".//year"))),
            "authors": [_text_of(x) for x in cit.iter() if _local(x) == "surname"]
            or [_text_of(x) for x in cit.iter() if _local(x) == "collab"][:1],
        }
        for pid in el.iter("pub-id"):
            kind, value = (pid.get("pub-id-type") or "").lower(), _text_of(pid)
            key = {"doi": "doi", "pmid": "pmid", "pmcid": "pmcid", "pmc": "pmcid"}.get(kind)
            if key and not r[key]:
                r[key] = value
        for ext in el.iter("ext-link"):
            href = ext.get(_XLINK) or _text_of(ext)
            if not r["doi"] and "doi.org/10." in href:
                r["doi"] = href.split("doi.org/", 1)[1]
        r["id"] = r["id"] or f"ref{r['position']}"
        refs.append(r)
    return refs


def read_jats(xml_path: Path, text: str) -> ReadResult:
    from ingestion_workflow.extractors.pubget_extractor import article_text

    tree = etree.parse(str(xml_path))
    root = tree.getroot()
    refs = _jats_references(root)
    ids = {r["id"] for r in refs}
    links: List[dict] = []
    dropped: Dict[object, List[str]] = {}
    for x in root.iter("xref"):
        rids = (x.get("rid") or "").split()
        if not (x.get("ref-type") == "bibr" or (rids and all(r in ids for r in rids))):
            continue
        resolved = [r for r in rids if r in ids]
        if not resolved:
            continue
        i = len(links)
        links.append({"references": resolved, "has_text": bool(_text_of(x))})
        # pubget's stylesheet drops <sup> and <sub> whole: anchor such a marker after them
        outer = None
        for a in x.iterancestors():
            if _local(a) in ("sup", "sub"):
                outer = a
        if outer is None:
            _wrap(x, i)
        else:
            dropped.setdefault(outer, []).append(_open(i) + _E)
    for el, tags in dropped.items():
        el.tail = "".join(tags) + (el.tail or "")
    notes: Dict[str, int] = {}
    rebuilt, spans = _strip(article_text(tree, xml_path.parent))
    spans = _place(rebuilt, spans, text, notes)
    citations = _join_ranges(_citations(text, links, spans, "xref", notes), refs, text)
    return ReadResult(refs, citations, notes)


# -------------------------------------------------------------------- Elsevier
def _q(tag: str, ns: str = _CE) -> str:
    return f"{{{ns}}}{tag}"


def _elsevier_references(root) -> List[dict]:
    refs = []
    for el in root.iter(_q("bib-reference")):
        r = {"id": el.get("id") or "", "position": len(refs) + 1, "provider": "source_xml",
             "label": _text_of(el.find(_q("label"))) or None, "doi": None, "pmid": None, "pmcid": None,
             "title": None, "year": None, "authors": []}
        source_text = el.find(".//" + _q("source-text"))
        structured = el.findall(".//" + _q("reference", _SB))
        other = el.findall(".//" + _q("other-ref"))
        r["text"] = (_text_of(source_text) if source_text is not None
                     else _squash(" ".join(_text_of(c) for c in structured + other)) or _text_of(el))
        if structured:
            sb = structured[0]
            contribution = sb.find(_q("contribution", _SB))
            if contribution is not None:
                r["title"] = _text_of(contribution.find(".//" + _q("maintitle", _SB))) or None
            r["year"] = next((y for y in (_year(_text_of(d)) for d in sb.iter(_q("date", _SB))) if y), None)
            r["doi"] = _text_of(sb.find(".//" + _q("doi"))) or None
            authors = sb.find(".//" + _q("authors", _SB))
            if authors is not None:
                r["authors"] = [_text_of(s) for s in authors.iter(_q("surname"))] or \
                    [_text_of(c) for c in authors.iter(_q("collaboration", _SB))][:1]
        if not r["doi"]:
            for link in el.iter(_q("inter-ref")):
                href = link.get(_XLINK) or ""
                if "doi.org/10." in href or href.startswith("doi:"):
                    r["doi"] = re.sub(r"^.*?(10\.)", r"\1", href)
                    break
        r["id"] = r["id"] or f"ref{r['position']}"
        refs.append(r)
    return refs


def read_elsevier(xml_path: Path, text: str) -> ReadResult:
    from elsevier_coordinate_extraction.extract.text import extract_text_from_article, format_article_text

    root = etree.fromstring(xml_path.read_bytes())
    refs = _elsevier_references(root)
    ids = {r["id"] for r in refs}
    links: List[dict] = []
    for x in root.iter(_q("cross-ref"), _q("cross-refs")):
        resolved = [r for r in (x.get("refid") or "").split() if r in ids]
        if resolved:
            links.append({"references": resolved, "has_text": bool(_text_of(x))})
            _wrap(x, len(links) - 1)
    payload = etree.tostring(root, encoding="utf-8", xml_declaration=True)
    # the calls save_article_text makes, as the elsevier extractor runs it
    marked = format_article_text(extract_text_from_article(payload, True, True))
    notes: Dict[str, int] = {}
    rebuilt, spans = _strip(marked)
    spans = _place(rebuilt, spans, text, notes)
    return ReadResult(refs, _citations(text, links, spans, "xref", notes), notes)


# ------------------------------------------------------------------------ HTML
#: Where pages put a reference's id: OUP content-id/data-legacy-id, Wiley data-bib-id.
_ID_ATTRS = ("id", "content-id", "data-legacy-id", "data-bib-id", "data-id")
#: Where a link names its target: OUP reveal-id/data-open, MIT data-modal-source-id, SAGE data-xml-rid.
_TARGET_ATTRS = ("reveal-id", "data-open", "data-modal-source-id", "data-xml-rid", "data-rid")
_BLOCKS = {"p", "div", "li", "td", "th", "section", "dd", "h1", "h2", "h3", "h4", "h5", "h6",
           "figcaption", "caption"}
_YEAR = re.compile(r"\b(1[89]|20)\d\d[a-z]?\b")
_NOT_A_REF = re.compile(r"(^|[-_ ])(fig|figure|tab|tbl|table|sec|section|app|supp|fn|foot)", re.I)
_PLACE_CONTEXT = 40


def _spaced(el) -> str:
    return _squash(" ".join(el.itertext()))


def _link_targets(a) -> List[str]:
    out = []
    href = a.get("href") or ""
    if "#" in href and not href.startswith(("http", "/")):
        out.append(href.split("#", 1)[1])
    for k in _TARGET_ATTRS:
        if a.get(k):
            out.extend(a.get(k).split())
    return out


def _reference_element(el, link_ids):
    """Climb from a link's target to the element holding the whole entry.

    A table, figure, section or footnote is not a reference, however citation-like its
    text ("Table 1 ... 2004", "5. Conclusions ... 2019").
    """
    if el is not None and (el.tag in ("table", "figure", "section", "h1", "h2", "h3", "h4", "h5", "h6")
                           or _NOT_A_REF.search(el.get("id") or "") or el.find(".//table") is not None
                           or any(el.find(f".//h{k}") is not None for k in range(1, 7))):
        return None
    for _ in range(4):
        if el is None:
            return None
        t = _spaced(el)
        if 25 <= len(t) <= 3000 and _YEAR.search(t):
            inner = sum(1 for x in el.iter() if x is not el and any(x.get(k) in link_ids for k in _ID_ATTRS))
            return el if inner <= 3 else None  # a container of several entries is not one
        el = el.getparent()
    return None


class _Found(Exception):
    pass


def _text_before(block, a) -> str:
    out: List[str] = []

    def walk(el):
        if el is a:
            raise _Found
        if el.text:
            out.append(el.text)
        for ch in el:
            walk(ch)
            if ch.tail:
                out.append(ch.tail)

    try:
        walk(block)
    except _Found:
        pass
    return "".join(out)


def _html_reference(el, rid: str, position: int) -> dict:
    t = _spaced(el)
    r = {"id": rid, "position": position, "provider": "source_html", "label": None, "text": t,
         "doi": None, "pmid": None, "pmcid": None, "title": None, "year": _year(t), "authors": []}
    for a in el.iter("a"):
        href = a.get("href") or ""
        m = re.search(r"doi\.org/(10\.[^?#\s]+)", href)
        if m and not r["doi"]:
            r["doi"] = m.group(1)
        m = re.search(r"(?:pubmed/|ncbi\.nlm\.nih\.gov/(?:pubmed/)?|access_num=)(\d{6,9})\b", href)
        if m and not r["pmid"] and ("link_type=MED" in href or "access_num" not in href):
            r["pmid"] = m.group(1)
        m = re.search(r"PMC(\d{5,9})", href)
        if m and not r["pmcid"]:
            r["pmcid"] = "PMC" + m.group(1)
    if not r["doi"]:
        m = re.search(r"\b(10\.\d{4,9}/[^\s\"<>]+[^\s\"<>.,;])", t)
        r["doi"] = m.group(1) if m else None
    return r


class _Letters:
    """A text without whitespace, mapping back to the original's offsets."""

    def __init__(self, text: str):
        self.idx = [i for i, ch in enumerate(text) if not ch.isspace()]
        self.s = "".join(text[i] for i in self.idx)


def read_html(html_path: Path, text: str) -> ReadResult:
    root = lhtml.parse(str(html_path), lhtml.HTMLParser(encoding="utf-8", huge_tree=True)).getroot()
    notes: Dict[str, int] = {}
    if root is None:
        return ReadResult([], [], notes)
    for bad in root.xpath("//script|//style|//noscript"):
        bad.drop_tree()
    by_id = {}
    for el in root.iter():
        if isinstance(el.tag, str):
            for k in _ID_ATTRS:
                v = el.get(k)
                if v and (k == "id" or v not in by_id):
                    by_id[v] = el
    candidates = [(a, t) for a in root.iter("a") for t in _link_targets(a) if t in by_id]
    link_ids = {t for _, t in candidates}
    entry_of = {}
    for a, t in candidates:
        el = _reference_element(by_id[t], link_ids)
        if el is not None and not any(x is a for x in el.iter()):  # a link inside its entry is a back-link
            entry_of[(id(a), t)] = el
    # entries in document order, one per text: pages repeat their list in side panels
    order = {el: i for i, el in enumerate(root.iter())}
    refs: List[dict] = []
    rid_of, by_text = {}, {}
    for el in sorted({id(e): e for e in entry_of.values()}.values(), key=order.get):
        key = _spaced(el)[:200]
        if key not in by_text:
            ref = _html_reference(el, el.get("id") or f"ref{len(refs) + 1}", len(refs) + 1)
            refs.append(ref)
            by_text[key] = ref["id"]
        rid_of[id(el)] = by_text[key]

    letters = _Letters(text)
    sentences = Sentences(text)
    citations, cursor = [], 0
    for a, t in candidates:
        el = entry_of.get((id(a), t))
        if el is None:
            continue
        block = a.getparent()
        while block is not None and block.tag not in _BLOCKS:
            block = block.getparent()
        before = re.sub(r"\s+", "", _text_before(block, a) if block is not None else "")
        marker = re.sub(r"\s+", "", _spaced(a))
        placed = None
        for n in (_PLACE_CONTEXT, 20, 8, 0):
            needle = (before[-n:] if n else "") + marker
            if not needle:
                break
            pos = letters.s.find(needle, cursor)
            if pos < 0:
                pos = letters.s.find(needle)
            if pos >= 0:
                unique = letters.s.find(needle, pos + 1) < 0
                placed = (pos + len(needle) - len(marker), n == _PLACE_CONTEXT and unique)
                break
        if placed is None:
            notes["links_not_in_text"] = notes.get("links_not_in_text", 0) + 1
            continue
        ms, strong = placed
        cursor = ms
        if marker:
            s, e = letters.idx[ms], letters.idx[ms + len(marker) - 1] + 1
        else:
            s = e = letters.idx[ms] if ms < len(letters.idx) else len(text)
        sentence = sentences.of(s)
        citations.append({
            "text_span": {"start_char": s, "end_char": e, "text": text[s:e]},
            "sentence": {"start_char": sentence[0], "end_char": sentence[1]} if sentence else None,
            "references": [rid_of[id(el)]],
            "method": "link",
            "confidence": LINK_CONFIDENCE if strong else LINK_CONFIDENCE_WEAK,
            "marker_in_text": bool(marker),
        })
    return ReadResult(refs, citations, notes)


# -------------------------------------------------------------------- dispatch
def _main_file(source: str, download):
    if source in JATS_SOURCES:
        from ingestion_workflow.extractors.pubget_extractor import _select_article_file

        return _select_article_file(download)
    if source == "elsevier":
        from ingestion_workflow.extractors.elsevier_extractor import _select_content_file

        return _select_content_file(download)
    if source == "ace":
        from ingestion_workflow.extractors.ace_extractor import _select_html_file

        return _select_html_file(download)
    return None


#: Sources whose download marks its citations. A PDF does not.
READABLE_SOURCES = JATS_SOURCES + ("elsevier", "ace")


def read(source: str, download, text: str) -> Optional[ReadResult]:
    """The references and citations one source's download marks, placed in `text`.

    `download` is that source's DownloadResult and `text` the text its extraction kept.
    None when the source marks nothing (a PDF) or its main file is missing.
    """
    f = _main_file(source, download)
    if f is None or not Path(f.file_path).exists():
        return None
    path = Path(f.file_path)
    if source in JATS_SOURCES:
        return read_jats(path, text)
    if source == "elsevier":
        return read_elsevier(path, text)
    return read_html(path, text)
