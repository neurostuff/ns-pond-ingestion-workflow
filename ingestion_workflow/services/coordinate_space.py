"""Read a table's coordinate space from the article's prose.

For the tables whose space nothing states (null, or `OTHER` in payloads written
before null existed). The table's own caption and footer
are read first, then the Methods, then the Results -- the sections that say
what this paper did. The introduction and discussion describe other studies,
the front matter carries affiliations ("Montreal Neurological Institute"), and
the references cite Talairach & Tournoux whatever space a paper used.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

from ingestion_workflow.models import CoordinateSpace

__all__ = ["SpaceReading", "findings", "read_space", "space_in", "sectionize"]

# --- sections ----------------------------------------------------------------

#: Adapted from pondie's `evidence.retrieval`. Matched against the lowercased
#: heading with numbering and punctuation stripped, in order.
_TOP_LEVEL: List[Tuple[str, str]] = [
    (r"materials?\s+and\s+methods?|methods?\s+and\s+materials?", "methods"),
    (r"^(?:methods?|methodology|experimental\s+procedures?|patients?\s+and\s+methods?|"
     r"subjects?\s+and\s+methods?)$", "methods"),
    (r"^(?:results?|findings?|imaging\s+results|results?\s+and\s+discussion)$", "results"),
    (r"^(?:discussion|conclusions?|limitations?|general\s+discussion|summary)$", "discussion"),
    (r"^(?:abstract|objectives?|background\s+and\s+aims)$", "abstract"),
    (r"^(?:introduction|background)$", "intro"),
    (r"^(?:references?|bibliography|literature\s+cited|acknowledge?ments?|funding|"
     r"conflicts?\s+of\s+interest|competing\s+interests?|author\s+contributions?|"
     r"supplementary\s+(?:material|data|information)|data\s+availability)$", "back"),
]

#: Headings that name a Methods subsection. Taken only where they are not
#: nested inside another section -- "### Task performance" under Results is
#: still Results.
_METHODS_SUBSECTION = re.compile(
    r"participants?|subjects?|procedures?|acquisition|data\s+analysis|"
    r"statistical\s+analys|image\s+processing|pre-?processing|paradigm|"
    r"stimul|task|apparatus|measures?|instruments?|scanning|imaging\s+(?:parameters|protocol)"
)

_ATX = re.compile(r"^(#{1,6})[ \t]*(.+?)[ \t]*$", re.MULTILINE)
#: A bare line short enough to be a heading. ACE and PDF text has no markup, so
#: only the top-level names count here: a short line in prose or in an inlined
#: table ("Task", "Stimuli") is too easily something else.
_BARE = re.compile(r"^[ \t]*(\S[^\n]{0,58}?)[ \t]*$", re.MULTILINE)


def _canon(heading: str) -> str:
    text = re.sub(r"^(?:[#\d.\s]+|[IVX]+\.\s*)", "", heading).strip(" .:#*_").lower()
    return re.sub(r"\s+", " ", text)


def _top_level(heading: str) -> Optional[str]:
    text = _canon(heading)
    for pattern, label in _TOP_LEVEL:
        if re.search(pattern, text):
            return label
    return None


def sectionize(text: str) -> List[Tuple[int, int, str]]:
    """`(start, end, label)` spans covering the text, in order.

    Markdown headings when the text has them (pubget, Elsevier); otherwise
    bare lines naming a top-level section (ACE, PDF). A heading that names no
    section inherits the one it sits in. Text before the first heading is
    labelled `front`: title, authors, affiliations, abstract.
    """
    marks: List[Tuple[int, str]] = []
    atx = list(_ATX.finditer(text))
    if atx:
        top_depth: Optional[int] = None
        for match in atx:
            depth = len(match.group(1))
            label = _top_level(match.group(2))
            if label:
                top_depth = depth if top_depth is None else min(top_depth, depth)
            elif (top_depth is None or depth <= top_depth) and _METHODS_SUBSECTION.search(
                _canon(match.group(2))
            ):
                label = "methods"
            if label:
                marks.append((match.start(), label))
    else:
        for match in _BARE.finditer(text):
            label = _top_level(match.group(1))
            if label:
                marks.append((match.start(), label))

    if not marks:
        return [(0, len(text), "unknown")]
    spans: List[Tuple[int, int, str]] = []
    if marks[0][0] > 0:
        spans.append((0, marks[0][0], "front"))
    for index, (start, label) in enumerate(marks):
        end = marks[index + 1][0] if index + 1 < len(marks) else len(text)
        spans.append((start, end, label))
    return spans


# --- naming a space ----------------------------------------------------------

#: Case matters for the abbreviations: `MNI` and `ICBM` are capitals in print,
#: and lower case is how `mni2tal` and `icbm2tal` are spelled.
_MNI = (
    r"(?:(?-i:(?<![A-Za-z])(?:MNI|ICBM)(?![a-z]))"
    r"|(?i:\bMontreal\s+Neurologic(?:al)?\s+Institute\b))"
)
#: Talairach and its common misspellings: Tailarach, Talaraich, Talariach, Talairch.
_TAL = (
    r"(?:(?i:\bta(?:lai|ila|lia|la|ilai)r(?:a|ai|ia)?ch\b)"
    r"|(?-i:(?<![A-Za-z])TAL)(?=[\s-]*(?:space|coordinates?|template|atlas)\b))"
)
_SPACE = rf"(?P<space>{_MNI}|{_TAL})"
_MNI_RE = re.compile(_MNI)
_TAL_RE = re.compile(_TAL)

_STANDARD = r"(?:(?:standard|stereota[cx]tic|common|normali[sz]ed|reference)\s+){0,2}"
_COORDS = (
    r"(?:co-?ordinates?|foci|focus|peaks?|maxima|locations?|activations?|clusters?|"
    r"results|x\s*,\s*y\s*,\s*z)"
)

#: Each names the space the reported coordinates are in. In rank order: a
#: sentence about the coordinates themselves beats one about the images.
_REPORTED = re.compile(
    rf"{_COORDS}\b[^.;]{{0,60}}?\b(?:are|were|is|was|be)\s+(?:all\s+)?"
    r"(?:reported|given|presented|listed|shown|expressed|displayed|provided|"
    r"labell?ed|defined)\s+(?:here\s+)?(?:in|using|according\s+to|with\s+respect\s+to)\s+"
    rf"(?:the\s+)?{_STANDARD}{_SPACE}",
    re.IGNORECASE,
)
_CONVERTED = re.compile(
    r"\b(?:convert|transform|translat|transpos|mapp?|warp)\w*\b[^.;]{0,80}?"
    rf"\b(?:in)?to\s+(?:the\s+)?(?:corresponding\s+)?{_STANDARD}{_SPACE}",
    re.IGNORECASE,
)
#: "Talairach coordinates were reported into MNI space" -- into, not in.
_PUT_INTO = re.compile(
    rf"\b(?:reported|brought|put|placed)\s+into\s+(?:the\s+)?{_STANDARD}{_SPACE}", re.IGNORECASE
)
_CONVERTER = re.compile(r"\b(?P<space>mni2tal|icbm2tal|tal2mni|tal2icbm)\b", re.IGNORECASE)
#: "Talairach coordinates", "coordinates in MNI space", "MNI (x, y, z)".
_IN_SPACE = (
    re.compile(
        rf"{_SPACE}[\s-]*(?:\(?\s*(?:19|20)\d\d\s*\)?\s*)?(?:space\s+|stereota[cx]tic\s+|template\s+)?"
        r"(?:co-?ordinates?|coordinate\s+system|\(?\s*x\s*,\s*y\s*,\s*z)",
        re.IGNORECASE,
    ),
    re.compile(
        rf"{_COORDS}\s+(?:are\s+|were\s+)?(?:in|of|from|within)\s+(?:the\s+)?{_STANDARD}{_SPACE}",
        re.IGNORECASE,
    ),
)
_NORMALISED = re.compile(
    r"\b(?:normali[sz]|regist|co-?regist|spatially\s+transform|warp)\w*\b[^.;]{0,80}?"
    rf"\b(?:in)?to\s+(?:the\s+|a\s+)?{_STANDARD}{_SPACE}",
    re.IGNORECASE,
)

RANKS = ("reported", "converted", "in-space", "normalised", "named")

#: A conversion done to look up a label, not to report in. The coordinates
#: stay where they were.
_TOOLS = r"(?:transform|conver|mapp|mni2tal|icbm2tal|tal2mni|tal2icbm|lancaster|brett)"
_FOR_LABELS = re.compile(
    # converted ... to find / for labelling / in order to determine
    r"\b(?:for|to|in\s+order\s+to|so\s+as\s+to)\s+(?:the\s+|an?\s+)?(?:purposes?\s+of\s+)?"
    r"(?:anatomical(?:ly)?\s+|regional\s+|structural\s+)?"
    r"(?:label|locali[sz]|identif|determin|assign|look\s*up|find|obtain|interpret|use\s+the\s+talairach)\w*"
    # labelled ... by / using a transformation
    r"|\b(?:label|identif|determin|assign|locali[sz])\w*[^.;]{0,60}?\b(?:by|via|using|after|following)\s+"
    rf"(?:\S+\s+){{0,3}}?{_TOOLS}"
    r"|\b(?:talairach\s+)?da?emon\b|\btalairach\s+(?:client|applet)\b",
    re.IGNORECASE,
)
#: Named, but not as a coordinate space: atlas look-up tools, FreeSurfer's
#: affine step, tissue priors, a list of what a meta-analysis accepted, and
#: the institute as a place.
_NOT_A_SPACE = re.compile(
    r"\bta(?:lai|ila)rach[\s-]*(?:like|da?emon|client|applet|labels?|\.xfm)\b"
    r"|\bICBM\s+(?:tissue|probabilistic|452)\b|\btissue\s+probabilit"
    r"|(?:MNI|ta(?:lai|ila)rach)\s*(?:,|or|and|/|and/or)\s*(?:the\s+)?(?:MNI|ta(?:lai|ila)rach)"
    r"|(?:MNI|ta(?:lai|ila)rach)[^.;]{0,40}?(?:\bor\b|\beither\b)[^.;]{0,30}?"
    r"(?:MNI|montreal\s+neurologic(?:al)?\s+institute(?:\s*\(MNI\))?|ta(?:lai|ila)rach)"
    r"|\b(?:at|from)\s+the\s+Montreal\s+Neurologic(?:al)?\s+Institute(?:\s*\(MNI\))?"
    r"|\bMontreal\s+Neurologic(?:al)?\s+Institute\b[^\n.]{0,60}\b(?:McGill|Qu[eé]bec|Canada|Brain\s+Imaging\s+Cent)",
    re.IGNORECASE,
)
#: A FreeSurfer step list: its "transformation to Talairach" is an affine
#: the reconstruction uses, not where anything is reported.
_FREESURFER = re.compile(
    r"free\s*surfer|recon-?all|tessellat|topolog\w*\s+correct|intensity\s+(?:inhomogeneity\s+)?normali[sz]ation"
    r"|watershed|surface\s+deformation|automated\s+talairach|white\s+matter\s+based\s+on\s+intensity",
    re.IGNORECASE,
)

_SENTENCE = re.compile(r"(?<=[.!?])\s+(?=[A-Z(\[])|\n\s*\n")


@dataclass(frozen=True)
class SpaceReading:
    """A space, where it was read, and the words it was read from."""

    space: CoordinateSpace
    where: str
    rule: str
    evidence: str


def _which(token: str) -> CoordinateSpace:
    lowered = token.lower()
    if lowered in ("mni2tal", "icbm2tal"):
        return CoordinateSpace.TALAIRACH
    if lowered in ("tal2mni", "tal2icbm"):
        return CoordinateSpace.MNI
    return CoordinateSpace.MNI if _MNI_RE.fullmatch(token) else CoordinateSpace.TALAIRACH


def _other(space: CoordinateSpace) -> CoordinateSpace:
    return CoordinateSpace.TALAIRACH if space is CoordinateSpace.MNI else CoordinateSpace.MNI


def _sentences(passage: str) -> Iterable[str]:
    for sentence in _SENTENCE.split(passage):
        sentence = " ".join(sentence.split())
        if sentence:
            yield sentence


def _spaces(pattern: "re.Pattern[str]", sentence: str) -> List[CoordinateSpace]:
    return [_which(match.group("space")) for match in pattern.finditer(sentence)]


def findings(passage: str) -> Dict[str, List[Tuple[CoordinateSpace, str]]]:
    """Every space the passage names, by rank, with the sentence that named it."""
    found: Dict[str, List[Tuple[CoordinateSpace, str]]] = {}

    def add(rule: str, spaces: Iterable[CoordinateSpace], sentence: str) -> None:
        for space in spaces:
            found.setdefault(rule, []).append((space, sentence))

    for raw in _sentences(passage):
        sentence = _NOT_A_SPACE.sub(" ", raw)
        if _FREESURFER.search(sentence):
            sentence = _TAL_RE.sub(" ", sentence)
        reported = _spaces(_REPORTED, sentence)
        converted = [
            x for pattern in (_CONVERTED, _PUT_INTO, _CONVERTER)
            for x in _spaces(pattern, sentence)
        ]
        # "When foci were reported in Talairach, they were transformed into
        # MNI": the report is the condition, the conversion is the answer.
        if not (converted and set(reported) - set(converted)):
            add("reported", reported, raw)
        if converted and _FOR_LABELS.search(raw):
            # "MNI coordinates were converted to Talairach to find the
            # Brodmann area": the coordinates stay in MNI. Said only when the
            # sentence names the space they came from.
            named = set(_spaces(re.compile(_SPACE), sentence))
            add("converted", [_other(t) for t in set(converted) if _other(t) in named], raw)
            # Nor is the space it was converted to named as an answer.
            for target in set(converted):
                named_as = _TAL_RE if target is CoordinateSpace.TALAIRACH else _MNI_RE
                sentence = named_as.sub(" ", sentence)
        elif converted:
            add("converted", converted, raw)
        else:
            # The source of a conversion is named beside "coordinates" too, so
            # this rank only reads sentences that convert nothing.
            add("in-space", [x for pattern in _IN_SPACE for x in _spaces(pattern, sentence)], raw)
        add("normalised", _spaces(_NORMALISED, sentence), raw)
        add("named", [_which(m.group(0)) for m in _MNI_RE.finditer(sentence)], raw)
        add("named", [_which(m.group(0)) for m in _TAL_RE.finditer(sentence)], raw)
    return found


def space_in(passage: str, where: str = "") -> Optional[SpaceReading]:
    """The space one passage puts its coordinates in, or None.

    The strongest rank that names a space decides. Two spaces at that rank
    are no answer: a paper that reports in both cannot be decided here, and
    guessing would put half its coordinates in the wrong brain.
    """
    return _decide(findings(passage), where)


def _decide(found, where: str) -> Optional[SpaceReading]:
    for rule in RANKS:
        readings = found.get(rule)
        if not readings:
            continue
        if len({space for space, _ in readings}) > 1:
            return None
        space, sentence = readings[0]
        return SpaceReading(space, where, rule, sentence[:300])
    return None


def read_space(
    text: Optional[str],
    *,
    caption: str = "",
    footer: str = "",
) -> Optional[SpaceReading]:
    """The space a table's coordinates are in, read from the article.

    The table's own caption and footer, then the Methods, then the Results.
    The first that names a space decides; one that names both decides too, as
    no answer, because reading on would let a later section outvote the one
    that knows.
    """
    scopes: List[Tuple[str, str]] = [("table", f"{caption}\n\n{footer}")]
    if text:
        sections = sectionize(text)
        for label in ("methods", "results"):
            passage = "\n\n".join(
                text[start:end] for start, end, name in sections if name == label
            )
            scopes.append((label, passage))
    for where, passage in scopes:
        found = findings(passage)
        if found:
            return _decide(found, where)
    return None
