"""What the set-role classifier and its labeller read about one PROSE set.

A prose set is a run of coordinates in the text. What it is for is in the
sentences around them ("we placed a 6 mm sphere at ..."; "peaks reported by
Lee et al. (2008)"), the section heading, the paragraph's neighbours and the
citation markers among them. Table sets have their own builder
(`table_context`); the two share only the label vocabulary and point shape.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, List, Mapping, Sequence

from ingestion_workflow.services import coordinate_text

from .common import (
    MAX_CHARS,
    SENTENCE,
    as_point,
    citations,
    citing,
    clip,
    cue_summary,
    point_summary,
    sentences,
)

#: Bump when the serialisation changes (see `table_context.TABLE_CONTEXT_VERSION`).
PROSE_CONTEXT_VERSION = 4


@dataclass
class ProseSetContext:
    """Everything read about one prose set."""

    name: str = ""
    description: str = ""
    heading: str = ""
    #: The passage holding the coordinates.
    passage: str = ""
    #: The sentence(s) of the passage holding this set's coordinates.
    local: str = ""
    before: str = ""
    after: str = ""
    points: List[Mapping[str, Any]] = field(default_factory=list)

    def cue_text(self) -> str:
        return " ".join(
            [self.name, self.description, self.heading, self.passage, self.before, self.after]
        )

    def citations(self) -> List[str]:
        return citations(" ".join([self.before, self.passage, self.after]))

    def evidence_sentences(self) -> List[str]:
        """The sentences a labeller may cite as evidence, in a fixed order."""
        out: List[str] = []
        for text in (self.before, self.passage, self.after):
            out += [s for s in sentences(text) if s not in out]
        return out


#: Longest LOCAL: the set's coordinates and the words around them.
LOCAL_CHARS = 400


def local_text(text: str, points: Sequence[Mapping[str, Any]], limit: int = LOCAL_CHARS) -> str:
    """The sentences of `text` printing the set's coordinates, in order, in `limit` chars."""
    return _join([(text, piece) for piece in _pieces(text, points)], limit)


def _pieces(text: str, points: Sequence[Mapping[str, Any]]) -> List[tuple]:
    """(start, end, first, last) for each run of sentences printing some of the points.

    `first` and `last` bound the coordinates in the run. A triple cut by a
    sentence break joins both sentences. Points not found add nothing.
    """
    spans = sorted(s for s in coordinate_text.find_points(points, text) if s)
    edges = [0] + [m.end() for m in SENTENCE.finditer(text)] + [len(text)]
    runs: List[list] = []
    for a, b in spans:
        lo = max(e for e in edges if e <= a)
        hi = min(e for e in edges if e >= b)
        if runs and lo <= runs[-1][1]:
            runs[-1][1:] = [max(hi, runs[-1][1]), runs[-1][2], max(b, runs[-1][3])]
        else:
            runs.append([lo, hi, a, b])
    return [tuple(r) for r in runs]


def _join(pieces: Sequence[tuple], limit: int) -> str:
    """The pieces' text, each given an equal share of `limit`.

    A piece longer than its share is cut around its coordinates (`_window`), so
    a long sentence, or a list of other sets' coordinates, does not push the
    set's own out.
    """
    if not pieces:
        return ""
    share = (limit + 1) // len(pieces) - 1
    out = []
    for text, (lo, hi, first, last) in pieces:
        if hi - lo > share:
            lo, hi = _window(text, lo, hi, first, last, share)
        out.append(" ".join(text[lo:hi].split()))
    return " ".join(t for t in out if t)


def _window(text: str, lo: int, hi: int, first: int, last: int, limit: int) -> tuple:
    """`limit` chars of `text[lo:hi]` centred on `text[first:last]`, cut at word breaks.

    Coordinates longer than `limit` keep their start.
    """
    pad = max(0, limit - (last - first)) // 2
    start, end = first - pad, max(last + pad, first + limit - pad)
    if start < lo:
        start, end = lo, end + lo - start
    if end > hi:
        start, end = max(lo, start - (end - hi)), hi
    end = min(end, start + limit)
    # A cut word is dropped, never a coordinate.
    while start > lo and start < first and not text[start - 1].isspace():
        start += 1
    while end < hi and end > last and not text[end].isspace():
        end -= 1
    return start, end


def build(
    analysis: Mapping[str, Any],
    passages: Sequence[Mapping[str, Any]] = (),
    *,
    passage: Mapping[str, Any] = None,
) -> ProseSetContext:
    """The context of a prose analysis: its passage(s), their heading and neighbours.

    The prose stage's sets name their passages by index (`metadata.passages`);
    a training row that is itself one passage is given as `passage`.
    """
    meta = analysis.get("metadata") or {}
    read = (
        [passage]
        if passage is not None
        else [passages[i] for i in meta.get("passages", []) if 0 <= i < len(passages)]
    )
    points = analysis.get("coordinates") or analysis.get("points") or []
    plain_points = [as_point(p) for p in points]
    return ProseSetContext(
        name=analysis.get("name") or "",
        description=analysis.get("description") or "",
        heading=next((p.get("heading") or "" for p in read if p.get("heading")), ""),
        passage=" ".join(p.get("text") or "" for p in read),
        local=_local(read, plain_points),
        before=(read[0].get("before") or "") if read else "",
        after=(read[-1].get("after") or "") if read else "",
        points=plain_points,
    )


def _local(read: Sequence[Mapping[str, Any]], points: Sequence[Mapping[str, Any]]) -> str:
    texts = [p.get("text") or "" for p in read]
    return _join([(t, piece) for t in texts for piece in _pieces(t, points)], LOCAL_CHARS)


def serialize(context: ProseSetContext, max_chars: int = MAX_CHARS) -> str:
    """The classifier's input string for one prose set: short fields first."""
    parts = [
        "[ORIGIN] text",
        f"[POINTS] {point_summary(context.points)}",
        f"[CUES] {cue_summary(context.cue_text())}",
        f"[NAME] {clip(context.name, 200)}",
        f"[DESCRIPTION] {clip(context.description, 200)}",
        f"[HEADING] {clip(context.heading, 120)}",
        f"[CITATIONS] {clip('; '.join(context.citations()[:6]), 200)}",
        f"[LOCAL] {clip(context.local, LOCAL_CHARS)}",
        f"[PASSAGE] {clip(context.passage, 900)}",
        f"[BEFORE] {clip(context.before, 400)}",
        f"[AFTER] {clip(context.after, 400)}",
    ]
    text = " ".join(part for part in parts if not part.endswith("] "))
    return text if len(text) <= max_chars else text[: max_chars - 1] + "…"


def prior_evidence(context: ProseSetContext, limit: int = 2) -> List[str]:
    """Sentences citing another publication: the passage, then its neighbours."""
    return citing(
        [context.passage, context.before, context.after, context.name, context.description], limit
    )
