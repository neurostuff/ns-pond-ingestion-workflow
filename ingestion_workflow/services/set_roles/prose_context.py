"""What the set-role classifier and its labeller read about one PROSE set.

A prose set is a run of coordinates in the text. What it is for is in the
sentences around them ("we placed a 6 mm sphere at ..."; "peaks reported by
Lee et al. (2008)"), the section heading, the paragraph's neighbours and the
citation markers among them. Table sets have their own builder
(`table_context`); the two share only the label vocabulary and point shape.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, List, Mapping, Sequence

from .common import (
    MAX_CHARS,
    as_point,
    citations,
    citing,
    clip,
    cue_summary,
    point_summary,
    sentences,
)
from .common import _SENTENCE

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


def _number(value: Any) -> str:
    """A coordinate as it is written in text: any minus sign, an optional ".0"."""
    v = float(value)
    body = f"{abs(v):g}".replace(".", r"\.")
    sign = r"[-\u2212\u2013]\s?" if v < 0 else r"(?:\+\s?)?"
    return sign + body + (r"(?:\.0+)?" if v == int(v) else "")


def _coordinate_spans(text: str, points: Sequence[Mapping[str, Any]]) -> List[tuple]:
    """Where each point's "x, y, z" is written in `text`, as (start, end) offsets."""
    spans = []
    for point in points:
        try:
            parts = [_number(point[axis]) for axis in "xyz"]
        except (KeyError, TypeError, ValueError):
            continue
        # A number must not be the tail of a longer one ("-142" is not "-42").
        rx = r"(?<![\d.])" + r"(?:[,;/]\s*|\.\s+|\s+)(?:[xyzXYZ]\s*[=:]\s*)?".join(parts) + r"(?![\d])"
        spans += [m.span() for m in re.finditer(rx, text)]
    return spans


def local_text(text: str, points: Sequence[Mapping[str, Any]]) -> str:
    """The sentences of `text` that a point's coordinates overlap, in text order.

    A coordinate triple cut by a sentence break touches both sentences, so
    both are kept. Points not found in the text add nothing.
    """
    spans = _coordinate_spans(text, points)
    if not spans:
        return ""
    edges = [0] + [m.end() for m in _SENTENCE.finditer(text)] + [len(text)]
    out = []
    for a, b in zip(edges, edges[1:]):
        if any(start < b and end > a for start, end in spans):
            out.append(" ".join(text[a:b].split()))
    return " ".join(out)


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
        local=" ".join(
            t for t in (local_text(p.get("text") or "", plain_points) for p in read) if t
        ),
        before=(read[0].get("before") or "") if read else "",
        after=(read[-1].get("after") or "") if read else "",
        points=plain_points,
    )


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
        f"[LOCAL] {clip(context.local, 400)}",
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
