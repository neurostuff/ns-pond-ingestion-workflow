"""What the set-role classifier and its labeller read about one PROSE set.

A prose set is a run of coordinates in the text. What it is for is in the
sentences around them ("we placed a 6 mm sphere at ..."; "peaks reported by
Lee et al. (2008)"), the section heading, the paragraph's neighbours and the
citation markers among them. Table sets have their own builder
(`table_context`); the two share only the label vocabulary and point shape.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, List, Mapping, Optional, Sequence

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
from .labels import label_from_prose

#: Bump when the serialisation changes (see `table_context.TABLE_CONTEXT_VERSION`).
PROSE_CONTEXT_VERSION = 1

#: `[PROPOSED]` for a training set no model proposed a role for.
NO_PROPOSAL = "unknown"

_FROM_ANALYSIS = object()


@dataclass
class ProseSetContext:
    """Everything read about one prose set."""

    name: str = ""
    description: str = ""
    heading: str = ""
    #: The passage holding the coordinates.
    passage: str = ""
    before: str = ""
    after: str = ""
    points: List[Mapping[str, Any]] = field(default_factory=list)
    proposed: str = "result"

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


def build(
    analysis: Mapping[str, Any],
    passages: Sequence[Mapping[str, Any]] = (),
    *,
    passage: Mapping[str, Any] = None,
    proposed: Any = _FROM_ANALYSIS,
) -> ProseSetContext:
    """The context of a prose analysis: its passage(s), their heading and neighbours.

    The prose stage's sets name their passages by index (`metadata.passages`);
    a training row that is itself one passage is given as `passage`. The proposal
    is the prose model's role on the analysis unless `proposed` gives it (a
    training row passes the prose model's role, or None when it has none).
    """
    meta = analysis.get("metadata") or {}
    read = (
        [passage]
        if passage is not None
        else [passages[i] for i in meta.get("passages", []) if 0 <= i < len(passages)]
    )
    points = analysis.get("coordinates") or analysis.get("points") or []
    role: Optional[str] = (
        meta.get("role")
        or next((p.get("role") for p in points if isinstance(p, dict) and p.get("role")), None)
        if proposed is _FROM_ANALYSIS
        else proposed
    )
    return ProseSetContext(
        name=analysis.get("name") or "",
        description=analysis.get("description") or "",
        heading=next((p.get("heading") or "" for p in read if p.get("heading")), ""),
        passage=" ".join(p.get("text") or "" for p in read),
        before=(read[0].get("before") or "") if read else "",
        after=(read[-1].get("after") or "") if read else "",
        points=[as_point(p) for p in points],
        proposed=NO_PROPOSAL if proposed is None else label_from_prose(role),
    )


def serialize(context: ProseSetContext, max_chars: int = MAX_CHARS) -> str:
    """The classifier's input string for one prose set: short fields first."""
    parts = [
        "[ORIGIN] text",
        f"[PROPOSED] {context.proposed}",
        f"[POINTS] {point_summary(context.points)}",
        f"[CUES] {cue_summary(context.cue_text())}",
        f"[NAME] {clip(context.name, 200)}",
        f"[DESCRIPTION] {clip(context.description, 200)}",
        f"[HEADING] {clip(context.heading, 120)}",
        f"[CITATIONS] {clip('; '.join(context.citations()[:6]), 200)}",
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
