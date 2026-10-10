"""What the set-role classifier and its labeller read about one TABLE set.

A table set is one analysis of a coordinate table. What it is for is in the
caption ("Regions of interest used for the PPI analysis"), the footer, the
header rows, the rows the set was read from, what the table's other analyses
are, and the Methods and Results sentences that cite the table. Prose sets are
read from different evidence and have their own builder (`prose_context`);
the two share only the label vocabulary (`labels`) and point shape (`common`).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Iterable, List, Mapping, Optional, Sequence

from .common import (
    MAX_CHARS,
    as_point,
    citing,
    citing_sentences,
    clip,
    cue_summary,
    point_summary,
    sentences,
)
from .labels import RESULT, SetRole

#: Bump when the serialisation changes: a model records the version it was
#: trained on and the roles stage refuses one built for another.
TABLE_CONTEXT_VERSION = 1

#: How many of the set's rows, header rows and neighbours the input keeps.
MAX_ROWS, MAX_HEADER, MAX_NEIGHBOURS = 8, 3, 6


@dataclass
class TableSetContext:
    """Everything read about one table set."""

    name: str = ""
    description: str = ""
    table_label: str = ""
    caption: str = ""
    footer: str = ""
    #: Header lines of the serialised table (cells marked `#`).
    header: List[str] = field(default_factory=list)
    #: The serialised rows holding this set's coordinates.
    rows: List[str] = field(default_factory=list)
    #: The names of the table's other analyses, with their point counts.
    neighbours: List[str] = field(default_factory=list)
    #: Sentences of the article that cite the table.
    citing: List[str] = field(default_factory=list)
    points: List[Mapping[str, Any]] = field(default_factory=list)
    proposed: SetRole = RESULT

    def cue_text(self) -> str:
        return " ".join([self.name, self.description, self.caption, self.footer, *self.citing])

    def evidence_sentences(self) -> List[str]:
        """The sentences a labeller may cite as evidence, in a fixed order."""
        out: List[str] = []
        for text in (self.caption, self.footer, *self.citing):
            out += [s for s in sentences(text) if s not in out]
        return out


def _numbers(line: str) -> List[float]:
    return [float(n) for n in re.findall(r"(?<![\w.])[-−–]?\d+(?:\.\d+)?", line.replace("−", "-"))]


def split_table(table_text: Optional[str]) -> tuple:
    """(header lines, body lines) of an `nspond_tables` serialisation."""
    header, body = [], []
    for line in (table_text or "").splitlines():
        if not line.strip():
            continue
        cells = [c.strip() for c in line.split(" | ")]
        (header if any(c.startswith("#") for c in cells) and not body else body).append(
            line.strip()
        )
    return header, body


def rows_for(points: Sequence[Any], body: Sequence[str]) -> List[str]:
    """The body lines holding each point's x, y and z, in table order."""
    wanted = []
    for p in map(as_point, points):
        try:
            wanted.append(tuple(round(float(p[k]), 1) for k in ("x", "y", "z")))
        except (TypeError, ValueError, KeyError):
            continue
    out = []
    for line in body:
        numbers = [round(n, 1) for n in _numbers(line)]
        if any(_contains(numbers, triple) for triple in wanted):
            out.append(line)
    return out


def _contains(numbers: List[float], triple: tuple) -> bool:
    return any(tuple(numbers[i : i + 3]) == triple for i in range(len(numbers) - 2))


def neighbour_names(analyses: Iterable[Mapping[str, Any]], index: int) -> List[str]:
    """`name (n)` for every other analysis of the table."""
    return [
        f"{a.get('name') or '?'} ({len(a.get('coordinates') or a.get('points') or [])})"
        for i, a in enumerate(analyses)
        if i != index
    ]


def build(
    analysis: Mapping[str, Any],
    *,
    index: int = 0,
    siblings: Sequence[Mapping[str, Any]] = (),
    table_text: Optional[str] = None,
    article_text: Optional[str] = None,
    caption: Optional[str] = None,
    footer: Optional[str] = None,
    table_label: Optional[str] = None,
) -> TableSetContext:
    """The context of a table analysis, as the analyses payload stores it.

    `siblings` is the table's analyses (this one included, at `index`);
    `table_text` its serialisation, when it can be read.
    """
    meta = (analysis.get("metadata") or {}).get("table_metadata") or {}
    label = (
        table_label
        or meta.get("table_label")
        or (f"Table {analysis['table_number']}" if analysis.get("table_number") else "")
    )
    points = analysis.get("coordinates") or analysis.get("points") or []
    header, body = split_table(table_text)
    return TableSetContext(
        name=analysis.get("name") or "",
        description=analysis.get("description") or "",
        table_label=label,
        caption=caption if caption is not None else analysis.get("table_caption") or "",
        footer=footer if footer is not None else analysis.get("table_footer") or "",
        header=header[:MAX_HEADER],
        rows=rows_for(points, body),
        neighbours=neighbour_names(siblings, index),
        citing=citing_sentences(article_text, label),
        points=[as_point(p) for p in points],
    )


def serialize(context: TableSetContext, max_chars: int = MAX_CHARS) -> str:
    """The classifier's input string for one table set: short fields first."""
    parts = [
        "[ORIGIN] table",
        f"[PROPOSED] {context.proposed.render()}",
        f"[POINTS] {point_summary(context.points)}",
        f"[CUES] {cue_summary(context.cue_text())}",
        f"[NAME] {clip(context.name, 200)}",
        f"[DESCRIPTION] {clip(context.description, 200)}",
        f"[TABLE] {clip(context.table_label, 40)}",
        f"[NEIGHBOURS] {clip('; '.join(context.neighbours[:MAX_NEIGHBOURS]), 300)}",
        f"[CAPTION] {clip(context.caption, 500)}",
        f"[FOOTER] {clip(context.footer, 300)}",
        f"[HEADER] {clip(' / '.join(context.header), 300)}",
        f"[ROWS] {clip(' / '.join(context.rows[:MAX_ROWS]), 600)}",
        *(f"[CITED] {clip(s, 300)}" for s in context.citing[:3]),
    ]
    text = " ".join(part for part in parts if not part.endswith("] "))
    return text if len(text) <= max_chars else text[: max_chars - 1] + "…"


def prior_evidence(context: TableSetContext, limit: int = 2) -> List[str]:
    """Sentences citing another publication: caption and footer, then those citing the table."""
    return citing(
        [context.caption, context.footer, *context.citing, context.name, context.description],
        limit,
    )
