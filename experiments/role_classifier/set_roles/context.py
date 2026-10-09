"""The text a set-role classifier reads for one coordinate set.

A coordinate set is one analysis of the parse: a table's row group or a run of
prose points. What it is for is rarely in the numbers. It is in the caption
("Regions of interest used for the PPI analysis"), the sentence that cites the
table, the heading and neighbours of a passage, and a citation beside the
coordinates. This module gathers those deterministically and serialises them
into one tagged string, short fields first, so truncation to the encoder's
length cuts the long free text and never the structure.

The same function builds training and inference inputs, so a model never sees
a context shaped differently from the ones it learned on.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from .labels import label_from_prose

#: Bump when the serialisation changes: a model is trained against one version
#: and the roles stage refuses a model built for another.
CONTEXT_VERSION = 1

#: Characters, not tokens, so the builder needs no tokenizer. About 512
#: tokens of English.
MAX_CHARS = 2400

_CITATION = re.compile(
    r"\(\s*[A-Z][A-Za-z'\-]+(?:\s+(?:et\s+al\.?|and|&)\s*[A-Za-z'\-]*)?,?\s+(?:19|20)\d{2}[a-z]?"
    r"|\[\s*\d+(?:\s*[,–\-]\s*\d+)*\s*\]"
    r"|\b[A-Z][A-Za-z'\-]+\s+et\s+al\.?\s*\(?(?:19|20)\d{2}"
)
_PRIOR_CUES = re.compile(
    r"\b(previous(?:ly)?|prior|earlier|reported\s+(?:by|in)|based\s+on|adapted\s+from|derived\s+from"
    r"|taken\s+from|as\s+(?:described|defined)\s+(?:by|in)|meta-?analys[ie]s|neurosynth|neuroquery"
    r"|literature|independent\s+(?:study|sample|dataset))\b",
    re.I,
)
_ANCHOR_CUES = re.compile(
    r"\b(roi|rois|region[s]?\s+of\s+interest|seed[s]?|sphere[s]?|mask[s]?|centred|centered"
    r"|stimulat\w*|tms|tdcs|electrode[s]?|target(?:ed|s)?|node[s]?|parcel\w*|a\s+priori)\b",
    re.I,
)
_DISPLAY_CUES = re.compile(r"\b(slice[s]?|crosshair[s]?|displayed|shown\s+at|overlaid|axial|coronal|sagittal)\b", re.I)
_SENTENCE = re.compile(r"(?<=[.!?])\s+(?=[A-Z(\[])")


@dataclass
class SetContext:
    """Everything the classifier reads about one set."""

    origin: str  # "table" or "text"
    name: str = ""
    description: str = ""
    table_label: str = ""
    caption: str = ""
    footer: str = ""
    citing: List[str] = field(default_factory=list)
    heading: str = ""
    passage: str = ""
    before: str = ""
    after: str = ""
    proposed: str = "result"
    points: List[Mapping[str, Any]] = field(default_factory=list)

    def cue_text(self) -> str:
        return " ".join([self.name, self.description, self.caption, self.footer, *self.citing,
                         self.heading, self.passage, self.before, self.after])


def _clip(text: Optional[str], limit: int) -> str:
    text = re.sub(r"\s+", " ", text or "").strip()
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "…"


def point_summary(points: Sequence[Mapping[str, Any]]) -> str:
    """The shape of a set's points as tokens: what an ROI list and a peak list differ in.

    Peaks carry statistics and cluster sizes; ROI centres and seeds usually do
    not, come in fewer, and are often printed as left/right pairs.
    """
    n = len(points)
    if not n:
        return "n=0"
    xyz = [(float(p["x"]), float(p["y"]), float(p["z"])) for p in points]
    kinds: Dict[str, int] = {}
    values = [p.get("statistic_value") for p in points]
    for p in points:
        kind = (p.get("statistic_type") or "none").lower()
        kinds[kind] = kinds.get(kind, 0) + 1
    with_value = [v for v in values if isinstance(v, (int, float))]
    negative = sum(1 for v in with_value if v < 0)
    mirrored = sum(
        1 for i, (x, y, z) in enumerate(xyz)
        if any(j != i and abs(x + u) <= 2 and abs(y - v) <= 4 and abs(z - w) <= 4 and abs(x) > 2
               for j, (u, v, w) in enumerate(xyz))
    )
    centre = [sum(c[i] for c in xyz) / n for i in range(3)]
    spread = sum(math.dist(c, centre) for c in xyz) / n
    integral = sum(1 for c in xyz if all(float(v).is_integer() for v in c))
    return " ".join([
        f"n={n}",
        "stats=" + ",".join(f"{k}:{v}" for k, v in sorted(kinds.items())),
        f"valued={len(with_value)}/{n}",
        f"negative={negative}",
        f"clusters={sum(1 for p in points if p.get('cluster_size') is not None)}",
        f"subpeaks={sum(1 for p in points if p.get('is_subpeak'))}",
        f"seeds={sum(1 for p in points if p.get('is_seed'))}",
        f"mirrored={mirrored}",
        f"integral={integral}",
        f"spread={round(spread)}mm",
    ])


def cue_summary(context: SetContext) -> str:
    text = context.cue_text()
    return " ".join([
        f"citations={len(_CITATION.findall(text))}",
        f"prior_words={len(_PRIOR_CUES.findall(text))}",
        f"anchor_words={len(_ANCHOR_CUES.findall(text))}",
        f"display_words={len(_DISPLAY_CUES.findall(text))}",
    ])


def serialize(context: SetContext, max_chars: int = MAX_CHARS) -> str:
    """The classifier's input string for one set."""
    head = [
        f"[ORIGIN] {context.origin}",
        f"[PROPOSED] {context.proposed}",
        f"[POINTS] {point_summary(context.points)}",
        f"[CUES] {cue_summary(context)}",
        f"[NAME] {_clip(context.name, 200)}",
    ]
    if context.description:
        head.append(f"[DESCRIPTION] {_clip(context.description, 200)}")
    if context.origin == "table":
        body = [
            f"[TABLE] {_clip(context.table_label, 40)}",
            f"[CAPTION] {_clip(context.caption, 500)}",
            f"[FOOTER] {_clip(context.footer, 400)}",
            *(f"[CITED] {_clip(s, 300)}" for s in context.citing[:3]),
        ]
    else:
        body = [
            f"[HEADING] {_clip(context.heading, 120)}",
            f"[PASSAGE] {_clip(context.passage, 900)}",
            f"[BEFORE] {_clip(context.before, 400)}",
            f"[AFTER] {_clip(context.after, 400)}",
        ]
    text = " ".join(head + [part for part in body if not part.endswith("] ")])
    return text if len(text) <= max_chars else text[: max_chars - 1] + "…"


def citing_sentences(text: Optional[str], label: Optional[str], limit: int = 3) -> List[str]:
    """Sentences of the article that cite a table by its label ("Table 2", "Table S1")."""
    if not text or not label:
        return []
    match = re.search(r"(?:supplementa\w*\s+)?table\s*(s?\d+[a-z]?)", label, re.I)
    if not match:
        return []
    pattern = re.compile(rf"\btables?\s*{re.escape(match.group(1))}\b", re.I)
    out = []
    for sentence in _SENTENCE.split(text):
        if pattern.search(sentence):
            out.append(sentence.strip())
            if len(out) >= limit:
                break
    return out


def table_context(analysis: Mapping[str, Any], article_text: Optional[str] = None) -> SetContext:
    """The context of a table analysis, as the analyses payload stores it."""
    meta = (analysis.get("metadata") or {}).get("table_metadata") or {}
    label = meta.get("table_label") or (f"Table {analysis['table_number']}"
                                        if analysis.get("table_number") else "")
    return SetContext(
        origin="table",
        name=analysis.get("name") or "",
        description=analysis.get("description") or "",
        table_label=label,
        caption=analysis.get("table_caption") or "",
        footer=analysis.get("table_footer") or "",
        citing=citing_sentences(article_text, label),
        proposed="result",
        points=analysis.get("coordinates") or [],
    )


def prose_context(analysis: Mapping[str, Any], passages: Sequence[Mapping[str, Any]]) -> SetContext:
    """The context of a prose analysis: its passages, their heading and neighbours."""
    meta = analysis.get("metadata") or {}
    read = [passages[i] for i in meta.get("passages", []) if 0 <= i < len(passages)]
    return SetContext(
        origin="text",
        name=analysis.get("name") or "",
        description=analysis.get("description") or "",
        heading=next((p.get("heading") or "" for p in read if p.get("heading")), ""),
        passage=" ".join(p.get("text") or "" for p in read),
        before=read[0].get("before") or "" if read else "",
        after=read[-1].get("after") or "" if read else "",
        proposed=label_from_prose(meta.get("role")),
        points=analysis.get("coordinates") or [],
    )


def contexts_for(payload: Mapping[str, Any], passages: Sequence[Mapping[str, Any]] = (),
                 article_text: Optional[str] = None) -> Iterable[tuple]:
    """(table id, analysis index, SetContext) for every analysis of a payload."""
    for table_id, collection in (payload or {}).items():
        for index, analysis in enumerate((collection or {}).get("analyses", [])):
            if (analysis.get("metadata") or {}).get("source") == "prose":
                yield table_id, index, prose_context(analysis, passages)
            else:
                yield table_id, index, table_context(analysis, article_text)
