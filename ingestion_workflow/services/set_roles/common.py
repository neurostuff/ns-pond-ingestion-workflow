"""What the table and prose context builders share: point shape, cues, sentences.

Only these helpers are shared. A table set and a prose set are read from
different evidence (a caption, header and rows; the sentences around the
coordinates), so each has its own builder and its own version:
`table_context` and `prose_context`.
"""

from __future__ import annotations

import math
import re
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

#: Characters, not tokens, so the builder needs no tokenizer. About 512
#: tokens of English.
MAX_CHARS = 2400

_CITATION = re.compile(
    r"\(\s*[A-Z][A-Za-z'\-]+(?:\s+(?:et\s+al\.?|and|&)\s*[A-Za-z'\-]*)?,?\s+(?:19|20)\d{2}[a-z]?"
    r"|\[\s*\d+(?:\s*[,–\-]\s*\d+)*\s*\]"
    # "(41)", "(12, 14)": numbered citations in parentheses, at most two, so a
    # coordinate triple "(4, 30, 22)" is not one.
    r"|\(\s*\d{1,3}(?:\s*[–\-]\s*\d{1,3})?(?:\s*,\s*\d{1,3}(?:\s*[–\-]\s*\d{1,3})?)?\s*\)"
    r"|\b[A-Z][A-Za-z'\-]+\s+et\s+al\.?\s*\(?(?:19|20)\d{2}"
    r"|\b[A-Z][A-Za-z'\-]+(?:\s+(?:and|&)\s+[A-Z][A-Za-z'\-]+)?\s*\((?:19|20)\d{2}[a-z]?\)"
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
#: Slice, crosshair and view words: a feature of the text. They imply no role of their own.
_SLICE_CUES = re.compile(
    r"\b(slice[s]?|crosshair[s]?|displayed|shown\s+at|overlaid|axial|coronal|sagittal)\b", re.I
)
#: A sentence break, but not after "et al.", "e.g.", "i.e.", "Fig." or "vs.".
_SENTENCE = re.compile(
    r"(?<!\bal\.)(?<!\be\.g\.)(?<!\bi\.e\.)(?<!\bFig\.)(?<!\bfig\.)(?<!\bvs\.)"
    r"(?<=[.!?])\s+(?=[A-Z(\[])"
)


#: A caption's leading label, which the sentence pattern would split off.
_LABEL_ONLY = re.compile(r"^(?:supplementa\w*\s+)?(?:table|fig\.?|figure)\s*s?\d+[a-z]?\.$", re.I)


def clip(text: Optional[str], limit: int) -> str:
    text = re.sub(r"\s+", " ", text or "").strip()
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "…"


def point_summary(points: Sequence[Mapping[str, Any]]) -> str:
    """The shape of a set's points as tokens: what an ROI list and a peak list differ in.

    Peaks carry statistics and cluster sizes; ROI centres and seeds usually do
    not, come in fewer, and are often printed as left/right pairs. Point flags
    that restate a role (`is_seed`) are not read: the role is what is predicted.
    """
    points = [
        p
        for p in map(as_point, points)
        if all(isinstance(p.get(k), (int, float)) for k in ("x", "y", "z"))
    ]
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
        1
        for i, (x, y, z) in enumerate(xyz)
        if any(
            j != i and abs(x + u) <= 2 and abs(y - v) <= 4 and abs(z - w) <= 4 and abs(x) > 2
            for j, (u, v, w) in enumerate(xyz)
        )
    )
    centre = [sum(c[i] for c in xyz) / n for i in range(3)]
    spread = sum(math.dist(c, centre) for c in xyz) / n
    integral = sum(1 for c in xyz if all(float(v).is_integer() for v in c))
    return " ".join(
        [
            f"n={n}",
            "stats=" + ",".join(f"{k}:{v}" for k, v in sorted(kinds.items())),
            f"valued={len(with_value)}/{n}",
            f"negative={negative}",
            f"clusters={sum(1 for p in points if p.get('cluster_size') is not None)}",
            f"subpeaks={sum(1 for p in points if p.get('is_subpeak'))}",
            f"mirrored={mirrored}",
            f"integral={integral}",
            f"spread={round(spread)}mm",
        ]
    )


def cue_summary(text: str) -> str:
    return " ".join(
        [
            f"citations={len(_CITATION.findall(text))}",
            f"prior_words={len(_PRIOR_CUES.findall(text))}",
            f"anchor_words={len(_ANCHOR_CUES.findall(text))}",
            f"figure_words={len(_SLICE_CUES.findall(text))}",
        ]
    )


def sentences(text: Optional[str]) -> List[str]:
    """A text's sentences, whitespace collapsed."""
    text = re.sub(r"\s+", " ", text or "").strip()
    out: List[str] = []
    for piece in _SENTENCE.split(text) if text else []:
        if out and _LABEL_ONLY.match(out[-1]):
            out[-1] = f"{out[-1]} {piece}"  # "Table 2." is a caption's label, not a sentence
        elif piece:
            out.append(piece)
    return out


def citations(text: Optional[str]) -> List[str]:
    """The citation markers in a text, in order, each once."""
    out: List[str] = []
    for match in _CITATION.finditer(text or ""):
        if match.group(0) not in out:
            out.append(match.group(0))
    return out


def citing(texts: Iterable[str], limit: int = 2) -> List[str]:
    """Sentences of `texts` that cite another publication, in the order given."""
    out: List[str] = []
    for text in texts:
        for sentence in sentences(text):
            if _CITATION.search(sentence) and sentence not in out:
                out.append(sentence)
                if len(out) >= limit:
                    return out
    return out


def as_point(point: Any) -> Dict[str, Any]:
    """A point in the analyses payload's shape, from any of the shapes the training data uses.

    nu-v21 targets write `[x, y, z, statistic, value, cluster]`; the prose
    datasets `{"xyz": [...], "stat": [kind, value], "cluster": [n, unit]}`.
    """
    if isinstance(point, (list, tuple)):
        x, y, z, *rest = list(point) + [None] * 3
        return {
            "x": x,
            "y": y,
            "z": z,
            "statistic_type": rest[0],
            "statistic_value": rest[1],
            "cluster_size": rest[2],
        }
    if "xyz" in point:
        stat = point.get("stat") or [None, None]
        cluster = point.get("cluster") or [None, None]
        x, y, z = point["xyz"]
        return {
            "x": x,
            "y": y,
            "z": z,
            "statistic_type": stat[0],
            "statistic_value": stat[1],
            "cluster_size": cluster[0],
            "is_subpeak": point.get("is_subpeak", False),
        }
    return dict(point)


def citing_sentences(text: Optional[str], label: Optional[str], limit: int = 3) -> List[str]:
    """Sentences of the article that cite a table by its label ("Table 2", "Table S1")."""
    if not text or not label:
        return []
    match = re.search(r"(?:supplementa\w*\s+)?table\s*(s?\d+[a-z]?)", label, re.I)
    if not match:
        return []
    pattern = re.compile(rf"\btables?\s*{re.escape(match.group(1))}\b", re.I)
    out = []
    for sentence in _SENTENCE.split(re.sub(r"\s+", " ", text)):
        if pattern.search(sentence):
            out.append(sentence.strip())
            if len(out) >= limit:
                break
    return out
