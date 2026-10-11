"""Derive a coordinate's subpeak flag from what the table printed.

`is_subpeak` and the direction were read off by the model, which had to be
told what to look for in thirty lines of prompt and could disagree with itself
between two rows of one table. Both are decidable from the numbers already
extracted, so they are decided here instead: same answer every time, and no
tokens spent asking.

The fine-tuned extractor settles it either way -- its points are bare
``[x, y, z, statistic_type, statistic, extent]`` tuples with no flag fields at
all, so for that path these are the only place the flags can come from.

A point's direction is study_schema's rule, read through
`models.statistics.side`. There is no deactivation flag and no seed flag. A
negative point belongs to the inverse contrast, which the sign split makes its
own analysis; a seed is what a whole set of points is for, its role, not a
property of one row.
"""

from __future__ import annotations

import re
from typing import Iterable, List, Optional, Sequence

__all__ = [
    "PLACEHOLDER_NAME",
    "is_placeholder",
    "subpeak_flags",
    "reports_extent",
    "SPLIT_RULE",
    "reversed_contrast",
    "inverse_name",
    "declared_split",
]

#: What the prompted rules told the model to answer for a table with no
#: coordinates, and the name it falls back on for points it cannot label.
PLACEHOLDER_NAME = "UNKNOWN"


def is_placeholder(name: Optional[str], points: Sequence) -> bool:
    """An `UNKNOWN` analysis with no points: a table reading, not an analysis.

    The prompt used to ask for one on every table with no coordinates, and it
    looks exactly like a named contrast reported `n.s.` -- zero points -- so
    pondie read each one as a null result. A zero-point analysis must mean the
    table named a contrast; a table with none is `no_coordinates` in the
    analyses stage's readings. An `UNKNOWN` analysis *with* points is kept:
    those are real coordinates whose label the model could not read.

    Checked where analyses are made and again where they leave for pondie and
    neurostore, because analyses stored before the check still hold them.
    """
    return (name or "").strip().upper() == PLACEHOLDER_NAME and not points

def reports_extent(cluster_sizes: Iterable[Optional[int]]) -> bool:
    """Does this analysis print a cluster extent at all?"""
    return any(size is not None for size in cluster_sizes)


def subpeak_flags(cluster_sizes: Sequence[Optional[int]]) -> List[bool]:
    """Which rows are local maxima inside another row's cluster.

    A table that reports extent prints it once per cluster, on the peak, and
    leaves it blank on the local maxima that follow. So within an analysis
    that reports extent *somewhere*, a row with no extent is a subpeak.

    An analysis that never reports extent says nothing about subpeaks, and
    none are marked -- every row is treated as a peak, which is what a table
    of peaks with no sizes actually is. This is why the decision is taken over
    the whole analysis and not row by row: one row's blank extent means
    nothing until you know whether its neighbours have one.
    """
    if not reports_extent(cluster_sizes):
        return [False] * len(cluster_sizes)
    return [size is None for size in cluster_sizes]


#: study_schema's SplitRule for the analyses stage's split.
SPLIT_RULE = "sign_of_directional_statistic"

#: The contrast forms whose reverse is the same words with the sides swapped, as
#: they are printed in the inverse halves of the ns-pond corpus: "vs"/"vs."/"versus",
#: ">", "<", "minus", and a dash with a space on both sides. An unspaced hyphen is
#: not one: it joins words ("EQ-I", "OBJ-SCD") at least as often as it subtracts.
_CONTRAST_OPERATOR = re.compile(
    r"\s+(?:vs\.?|versus|minus)\s+|\s*[<>]\s*|\s+[-–−]\s+", re.IGNORECASE
)
#: A label before the contrast, "(1) " or "B) " or "Encoding: ", which stays in front.
_LEADING_LABEL = re.compile(r"^(?:\(?[0-9A-Za-z]{1,2}[).]\s+|[^:<>]+:\s+)")
#: A qualifier after it, "(cluster size > 36)", which stays behind.
_TRAILING_QUALIFIER = re.compile(r"\s*[(\[][^()\[\]]*(?:\([^()]*\)[^()\[\]]*)*[)\]]$")
_QUOTES = "\"'‘’“”"


def _balanced(text: str) -> bool:
    return all(text.count(o) == text.count(c) for o, c in ("()", "[]", "{}"))


def reversed_contrast(name: str) -> Optional[str]:
    """`name` with its two sides swapped, or None when it is not one two-sided contrast.

    "A > B" is "B > A", "A vs. B" is "B vs. A", "A minus B" is "B minus A" and
    "A - B" is "B - A". A name with no such operator, or with more than one
    ("Go vs. Nogo - OC vs. YC"), has no reverse that can be read off it.
    """
    core = name.strip()
    opening = closing = ""
    if len(core) > 1 and core[0] in _QUOTES and core[-1] in _QUOTES:
        opening, closing, core = core[0], core[-1], core[1:-1].strip()
    label = _LEADING_LABEL.match(core)
    head = label.group(0) if label and _CONTRAST_OPERATOR.search(core[label.end():]) else ""
    core = core[len(head):]
    qualifier = _TRAILING_QUALIFIER.search(core)
    tail = qualifier.group(0) if qualifier and qualifier.start() > 0 else ""
    core = core[: len(core) - len(tail)]
    operators = list(_CONTRAST_OPERATOR.finditer(core))
    if len(operators) != 1:
        return None
    left, right = core[: operators[0].start()].strip(), core[operators[0].end():].strip()
    if not (left and right and _balanced(left) and _balanced(right)):
        return None
    swapped = f"{head}{right}{operators[0].group(0)}{left}{tail}"
    return f"{opening}{swapped}{closing}"


def inverse_name(name: str) -> str:
    """The name of the inverse half of the split analysis `name`.

    The reversed contrast when it can be read off the name; otherwise the name
    with " (inverse)". The half is declared by `split`, never by this name, and
    nothing reads the name back.
    """
    return reversed_contrast(name) or f"{name} (inverse)"


def declared_split(metadata: Optional[dict], original_analysis: Optional[str] = None) -> Optional[dict]:
    """The analyses stage's `metadata["split"]` as study_schema's SignSplit, or None.

    The stage's `index`/`original_index` are positions in its own collection and
    mean nothing outside it, so they are not carried; `original_analysis` is the
    original's key where the output has one.
    """
    split = (metadata or {}).get("split")
    if not split or split.get("half") not in ("original", "inverse"):
        return None
    out = {"half": split["half"], "rule": SPLIT_RULE}
    if split["half"] == "inverse" and original_analysis:
        out["original_analysis"] = original_analysis
    return out
