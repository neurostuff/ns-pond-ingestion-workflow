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

from typing import Iterable, List, Optional, Sequence

__all__ = [
    "PLACEHOLDER_NAME",
    "is_placeholder",
    "subpeak_flags",
    "reports_extent",
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
