"""Derive the coordinate flags from what the table printed.

`is_subpeak` and `is_deactivation` were read off by the model, which had to be
told what to look for in thirty lines of prompt and could disagree with itself
between two rows of one table. Both are decidable from the numbers already
extracted, so they are decided here instead: same answer every time, and no
tokens spent asking.

The fine-tuned extractor settles it either way -- its points are bare
``[x, y, z, statistic_type, statistic, extent]`` tuples with no flag fields at
all, so for that path these are the only place the flags can come from.

``is_seed`` stays with the model. A seed region is named, not computed: nothing
in the numbers distinguishes a seed from a peak.
"""

from __future__ import annotations

from typing import Iterable, List, Optional, Sequence

__all__ = ["is_deactivation", "subpeak_flags", "reports_extent"]


def is_deactivation(statistic_value: Optional[float]) -> bool:
    """True when the statistic is explicitly negative.

    Only an explicit negative counts. Plenty of tables print magnitudes and
    put the direction in the contrast name ("A > B", "decreases"); reading
    that is the model's job, and inferring it from an unsigned number here
    would invent a result. So this is deliberately conservative: it marks
    what the table states and nothing else.
    """
    if statistic_value is None:
        return False
    try:
        return float(statistic_value) < 0
    except (TypeError, ValueError):
        return False


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
