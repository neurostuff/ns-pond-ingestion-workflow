"""Derive a coordinate's sign and subpeak flag from what the table printed.

`is_subpeak` and the direction were read off by the model, which had to be
told what to look for in thirty lines of prompt and could disagree with itself
between two rows of one table. Both are decidable from the numbers already
extracted, so they are decided here instead: same answer every time, and no
tokens spent asking.

The fine-tuned extractor settles it either way -- its points are bare
``[x, y, z, statistic_type, statistic, extent]`` tuples with no flag fields at
all, so for that path these are the only place the flags can come from.

There is no deactivation flag and no seed flag. A negative point belongs to
the inverse contrast, which the sign split makes its own analysis; a seed is
what a whole set of points is for, its role, not a property of one row.
"""

from __future__ import annotations

from typing import Iterable, List, Optional, Sequence

__all__ = ["NON_DIRECTIONAL_KINDS", "point_sign", "subpeak_flags", "reports_extent"]

#: Kinds whose value has no direction: a p value and an F are positive
#: whichever way the contrast runs. study_schema's `StatisticKind` says the
#: same of chi-square, which this workflow does not report.
NON_DIRECTIONAL_KINDS = frozenset({"P", "F"})


def point_sign(statistic_value, statistic_type: Optional[str] = None) -> str:
    """`positive`, `negative` or `unsigned`: study_schema's `PointSign`.

    Read from the statistic, whatever a model said. `negative` only when it is
    explicitly below zero: plenty of tables print magnitudes and put the
    direction in the contrast name, and inferring a direction from an unsigned
    number would invent a result. `unsigned` when there is no directional
    statistic to read -- none printed, a p value or an F only, or a value that
    is not a number. In a split analysis those points join the positive half
    and keep the tag, so the placement stays visible.
    """
    if statistic_value is None:
        return "unsigned"
    if statistic_type is not None and str(statistic_type).upper() in NON_DIRECTIONAL_KINDS:
        return "unsigned"
    try:
        value = float(statistic_value)
    except (TypeError, ValueError):
        return "unsigned"
    if value != value:                      # NaN
        return "unsigned"
    return "negative" if value < 0 else "positive"


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
