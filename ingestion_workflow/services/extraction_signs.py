"""Signs that the extractor invented or misplaced a table's points.

Each sign is checked per point against the table's own rows. A table is suspect on a sign
when at least `MIN_POINTS` of its points, and at least a third of them, carry it. Measured
on nu-v23's first 45,384 tables (2026-11-23): 15 of 16 suspect tables read by hand were
inventions, such as factor loadings, Cohen's d, time windows and (0, 0, 0) placeholders read
as coordinates. No table judged correct in a 50-table blind comparison was suspect.
"""

from __future__ import annotations

import re
from collections import Counter
from typing import Iterable, List, Optional, Sequence, Tuple

#: The signs, in the order they are reported.
UNPRINTED = "unprinted"
SCATTERED = "scattered"
STATISTIC_LIKE = "statistic_like"
REPEATED = "repeated"
STATISTIC_ELSEWHERE = "statistic_elsewhere"
SIGNS = (UNPRINTED, SCATTERED, STATISTIC_LIKE, REPEATED, STATISTIC_ELSEWHERE)

DESCRIPTIONS = {
    UNPRINTED: "have an x, y or z the table does not print",
    SCATTERED: "have an x, y and z no single row prints together",
    STATISTIC_LIKE: "look like statistics, not coordinates",
    REPEATED: "repeat one number as x, y and z",
    STATISTIC_ELSEWHERE: "carry a statistic printed only in another point's row",
}

MIN_POINTS = 3
_NUMBER = re.compile(r"\d+(?:\.\d+)?(?:[eE]-?\d+)?")

Point = Tuple[float, float, float, Optional[float]]


def _key(value: float) -> str:
    # Signs are ignored: "±30" and "-2/8" print one number for two points.
    return f"{abs(float(value)):.6g}"


def _small(value: float) -> bool:
    return value != int(value) and abs(value) < 10


def _long(value: float) -> bool:
    text = repr(abs(float(value)))
    return "e" in text or ("." in text and len(text.split(".")[1]) >= 3)


def point_signs(points: Sequence[Point], rows: Iterable[str]) -> Counter:
    """How many of `points` carry each sign, against the table's `rows` of text.

    Brain coordinates are mostly whole millimetres, so a point is statistic-like when all
    three of its numbers are non-integers under 10, or two of them have three or more
    decimals.
    """
    counts = [Counter(_key(n) for n in _NUMBER.findall(row)) for row in rows]
    anywhere = Counter(k for row in counts for k in row.elements())

    def homes(point) -> List[int]:
        want = Counter(_key(v) for v in point[:3])
        return [i for i, row in enumerate(counts) if all(row[k] >= n for k, n in want.items())]

    held = {i for p in points for i in homes(p)}
    signs: Counter = Counter()
    for point in points:
        x, y, z, statistic = point
        want = Counter(_key(v) for v in (x, y, z))
        found = homes(point)
        if any(not anywhere[k] for k in want):
            signs[UNPRINTED] += 1
        elif not found:
            signs[SCATTERED] += 1
        if all(_small(v) for v in (x, y, z)) or sum(_long(v) for v in (x, y, z)) >= 2:
            signs[STATISTIC_LIKE] += 1
        if len(want) == 1:
            signs[REPEATED] += 1
        if statistic is not None and found:
            s = _key(statistic)
            # Printed in a row with no point (under its point, or a spanned row) is fine.
            others = [i for i, row in enumerate(counts) if row[s] and i not in found]
            if not any(counts[i][s] for i in found) and others and all(i in held for i in others):
                signs[STATISTIC_ELSEWHERE] += 1
    return signs


def suspect_signs(points: Sequence[Point], rows: Iterable[str]) -> List[str]:
    """The signs on which a table of `points` is suspect, in `SIGNS` order."""
    signs = point_signs(points, list(rows))
    n = len(points)
    return [s for s in SIGNS if signs[s] >= MIN_POINTS and signs[s] >= n / 3]


def describe(points: Sequence[Point], rows: Iterable[str]) -> Optional[str]:
    """One sentence naming a suspect table's signs, or None when it is not suspect."""
    rows = list(rows)
    signs = point_signs(points, rows)
    found = suspect_signs(points, rows)
    if not found:
        return None
    parts = [f"{signs[s]} of {len(points)} points {DESCRIPTIONS[s]}" for s in found]
    return "suspect extraction: " + "; ".join(parts)


__all__ = ["SIGNS", "MIN_POINTS", "point_signs", "suspect_signs", "describe"]
