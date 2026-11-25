"""Coordinates read one or two columns off a table's headed x, y and z columns, read back.

nu-v23 sometimes reads x, y and z from the wrong cells of a row: the number printed
before x (a cluster size, a Brodmann area, a max t) becomes x, and z is lost; or the
other way, y, z and the next column. When the table heads one column each x, y and z,
side by side, the row the answer read from prints the right numbers under them, so a
point that sits at a shifted window of exactly one row's printed coordinates is read
back from that row. A point printed under the headed columns, or matching no shifted
window, or windows of rows that print different coordinates, keeps what was read.

A header can be set off its columns itself (a sub-header row one cell short, a spanned
cell resolved one column wide), and then the answer is right and the heading wrong; run
over production's stored answers (2026-11-25), headings alone would have moved correct
points onto cluster sizes and t values in about half the tables they touched. So the
values decide which window holds the coordinates. A coordinate column prints negative
numbers (left, posterior, inferior) and the columns beside it -- a size, an area, a t, a
p -- do not; a window is read back only when the headed columns include more columns
that print a negative number than the window the answer read, and print numbers on at
least as many rows. A table with no negative number in either keeps its answer. And a
point is read back only to a plausible brain point (`prose_passages.plausible`): a row
laid out apart from the rest can match a window by chance and put a cluster size in x.
"""

from __future__ import annotations

import re
from dataclasses import replace
from typing import Dict, List, Optional, Sequence, Tuple

from ingestion_workflow.models import ParseAnalysesOutput
from ingestion_workflow.services.prose_passages import plausible

#: The windows checked, nearest first: a point is read back from the first that holds it.
SHIFTS = (-1, 1, -2, 2)

_AXIS = re.compile(r"(?i)^\W*(?:mni|tal\w*|peak|coord\w*)?\W*([xyz])\W*(?:\(?\s*mm\s*\)?)?\W*$")

Triple = Tuple[float, float, float]


def _grid(markup: str) -> Tuple[List[Dict[int, str]], List[Dict[int, Optional[float]]]]:
    """The table's header rows and body rows that span every column, each as {column: text}
    or {column: number}, with spans resolved as nspond_tables reads them.

    A row a cell or more short of the table (a sub-peak row without its region, a
    sub-header without the cells a rowspan should fill) is placed from the left, so its
    columns are not the table's: it is left out."""
    from nspond_tables import serialize
    from nspond_tables.grid import as_number, is_header_row

    rows = list(serialize.from_source(markup).resolve())
    width = 1 + max((p.col for row in rows for p in row), default=-1)
    headers, body = [], []
    for row in rows:
        if len({p.col for p in row}) != width:
            continue
        if is_header_row(row):
            headers.append({p.col: p.cell.text for p in row})
        else:
            body.append({p.col: as_number(p.cell.text) for p in row})
    return headers, body


def headed_axes(headers: Sequence[Dict[int, str]]) -> Optional[Tuple[int, int, int]]:
    """The columns headed x, y and z, when the table heads exactly one of each, side by side."""
    found: Dict[str, set] = {"x": set(), "y": set(), "z": set()}
    for row in headers:
        for col, text in row.items():
            m = _AXIS.match(str(text or "").strip().lstrip("#"))
            if m:
                found[m.group(1).lower()].add(col)
    if any(len(cols) != 1 for cols in found.values()):
        return None
    x, y, z = (next(iter(found[a])) for a in "xyz")
    return (x, y, z) if (y, z) == (x + 1, x + 2) else None


def _window(row: Dict[int, Optional[float]], axes: Tuple[int, int, int], shift: int) -> Optional[Triple]:
    values = tuple(row.get(c + shift) for c in axes)
    return None if None in values else values  # type: ignore[return-value]


def _signed_columns(body: Sequence[Dict[int, Optional[float]]], axes: Tuple[int, int, int],
                    shift: int) -> int:
    """How many of the window's three columns print a negative number in some row."""
    return sum(any((row.get(c + shift) or 0) < 0 for row in body) for c in axes)


def realign(parsed: ParseAnalysesOutput, markup: str) -> Tuple[ParseAnalysesOutput, Optional[str]]:
    """The answer with each point read off the headed columns read back from its row, and
    what was read back (None when nothing was)."""
    try:
        headers, body = _grid(markup)
    except Exception:  # noqa: BLE001 - a table the grid cannot read is left as answered
        return parsed, None
    axes = headed_axes(headers)
    if axes is None:
        return parsed, None
    printed = {w for row in body for w in [_window(row, axes, 0)] if w}
    numeric = {s: sum(_window(row, axes, s) is not None for row in body) for s in (0, *SHIFTS)}
    signed = {s: _signed_columns(body, axes, s) for s in (0, *SHIFTS)}
    shifts = [s for s in SHIFTS if signed[0] > signed[s] and numeric[0] >= numeric[s]]
    if not shifts:
        return parsed, None
    total, moved = 0, {s: 0 for s in SHIFTS}
    analyses = []
    for analysis in parsed.analyses:
        points = []
        for point in analysis.points:
            total += 1
            here = tuple(point.coordinates)
            if here not in printed:
                for shift in shifts:
                    homes = {_window(row, axes, 0) for row in body if _window(row, axes, shift) == here}
                    homes.discard(None)
                    if homes:
                        if len(homes) == 1 and plausible(*next(iter(homes))):
                            point = replace(point, coordinates=list(homes.pop()))
                            moved[shift] += 1
                        break
            points.append(point)
        analyses.append(replace(analysis, points=points))
    if not any(moved.values()):
        return parsed, None
    where = "; ".join(
        f"{n} of {total} points read {abs(s)} column{'s' * (abs(s) > 1)} {'left' if s < 0 else 'right'}"
        for s, n in moved.items() if n
    )
    return replace(parsed, analyses=analyses), f"{where} of the headed x, y and z; read back from their rows"


__all__ = ["SHIFTS", "headed_axes", "realign"]
