"""Read the fine-tuned extractor's compact answer into the workflow's models.

v19 does not emit the same shape as the prompted models. It was trained on a
positional form, which is most of why its prompt is fifty tokens rather than
four thousand:

    {"space": "MNI" | "TAL" | null,
     "analyses": [{"name": ..., "measure": "voxels" | "mm^3" | null,
                   "points": [[x, y, z, stat_type, stat, extent], ...]}]}

Two things differ from the prompted schema and both are deliberate. The space
is stated once for the table rather than repeated on every coordinate, because
a paper normalises once. And a point is a fixed tuple with no flag fields, so
`is_subpeak` and `is_deactivation` are not read here -- they are derived from
the numbers downstream, in `coordinate_flags`.

Nothing in here raises on a malformed answer. Two of 869 benchmark tables came
back unparseable, and one bad table must cost one table, not the article.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, List, Optional, Sequence, Tuple

from ingestion_workflow.models import (
    CoordinatePoint,
    ParseAnalysesOutput,
    ParsedAnalysis,
    PointsValue,
)

logger = logging.getLogger(__name__)

__all__ = ["parse_payload", "ALLOWED_MEASURES"]

ALLOWED_MEASURES = {"voxels", "mm^3"}
_STAT_KINDS = {"T", "Z", "F", "P", "R", "B"}
_JSON = re.compile(r"\{.*\}", re.S)


def _number(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _extent(value: Any) -> Optional[int]:
    number = _number(value)
    return None if number is None else int(abs(number))


def _measure(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"mm3", "mm^3"}:
        return "mm^3"
    return "voxels" if text == "voxels" else None


def _space(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip().upper()
    if text == "MNI":
        return "MNI"
    return "TAL" if text in {"TAL", "TALAIRACH"} else None


def _point(row: Sequence[Any], space: Optional[str], measure: Optional[str]):
    """One [x, y, z, stat_type, stat, extent] tuple, or None if it is not one.

    Short rows are kept when they carry a coordinate: the tail is optional and
    a table with no statistic column still yields usable points. A row whose
    first three entries are not numbers is not a coordinate at all.
    """
    if not isinstance(row, (list, tuple)) or len(row) < 3:
        return None
    xyz = [_number(v) for v in row[:3]]
    if any(v is None for v in xyz):
        return None

    values: List[PointsValue] = []
    kind = str(row[3]).strip().upper() if len(row) > 3 and row[3] is not None else None
    statistic = _number(row[4]) if len(row) > 4 else None
    if kind in _STAT_KINDS and statistic is not None:
        values.append(PointsValue(value=statistic, kind=kind))
    elif statistic is not None:
        # A statistic whose type the model did not name is still a statistic,
        # and its sign is what decides is_deactivation.
        values.append(PointsValue(value=statistic, kind=None))

    return CoordinatePoint(
        coordinates=xyz,
        space=space,
        values=values or None,
        cluster_size=_extent(row[5]) if len(row) > 5 else None,
        cluster_measure=measure,
    )


def parse_payload(text: str) -> Tuple[ParseAnalysesOutput, Optional[str]]:
    """Return the analyses and the table-level coordinate space.

    The space comes back separately because it belongs to the table, and the
    caller already has a default to fall back on when the model says nothing.
    An empty result is a real answer here, not a failure: v19 is trained to
    return no analyses for a table that holds no coordinates.
    """
    if not text or not text.strip():
        return ParseAnalysesOutput(analyses=[]), None

    match = _JSON.search(text)
    if not match:
        logger.warning("extractor returned no JSON object (%d chars)", len(text))
        return ParseAnalysesOutput(analyses=[]), None
    try:
        payload = json.loads(match.group(0))
    except ValueError as exc:
        logger.warning("extractor returned unparseable JSON: %s", exc)
        return ParseAnalysesOutput(analyses=[]), None
    if not isinstance(payload, dict):
        return ParseAnalysesOutput(analyses=[]), None

    space = _space(payload.get("space"))
    analyses: List[ParsedAnalysis] = []
    for entry in payload.get("analyses") or []:
        if not isinstance(entry, dict):
            continue
        measure = _measure(entry.get("measure"))
        points = [
            point
            for point in (_point(row, space, measure) for row in entry.get("points") or [])
            if point is not None
        ]
        name = entry.get("name")
        named = bool(name is not None and str(name).strip())
        if not points and not named:
            # Neither a name nor a coordinate: nothing was reported, so there
            # is no analysis to record.
            continue
        # A NAMED analysis with no points is kept. The table names a contrast
        # and reports `n.s.`, so the paper ran it and found nothing -- which
        # is a result. Dropping it would say the paper never looked. This is
        # the opposite case to an empty payload, where the table reports no
        # contrasts at all and the whole answer is `{"analyses": []}`.
        analyses.append(
            ParsedAnalysis(name=str(name) if name is not None else None, points=points)
        )
    return ParseAnalysesOutput(analyses=analyses), space
