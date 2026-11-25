"""Whether a set's numbers can be brain coordinates at all, from the numbers alone.

A table parser reports every printed triple, and some are not locations in a
human template brain: geographic sites, crystal lattices, voxel indices, positions
in centimetres from a head frame, counts and scores. Their shape gives them away
before any model reads the context, so the roles stage asks this first, and a set
it refuses gets no role (`labels.SetRole(None)`) and never enters the parse.

The brain box is `prose_passages.plausible`'s, the one check prose coordinates
already pass. On the roles evaluation set (450 production tables, hand-labelled,
2026-11-18) these rules refuse 242 of 374 non-brain sets; the 44 brain sets they
refuse are mostly misread rows (a lost sign, a range read as x, y, z) and dipoles
in a head frame, which are not template coordinates either.

A thalamic or accumbens peak can lie within 10 mm of the origin, so the near-origin
rule reads the set's source (its table, or the paper's prose): a set is refused only
when nothing there is a plausible point beyond 10 mm. Rodent stereotaxic tables are
told apart elsewhere (`paper_parse.is_rodent_atlas_table`), not here.
"""

from __future__ import annotations

from typing import Any, Iterable, List, Optional, Tuple

from ingestion_workflow.services.prose_passages import plausible

from .common import as_point

#: Bump when a rule changes: it is part of the decision's `role_source`.
GEOMETRY_VERSION = 2
SOURCE = f"geometry@{GEOMETRY_VERSION}"

#: Why a set is refused, by rule; recorded as the set's `role_reason`.
OUTSIDE = "most points lie outside a template brain"
NEAR_ORIGIN = "every value is within 10 of zero: not millimetres in a template brain"
SAME_AXES = "every point repeats one number as x, y and z"

_NEAR = 10.0


def _triples(points: Iterable[Any]) -> List[Tuple[float, float, float]]:
    out = []
    for p in map(as_point, points):
        try:
            out.append(tuple(float(p[k]) for k in ("x", "y", "z")))
        except (TypeError, ValueError, KeyError):
            continue
    return out


def _near_origin(p: Tuple[float, float, float]) -> bool:
    return max(map(abs, p)) < _NEAR


def not_brain(points: Iterable[Any], source: Iterable[Any] = ()) -> Optional[str]:
    """Why these points cannot be template brain coordinates, or None when they can be.

    `source` is every point read from the set's table, or from the paper's prose.
    A set within 10 of zero is refused only when its source holds no plausible point
    beyond that. A set with no readable point is not refused: there is nothing to
    judge, and its role is the model's.
    """
    xyz = _triples(points)
    if not xyz:
        return None
    if all(x == y == z for x, y, z in xyz):
        return SAME_AXES
    if all(map(_near_origin, xyz)) and not any(
        plausible(*p) and not _near_origin(p) for p in _triples(source)
    ):
        return NEAR_ORIGIN
    if 2 * sum(not plausible(*p) for p in xyz) > len(xyz):
        return OUTSIDE
    return None
