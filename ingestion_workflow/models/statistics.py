"""Shared helpers for neuroimaging statistic kinds."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from study_schema.statistics import point_side

#: The kinds the extractor may report, in the order a table is read when it
#: offers more than one -- `nspond_tables.fields.STATISTIC_PRIORITY`, which is
#: where the rule lives. This tuple is the single source: the extractor's
#: template offers exactly these, `nuextract_payload` accepts exactly these,
#: and `PointsValue` stores exactly these.
#:
#: They disagreed once. The template offered six, the reader accepted eight,
#: and the store accepted six again -- so the model, trained to report Cohen's
#: d, was forbidden it by its prompt, and on the one path where it said D
#: anyway the store raised and took the table with it.
STATISTIC_KINDS = ("T", "Z", "D", "G", "F", "R", "B", "P")

#: D is Cohen's d and G is Hedges' g; g is almost always an SDM or ALE
#: meta-analysis. OTHER is for the prompted path, which reports free text.
ALLOWED_STATISTIC_KINDS = frozenset({*STATISTIC_KINDS, "OTHER"})

#: The letters, one to one onto study_schema's StatisticKind (its description
#: lists them; study_schema has no such map to import). Anything else, and no
#: letter at all, is `other`.
SCHEMA_KINDS = {
    "T": "t",
    "Z": "z",
    "F": "f",
    "D": "d",
    "G": "g",
    "R": "r",
    "B": "beta",
    "P": "p",
}


def schema_kind(letter: Optional[str]) -> str:
    """study_schema's StatisticKind for one of these letters."""
    return SCHEMA_KINDS.get(str(letter or "").strip().upper(), "other")


def point_values(statistic_value: Any, statistic_type: Optional[str]) -> List[Dict[str, Any]]:
    """A point's statistic as study_schema PointValue dicts: none unless it is a number."""
    if isinstance(statistic_value, bool) or not isinstance(statistic_value, (int, float)):
        return []
    if statistic_value != statistic_value:  # NaN
        return []
    return [{"kind": schema_kind(statistic_type), "value": float(statistic_value)}]


def side(statistic_value: Any, statistic_type: Optional[str] = None) -> Optional[str]:
    """`positive`, `negative`, or None when unsigned.

    study_schema.statistics decides: a value below zero is negative
    whatever its kind (a negative p or F means a signed or mislabelled column);
    otherwise a p, F or chi-square is unsigned, and any other kind, including
    none, is positive. Zero is positive; no value is unsigned.
    """
    return point_side(point_values(statistic_value, statistic_type))


def normalize_statistic_kind(kind: Any) -> Optional[str]:
    """Return one of the allowed statistic kinds for the provided hint."""
    if kind is None:
        return None
    normalized = str(kind).strip()
    if not normalized:
        return None
    upper_value = normalized.upper()
    if upper_value in ALLOWED_STATISTIC_KINDS:
        return upper_value
    lower = normalized.lower()
    # Named before the single letters, which match inside these words.
    if "cohen" in lower:
        return "D"
    if "hedge" in lower:
        return "G"
    if "z" in lower:
        return "Z"
    if "t" in lower or "stat" in lower:
        return "T"
    if "f" in lower:
        return "F"
    if "r" in lower or "correlation" in lower:
        return "R"
    if lower.startswith("p"):
        return "P"
    if "beta" in lower:
        return "B"
    return "OTHER"
