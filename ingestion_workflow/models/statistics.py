"""Shared helpers for neuroimaging statistic kinds."""

from __future__ import annotations

from typing import Any, Optional

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
