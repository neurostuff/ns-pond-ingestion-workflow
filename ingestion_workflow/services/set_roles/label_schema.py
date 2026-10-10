"""The answer a labeller gives for each coordinate set, as a strict JSON schema.

Table and prose sets are labelled from different prompts (`labeling`), but the
answer is the same: whether the numbers are brain coordinates at all, study_schema's
role and anchor kind, whether the coordinates come from another publication, and
which of the numbered context sentences show it. One labelling unit (a table, or a
passage) holds several sets, answered together, so each answer names its set by number.
"""

from __future__ import annotations

from typing import Any, Dict, Mapping, Optional

from .labels import ANCHOR_KINDS, COORDINATE_ROLES, SetRole, role_error

#: Bump when the schema or the instructions change; recorded on every label.
LABEL_VERSION = 2

SET_LABEL: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["set", "coordinates", "role", "anchor_kind", "from_prior_study", "evidence"],
    "properties": {
        "set": {"type": "integer", "description": "The set's number, as given"},
        "coordinates": {
            "type": "boolean",
            "description": "The numbers are locations in a brain; false for anything else",
        },
        "role": {
            "type": ["string", "null"],
            "enum": [*COORDINATE_ROLES, None],
            "description": "Null only when coordinates is false",
        },
        "anchor_kind": {
            "type": ["string", "null"],
            "enum": [*ANCHOR_KINDS, None],
            "description": "For an anchor only; null otherwise",
        },
        "from_prior_study": {
            "type": "boolean",
            "description": "The coordinates were taken from another publication",
        },
        "evidence": {
            "type": "array",
            "items": {"type": "integer"},
            "description": "Numbers of the sentences that show the role or the prior study; "
            "empty when none does",
        },
    },
}

SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["sets"],
    "properties": {"sets": {"type": "array", "items": SET_LABEL}},
}


def set_role(answer: Mapping[str, Any]) -> SetRole:
    """One answer's role fields; a reference is always from a prior study."""
    if not answer.get("coordinates"):
        return SetRole(None)
    role = answer["role"]
    return SetRole(
        role,
        answer.get("anchor_kind") if role == "anchor" else None,
        bool(answer["from_prior_study"]) or role == "reference",
    )


def check(answer: Mapping[str, Any], n_sets: int, n_sentences: int) -> Optional[str]:
    """Why a unit's answer is unusable, or None: each set answered once, each evidence id real."""
    seen = sorted(a.get("set") for a in answer.get("sets") or [])
    if seen != list(range(n_sets)):
        return f"answered sets {seen}, expected 0..{n_sets - 1}"
    for a in answer["sets"]:
        if a.get("coordinates") and a.get("role") is None:
            return f"set {a.get('set')}: coordinates without a role"
        if a.get("coordinates"):
            error = role_error({**a, "from_prior_study": bool(a.get("from_prior_study"))})
            if error:
                return f"set {a.get('set')}: {error}"
        if any(not 0 <= e < n_sentences for e in a.get("evidence") or []):
            return f"set {a.get('set')}: evidence outside 0..{n_sentences - 1}"
    return None
