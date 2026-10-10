"""The answer a labeller gives for each coordinate set, as a strict JSON schema.

Table and prose sets are labelled from different prompts (`labeling`), but the
answer is the same: study_schema's role, the anchor kind, whether the
coordinates come from another publication, and which of the numbered context
sentences show it. One labelling unit (a table, or a passage) holds several
sets, answered together, so each answer names its set by number.
"""

from __future__ import annotations

from typing import Any, Dict, Mapping, Optional

from .labels import ANCHOR_KINDS, COORDINATE_ROLES, join_label

#: Bump when the schema or the instructions change; recorded on every label.
LABEL_VERSION = 1

SET_LABEL: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["set", "role", "anchor_kind", "from_prior_study", "evidence"],
    "properties": {
        "set": {"type": "integer", "description": "The set's number, as given"},
        "role": {"type": "string", "enum": list(COORDINATE_ROLES)},
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


def label_of(answer: Mapping[str, Any]) -> str:
    """The classifier label for one answer (`anchor` + `seed` -> `anchor:seed`)."""
    return join_label(answer["role"], answer.get("anchor_kind"))


def check(answer: Mapping[str, Any], n_sets: int, n_sentences: int) -> Optional[str]:
    """Why a unit's answer is unusable, or None: each set answered once, each evidence id real."""
    seen = sorted(a.get("set") for a in answer.get("sets") or [])
    if seen != list(range(n_sets)):
        return f"answered sets {seen}, expected 0..{n_sets - 1}"
    for a in answer["sets"]:
        if a.get("role") not in COORDINATE_ROLES:
            return f"set {a.get('set')}: unknown role {a.get('role')!r}"
        if any(not 0 <= e < n_sentences for e in a.get("evidence") or []):
            return f"set {a.get('set')}: evidence outside 0..{n_sentences - 1}"
    return None
