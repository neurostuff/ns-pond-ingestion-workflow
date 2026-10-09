"""What a coordinate set can be for, in the study_schema's words.

The classifier predicts one label per set: a `CoordinateRole`, with the
`AnchorKind` folded in for anchors, since an ROI and a seed are told apart by
the same evidence that tells either from a result. Whether the coordinates come
from another publication is a second, independent answer
(ns-pond-ingestion-workflow#55): a borrowed seed is still a seed.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Tuple

#: Index order is the classifier head's output order; append, never reorder.
ROLE_LABELS: Tuple[str, ...] = (
    "result",
    "anchor:roi",
    "anchor:seed",
    "anchor:stimulation_target",
    "anchor:node",
    "localization",
    "reference",
    "display",
    "other",
)

#: The roles uploaded to neurostore: this study's results and the regions it
#: defined to get them. A peak quoted from another study, a display position
#: or an electrode location is kept in the parse for pondie and not uploaded.
UPLOADED_ROLES = frozenset({"result", "anchor"})

#: The prose model's vocabulary (prompts.prose_coordinates.ROLES) as labels.
_FROM_PROSE = {
    "result": "result",
    "roi": "anchor:roi",
    "seed": "anchor:seed",
    "target": "anchor:stimulation_target",
    "prior_study": "reference",
    "figure": "display",
    "other": "other",
}


def label_from_prose(role: Optional[str]) -> str:
    """The label a prose model's role proposes; `result` when it gave none."""
    return _FROM_PROSE.get(role or "result", "other")


def split_label(label: str) -> Tuple[str, Optional[str]]:
    """(`CoordinateRole`, `AnchorKind` or None) for a label."""
    role, _, kind = label.partition(":")
    return role, kind or None


def join_label(role: str, anchor_kind: Optional[str] = None) -> str:
    """The label for a role and anchor kind; an anchor of unknown kind is an ROI."""
    if role == "anchor":
        label = f"anchor:{anchor_kind or 'roi'}"
        return label if label in ROLE_LABELS else "anchor:roi"
    return role if role in ROLE_LABELS else "other"


@dataclass(frozen=True)
class RoleDecision:
    """What the roles stage records for one set."""

    label: str
    from_prior_study: bool
    #: The classifier's probability for `label`; None when the proposal stood
    #: because no classifier ran.
    confidence: Optional[float]
    #: `proposal`, or the classifier's name and version.
    source: str
    #: The label proposed before the classifier ran, kept so a reviewer can see
    #: what it overrode.
    proposed: str

    @property
    def role(self) -> str:
        return split_label(self.label)[0]

    @property
    def anchor_kind(self) -> Optional[str]:
        return split_label(self.label)[1]

    @property
    def uploaded(self) -> bool:
        return self.role in UPLOADED_ROLES

    def to_metadata(self) -> dict:
        return {
            "role": self.role,
            "anchor_kind": self.anchor_kind,
            "from_prior_study": self.from_prior_study,
            "confidence": self.confidence,
            "source": self.source,
            "proposed": self.proposed,
        }
