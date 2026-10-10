"""What a coordinate set can be for, in the study_schema's words.

A set's answer is three fields, as the paper-parse `CoordinateParse` has them:
`role` (a `CoordinateRole`), `anchor_kind` (an `AnchorKind`, for an anchor only)
and `from_prior_study`, independent of the role (ns-pond-ingestion-workflow#55):
a borrowed seed is still a seed. The values are study_schema's enums, imported,
never retyped here. Numbers that are not brain coordinates at all (channel
numbers, lattice points, a phantom's targets) have no role: `role` is None and
the set is left out of what is uploaded and of the extractors' targets.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Optional, Tuple

from study_schema.models.paper_parse import AnchorKind, CoordinateRole

#: Index order is the classifier heads' output order.
COORDINATE_ROLES: Tuple[str, ...] = tuple(r.value for r in CoordinateRole)
ANCHOR_KINDS: Tuple[str, ...] = tuple(k.value for k in AnchorKind)

#: The roles uploaded to neurostore: this study's results and the regions it
#: defined to get them. A peak quoted from another study, a bare slice position,
#: an electrode location or an `other` coordinate is kept in the parse for pondie and not
#: uploaded.
UPLOADED_ROLES: Tuple[str, ...] = (CoordinateRole.result.value, CoordinateRole.anchor.value)


def role_error(fields: Mapping[str, Any]) -> Optional[str]:
    """Why `role`, `anchor_kind` and `from_prior_study` are not study_schema's, or None."""
    role, kind = fields.get("role"), fields.get("anchor_kind")
    if role is not None and role not in COORDINATE_ROLES:
        return f"role {role!r} is not a CoordinateRole"
    if role == CoordinateRole.anchor.value and kind not in ANCHOR_KINDS:
        return f"anchor_kind {kind!r} is not an AnchorKind"
    if role != CoordinateRole.anchor.value and kind is not None:
        return f"anchor_kind {kind!r} on a {role} set"
    if not isinstance(fields.get("from_prior_study"), bool):
        return f"from_prior_study {fields.get('from_prior_study')!r} is not a boolean"
    if role is None and fields["from_prior_study"]:
        return "from_prior_study on numbers that are not coordinates"
    return None


@dataclass(frozen=True)
class SetRole:
    """One set's role fields; `role` None: the numbers are not coordinates."""

    role: Optional[str]
    anchor_kind: Optional[str] = None
    from_prior_study: bool = False

    def __post_init__(self) -> None:
        error = role_error(self.fields())
        if error:
            raise ValueError(error)

    @classmethod
    def of(cls, fields: Mapping[str, Any]) -> "SetRole":
        return cls(
            fields.get("role"),
            fields.get("anchor_kind"),
            bool(fields.get("from_prior_study")),
        )

    @property
    def coordinates(self) -> bool:
        return self.role is not None

    def fields(self) -> dict:
        return {
            "role": self.role,
            "anchor_kind": self.anchor_kind,
            "from_prior_study": self.from_prior_study,
        }


@dataclass(frozen=True)
class RoleDecision:
    """What the roles stage records for one set: the classifier's answer, never a default."""

    decided: SetRole
    #: The classifier's probability for what it decided: the role's, or that
    #: the numbers are not coordinates.
    confidence: float
    #: The classifier that decided, `name@version`.
    source: str
    #: The set's origin, `table` or `text`: which classifier read it.
    origin: str
    #: The sentences showing the coordinates come from another publication.
    prior_study_evidence: Tuple[dict, ...] = field(default=())

    @property
    def role(self) -> Optional[str]:
        return self.decided.role

    @property
    def anchor_kind(self) -> Optional[str]:
        return self.decided.anchor_kind

    @property
    def from_prior_study(self) -> bool:
        return self.decided.from_prior_study

    @property
    def uploaded(self) -> bool:
        return self.role in UPLOADED_ROLES

    def to_metadata(self) -> dict:
        """The decision in CoordinateParse's field names, plus who decided and how surely."""
        return {
            **self.decided.fields(),
            "prior_study_evidence": list(self.prior_study_evidence),
            "role_confidence": self.confidence,
            "role_source": self.source,
            "role_origin": self.origin,
        }
