"""What a coordinate set can be for, in the study_schema's words.

A set's answer is three fields, as the paper-parse `CoordinateParse` has them:
`role` (a `CoordinateRole`), `anchor_kind` (an `AnchorKind`, for an anchor only)
and `from_prior_study`, independent of the role (ns-pond-ingestion-workflow#55):
a borrowed seed is still a seed. The values are study_schema's enums, imported,
never retyped here. Numbers that are not brain coordinates at all (channel
numbers, lattice points, a phantom's targets) have no role: `role` is None and
the set is left out of the parse, so it is not uploaded, and of the extractors' targets.

Two more answers stay in ns-pond until study_schema has fields for them: `space`,
whether the coordinates are human MNI/Talairach millimetres (`template`) or a
location in another frame (`other`: an animal brain, a head or sensor frame,
voxel indices), and `coordinate_source`, where the coordinates come from
(`this_study`, `prior_study`, `atlas_or_database`). `from_prior_study` is
`coordinate_source == prior_study` (always true for a reference).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Optional, Tuple

from study_schema.models.paper_parse import AnchorKind, CoordinateRole

#: Index order is the classifier heads' output order.
COORDINATE_ROLES: Tuple[str, ...] = tuple(r.value for r in CoordinateRole)
ANCHOR_KINDS: Tuple[str, ...] = tuple(k.value for k in AnchorKind)

#: This study's own roles: its results and the regions it defined to get them. A peak
#: quoted from another study, a bare slice position, an electrode location or an `other`
#: coordinate is `held`. It is still parsed and uploaded with its role, and Neurostore's
#: releases export results only (docs/set-roles-artifact.md).
OWN_ROLES: Tuple[str, ...] = (CoordinateRole.result.value, CoordinateRole.anchor.value)

#: Whether a set's coordinates are human template millimetres; an `other` set is held.
SPACES: Tuple[str, ...] = ("template", "other")
#: Where a set's coordinates come from.
COORDINATE_SOURCES: Tuple[str, ...] = ("this_study", "prior_study", "atlas_or_database")


#: The fields every set carries under `metadata.set_role`, once the roles stage has decided it.
SET_ROLE_FIELDS = (
    "role",
    "anchor_kind",
    "from_prior_study",
    "prior_study_evidence",
    "space",
    "coordinate_source",
    "role_confidence",
    "role_source",
    "role_origin",
)


def is_decided(metadata: Mapping[str, Any]) -> bool:
    """Whether an analysis's metadata holds a complete role of study_schema's, and `held`.

    The one check every writer of a set's role makes; nothing guesses a role
    for a set that fails it.
    """
    role = metadata.get("set_role")
    return (
        isinstance(metadata.get("held"), bool)
        and isinstance(role, Mapping)
        and all(k in role for k in SET_ROLE_FIELDS)
        and bool(role.get("role_source"))
        and role_error(role) is None
    )


def role_error(fields: Mapping[str, Any]) -> Optional[str]:
    """Why `role`, `anchor_kind` and `from_prior_study` are not study_schema's, or None."""
    role, kind = fields.get("role"), fields.get("anchor_kind")
    if role is not None and role not in COORDINATE_ROLES:
        return f"role {role!r} is not a CoordinateRole"
    if role == CoordinateRole.anchor.value and kind is not None and kind not in ANCHOR_KINDS:
        return f"anchor_kind {kind!r} is not an AnchorKind"
    if role != CoordinateRole.anchor.value and kind is not None:
        return f"anchor_kind {kind!r} on a {role} set"
    if fields.get("from_prior_study") not in (True, False, None):
        return f"from_prior_study {fields.get('from_prior_study')!r} is not a boolean or null"
    if role is None and fields["from_prior_study"]:
        return "from_prior_study on numbers that are not coordinates"
    for name, values in (("space", SPACES), ("coordinate_source", COORDINATE_SOURCES)):
        value = fields.get(name)
        if value is None:
            continue
        if role is None:
            return f"{name} {value!r} on numbers that are not coordinates"
        if value not in values:
            return f"{name} {value!r} is not one of {list(values)}"
    return None


@dataclass(frozen=True)
class SetRole:
    """One set's role fields; `role` None: the numbers are not coordinates."""

    role: Optional[str]
    anchor_kind: Optional[str] = None
    from_prior_study: Optional[bool] = None

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
    #: Why a set has no role, when a rule rather than a model refused it (`geometry`).
    reason: Optional[str] = None
    #: `template` or `other` (`SPACES`); None for a set with no role.
    space: Optional[str] = None
    #: Where the coordinates come from (`COORDINATE_SOURCES`); None for a set with no role.
    coordinate_source: Optional[str] = None

    def __post_init__(self) -> None:
        error = role_error({**self.decided.fields(), "space": self.space,
                            "coordinate_source": self.coordinate_source})
        if error:
            raise ValueError(error)

    @property
    def role(self) -> Optional[str]:
        return self.decided.role

    @property
    def anchor_kind(self) -> Optional[str]:
        return self.decided.anchor_kind

    @property
    def from_prior_study(self) -> Optional[bool]:
        return self.decided.from_prior_study

    @property
    def held(self) -> bool:
        """Anything but a result or anchor in human template millimetres."""
        return not (self.role in OWN_ROLES and self.space == "template")

    def to_metadata(self) -> dict:
        """The decision in CoordinateParse's field names, plus who decided and how surely.

        `role_reason` is there only for a set a rule refused, so a model's
        decisions keep the record they always had.
        """
        out = {
            **self.decided.fields(),
            "space": self.space,
            "coordinate_source": self.coordinate_source,
            "prior_study_evidence": list(self.prior_study_evidence),
            "role_confidence": self.confidence,
            "role_source": self.source,
            "role_origin": self.origin,
        }
        if self.reason is not None:
            out["role_reason"] = self.reason
        return out
