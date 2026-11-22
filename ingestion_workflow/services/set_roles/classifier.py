"""Turning the role model's answer into the role a set is recorded with.

Every set's role is the decision model's answer (`decision`): there is no
proposal to fall back on and no default role. The answer is recorded with its
probability, so a reviewer can find the unsure ones.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Sequence

from .labels import RoleDecision, SetRole


@dataclass(frozen=True)
class Prediction:
    #: The `CoordinateRole` value the model answered; None: the numbers are not coordinates.
    role: Optional[str]
    #: Its `AnchorKind` value, for an anchor only.
    anchor_kind: Optional[str]
    #: Each role's probability, by `CoordinateRole` value.
    role_probabilities: Dict[str, float]
    #: `template` or `other` (`labels.SPACES`); None with no role.
    space: Optional[str]
    #: Where the coordinates come from (`labels.COORDINATE_SOURCES`); None with no role.
    source: Optional[str]
    #: The probability that the numbers locate something in a brain or on a head.
    coordinates_probability: float

    @property
    def confidence(self) -> float:
        """The probability of what was decided: the role's, or that the numbers are not
        coordinates."""
        if self.role is None:
            return 1.0 - self.coordinates_probability
        return self.role_probabilities.get(self.role, 0.0)


def decide(
    prediction: Prediction,
    *,
    source: str,
    origin: str,
    evidence: Sequence[str] = (),
) -> RoleDecision:
    """The recorded role for a set: its model's answer, with that answer's probability.

    `from_prior_study` is the model's source: true for a prior study's
    coordinates, and for a quoted peak (`reference`) whatever the source (study_schema:
    always true for `reference`); false for this study's and an atlas's or
    database's. `evidence` is the context's citing sentences
    (`context.prior_evidence`), recorded only for a set from a prior study.
    """
    if not isinstance(prediction, Prediction):
        raise TypeError(f"a role needs the classifier's prediction, got {prediction!r}")
    if prediction.role is None:
        return RoleDecision(decided=SetRole(None), confidence=prediction.confidence,
                            source=source, origin=origin)
    prior = prediction.role == "reference" or prediction.source == "prior_study"
    return RoleDecision(
        decided=SetRole(prediction.role, prediction.anchor_kind, prior),
        confidence=prediction.confidence,
        source=source,
        origin=origin,
        prior_study_evidence=tuple({"text": s} for s in evidence) if prior else (),
        space=prediction.space,
        coordinate_source=prediction.source,
    )
