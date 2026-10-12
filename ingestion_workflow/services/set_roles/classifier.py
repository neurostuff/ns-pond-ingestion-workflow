"""Turning a role model's answer into the role a set is recorded with.

Every set's role is its origin's role model's answer (`served`): there is no
proposal to fall back on and no default role. The answer is recorded with its
probability, so a reviewer can find the unsure ones. A set the model gave no
usable answer for (`Unanswered`) gets no role; the roles stage records why.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Sequence

from .labels import RoleDecision, SetRole


@dataclass(frozen=True)
class Prediction:
    #: The `CoordinateRole` value the model answered.
    role: str
    #: Its `AnchorKind` value, for an anchor only.
    anchor_kind: Optional[str]
    #: Each role's probability at the answer's first role token, by `CoordinateRole` value.
    role_probabilities: Dict[str, float]

    @property
    def confidence(self) -> float:
        return self.role_probabilities.get(self.role, 0.0)


@dataclass(frozen=True)
class Unanswered:
    """A set the model gave no usable answer for, and why."""

    reason: str


def decide(
    prediction: Prediction,
    *,
    source: str,
    origin: str,
    evidence: Sequence[str] = (),
) -> RoleDecision:
    """The recorded role for a set: its model's answer, with that answer's probability.

    A quoted peak (`reference`) is another study's by definition (study_schema:
    always true for `reference`). Nothing decides it for any other role, so it
    stays null there and is never set false. `evidence` is the context's citing
    sentences (`context.prior_evidence`), recorded only for a set from a prior study.
    """
    if not isinstance(prediction, Prediction):
        raise TypeError(f"a role needs the classifier's prediction, got {prediction!r}")
    prior = True if prediction.role == "reference" else None
    return RoleDecision(
        decided=SetRole(prediction.role, prediction.anchor_kind, prior),
        confidence=prediction.confidence,
        source=source,
        origin=origin,
        prior_study_evidence=tuple({"text": s} for s in evidence) if prior else (),
    )
