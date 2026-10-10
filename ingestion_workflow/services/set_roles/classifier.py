"""Turning a classifier's answer into the role a set is recorded with.

The classifier only overrides the proposal when it is confident: a table set is
proposed `result` and a prose set its prose model's role, and a wrong override
is costly -- a real result reclassified as a reference is not uploaded. Below
`min_confidence` the proposal stands, and the record says so.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Protocol, Sequence

from .labels import ANCHOR_KINDS, RoleDecision, SetRole


@dataclass(frozen=True)
class Prediction:
    #: That the numbers are brain coordinates.
    coordinates_probability: float
    #: By `CoordinateRole` value, and by `AnchorKind` value (read for an anchor only).
    role_probabilities: Dict[str, float]
    kind_probabilities: Dict[str, float]
    prior_probability: float

    @property
    def role(self) -> str:
        return max(self.role_probabilities, key=self.role_probabilities.get)

    @property
    def confidence(self) -> float:
        return self.role_probabilities[self.role]

    @property
    def anchor_kind(self) -> Optional[str]:
        if self.role != "anchor":
            return None
        return max(ANCHOR_KINDS, key=lambda k: self.kind_probabilities.get(k, 0.0))


class SetRoleClassifier(Protocol):
    #: `name@version`, recorded as each decision's source.
    source: str

    def predict(self, texts: Sequence[str]) -> List[Prediction]:
        """One prediction per serialised context, in order."""


def decide(
    proposed: SetRole,
    prediction: Optional[Prediction],
    *,
    source: str,
    min_confidence: float,
    prior_threshold: float = 0.5,
    evidence: Sequence[str] = (),
) -> RoleDecision:
    """The recorded role for a set, from its proposal and the classifier's answer.

    The classifier sets numbers aside as not coordinates, or overrides the
    proposed role, only at `min_confidence`. `evidence` is the context's citing
    sentences (`context.prior_evidence`), recorded only when the set is judged
    to come from a prior study.
    """
    if prediction is None:
        decided, confidence, decided_by = proposed, None, "proposal"
    elif 1 - prediction.coordinates_probability >= min_confidence:
        decided, confidence, decided_by = (
            SetRole(None),
            1 - prediction.coordinates_probability,
            source,
        )
    else:
        if prediction.confidence >= min_confidence:
            role, kind, confidence, decided_by = (
                prediction.role,
                prediction.anchor_kind,
                prediction.confidence,
                source,
            )
        else:
            role, kind, confidence, decided_by = (
                proposed.role,
                proposed.anchor_kind,
                prediction.role_probabilities.get(proposed.role),
                "proposal",
            )
        # A quoted peak is another study's by definition; otherwise the flag head decides.
        prior = role is not None and (
            role == "reference" or prediction.prior_probability >= prior_threshold
        )
        decided = SetRole(role, kind, prior)
    return RoleDecision(
        decided=decided,
        confidence=confidence,
        source=decided_by,
        proposed=proposed,
        prior_study_evidence=(
            tuple({"text": s} for s in evidence) if decided.from_prior_study else ()
        ),
    )


class EncoderClassifier:
    """A fine-tuned encoder, loaded from a model directory (layout in `model.py`)."""

    def __init__(self, path: Path, *, device: str = "cpu", batch_size: int = 32) -> None:
        from . import model as model_io  # noqa: PLC0415 - torch only when a model is configured

        self._model, self._tokenizer, meta = model_io.load(Path(path), device=device)
        self._device = device
        self._batch_size = batch_size
        self.source = f"{meta['name']}@{meta['version']}"

    def predict(self, texts: Sequence[str]) -> List[Prediction]:
        from . import model as model_io  # noqa: PLC0415

        return [
            Prediction(**fields)
            for fields in model_io.predict_batch(
                self._model,
                self._tokenizer,
                texts,
                device=self._device,
                batch_size=self._batch_size,
            )
        ]
