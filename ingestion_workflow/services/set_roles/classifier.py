"""Turning a classifier's answer into the role a set is recorded with.

Every set's role is its origin's classifier's answer: there is no proposal to
fall back on and no default role. The answer is recorded with its confidence,
so a reviewer can find the unsure ones.
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
    prediction: Prediction,
    *,
    source: str,
    origin: str,
    coordinates_threshold: float = 0.5,
    prior_threshold: float = 0.5,
    evidence: Sequence[str] = (),
) -> RoleDecision:
    """The recorded role for a set, from its classifier's answer alone.

    Below `coordinates_threshold` the numbers are not coordinates (`role` None);
    otherwise the role is the role head's top answer. `evidence` is the
    context's citing sentences (`context.prior_evidence`), recorded only when
    the set is judged to come from a prior study.
    """
    if not isinstance(prediction, Prediction):
        raise TypeError(f"a role needs the classifier's prediction, got {prediction!r}")
    if prediction.coordinates_probability < coordinates_threshold:
        decided, confidence = SetRole(None), 1 - prediction.coordinates_probability
    else:
        role, confidence = prediction.role, prediction.confidence
        # A quoted peak is another study's by definition; otherwise the flag head decides.
        prior = role == "reference" or prediction.prior_probability >= prior_threshold
        decided = SetRole(role, prediction.anchor_kind, prior)
    return RoleDecision(
        decided=decided,
        confidence=confidence,
        source=source,
        origin=origin,
        prior_study_evidence=(
            tuple({"text": s} for s in evidence) if decided.from_prior_study else ()
        ),
    )


class EncoderClassifier:
    """One origin's fine-tuned encoder, loaded from a model directory (layout in `model.py`)."""

    def __init__(
        self, path: Path, origin: str, *, device: str = "cpu", batch_size: int = 32
    ) -> None:
        from . import model as model_io  # noqa: PLC0415 - torch only when a model is configured

        self._model, self._tokenizer, meta = model_io.load(Path(path), origin, device=device)
        self.origin = origin
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
