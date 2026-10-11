"""Turning a classifier's answer into the role a set is recorded with.

Every set's role is its origin's classifier's answer: there is no proposal to
fall back on and no default role. The answer is recorded with its confidence,
so a reviewer can find the unsure ones. A set the classifier could not answer
(`Unanswered`) gets no role; the roles stage records why.

Two backends answer the one `RoleClassifier` interface: `EncoderClassifier`, a
fine-tuned encoder loaded from a model directory, and
`served.ServedClassifier`, a NuExtract3 role model behind a vLLM endpoint.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Mapping, Optional, Protocol, Sequence, Union

from .labels import ANCHOR_KINDS, RoleDecision, SetRole


@dataclass(frozen=True)
class Prediction:
    #: That the numbers are brain coordinates; None from a model that does not judge it.
    coordinates_probability: Optional[float]
    #: By `CoordinateRole` value, and by `AnchorKind` value (read for an anchor only).
    role_probabilities: Dict[str, float]
    kind_probabilities: Dict[str, float]
    #: That the coordinates come from another publication; None from a model without that answer.
    prior_probability: Optional[float]
    #: The role and anchor kind a generative model wrote, which its probabilities
    #: (read from one token's alternatives) need not rank first. None: the top-ranked.
    answer: Optional[str] = None
    answer_kind: Optional[str] = None

    @property
    def role(self) -> str:
        if self.answer is not None:
            return self.answer
        return max(self.role_probabilities, key=self.role_probabilities.get)

    @property
    def confidence(self) -> float:
        return self.role_probabilities.get(self.role, 0.0)

    @property
    def anchor_kind(self) -> Optional[str]:
        if self.role != "anchor":
            return None
        if self.answer_kind is not None:
            return self.answer_kind
        return max(ANCHOR_KINDS, key=lambda k: self.kind_probabilities.get(k, 0.0))


@dataclass(frozen=True)
class Unanswered:
    """A set the classifier gave no usable answer for, and why."""

    reason: str


class RoleClassifier(Protocol):
    """What the roles stage calls, whichever backend answers."""

    #: `name@version`, recorded as each decision's source.
    source: str
    #: A role whose probability reaches its threshold here is decided, whatever
    #: the classifier's top answer (`decide`); empty: the top answer stands.
    role_thresholds: Mapping[str, float]

    def predict(self, texts: Sequence[str]) -> List[Union[Prediction, Unanswered]]:
        """One answer per serialised context, in order."""


def decide(
    prediction: Prediction,
    *,
    source: str,
    origin: str,
    coordinates_threshold: float = 0.5,
    prior_threshold: float = 0.5,
    role_thresholds: Optional[Mapping[str, float]] = None,
    evidence: Sequence[str] = (),
) -> RoleDecision:
    """The recorded role for a set, from its classifier's answer alone.

    Below `coordinates_threshold` the numbers are not coordinates (`role` None);
    otherwise the role is the classifier's answer, or a role of
    `role_thresholds` whose probability reaches its threshold (a recall-first
    result threshold, say). A threshold only ever adds its role: below it the
    answer stands. `evidence` is the context's
    citing sentences (`context.prior_evidence`), recorded only when the set is
    judged to come from a prior study.
    """
    if not isinstance(prediction, Prediction):
        raise TypeError(f"a role needs the classifier's prediction, got {prediction!r}")
    coordinates = prediction.coordinates_probability
    if coordinates is not None and coordinates < coordinates_threshold:
        decided, confidence = SetRole(None), 1 - coordinates
    else:
        role, kind = prediction.role, prediction.anchor_kind
        for name, threshold in (role_thresholds or {}).items():
            if prediction.role_probabilities.get(name, 0.0) >= threshold:
                role, kind = name, prediction.anchor_kind if name == role else None
        confidence = prediction.role_probabilities.get(role, 0.0)
        # A quoted peak is another study's by definition; otherwise the flag head decides.
        prior = role == "reference" or (
            prediction.prior_probability is not None
            and prediction.prior_probability >= prior_threshold
        )
        decided = SetRole(role, kind, prior)
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
        self.role_thresholds: Dict[str, float] = {}

    def predict(self, texts: Sequence[str]) -> List[Union[Prediction, Unanswered]]:
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
