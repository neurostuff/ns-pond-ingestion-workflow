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

from .labels import RoleDecision, split_label


@dataclass(frozen=True)
class Prediction:
    probabilities: Dict[str, float]
    prior_probability: float

    @property
    def label(self) -> str:
        return max(self.probabilities, key=self.probabilities.get)

    @property
    def confidence(self) -> float:
        return self.probabilities[self.label]


class SetRoleClassifier(Protocol):
    #: `name@version`, recorded as each decision's source.
    source: str

    def predict(self, texts: Sequence[str]) -> List[Prediction]:
        """One prediction per serialised context, in order."""


def decide(
    proposed: str,
    prediction: Optional[Prediction],
    *,
    source: str,
    min_confidence: float,
    prior_threshold: float = 0.5,
    evidence: Sequence[str] = (),
) -> RoleDecision:
    """The recorded role for a set, from its proposal and the classifier's answer.

    `evidence` is the context's citing sentences (`context.prior_evidence`),
    recorded only when the set is judged to come from a prior study.
    """
    if prediction is None:
        label, prior, confidence, decided_by = (
            proposed,
            split_label(proposed)[0] == "reference",
            None,
            "proposal",
        )
    else:
        if prediction.confidence >= min_confidence:
            label, confidence, decided_by = prediction.label, prediction.confidence, source
        else:
            label, confidence, decided_by = (
                proposed,
                prediction.probabilities.get(proposed),
                "proposal",
            )
        # A quoted peak is another study's by definition; otherwise the flag head decides.
        prior = (
            split_label(label)[0] == "reference" or prediction.prior_probability >= prior_threshold
        )
    return RoleDecision(
        label=label,
        from_prior_study=prior,
        confidence=confidence,
        source=decided_by,
        proposed=proposed,
        prior_study_evidence=tuple({"text": s} for s in evidence) if prior else (),
    )


class EncoderClassifier:
    """A fine-tuned encoder, loaded from a model directory (layout in `model.py`)."""

    def __init__(self, path: Path, *, device: str = "cpu", batch_size: int = 32) -> None:
        from . import model as model_io  # noqa: PLC0415 - torch only when a model is configured

        self._model, self._tokenizer, meta = model_io.load(Path(path), device=device)
        self._labels = list(meta["labels"])
        self._device = device
        self._batch_size = batch_size
        self.source = f"{meta['name']}@{meta['version']}"

    def predict(self, texts: Sequence[str]) -> List[Prediction]:
        from . import model as model_io  # noqa: PLC0415

        return [
            Prediction(probs, prior)
            for probs, prior in model_io.predict_batch(
                self._model,
                self._tokenizer,
                texts,
                self._labels,
                device=self._device,
                batch_size=self._batch_size,
            )
        ]
