"""What each coordinate set is for: a result, an anchor, a reference, a display.

Its own step, between the parse and the space stage, rather than a side answer
of the prose model: it reads table sets and prose sets alike, with their
context, and decides the role and whether the coordinates come from another
publication. See `context` for the input, `labels` for the output, `model` for
the fine-tuned encoder, and `classifier` for when its answer overrides the
proposal. A draft: there is no training code or training data yet.
"""

from .classifier import EncoderClassifier, Prediction, SetRoleClassifier, decide
from .context import CONTEXT_VERSION, SetContext, contexts_for, serialize
from .labels import ROLE_LABELS, UPLOADED_ROLES, RoleDecision, label_from_prose

__all__ = [
    "CONTEXT_VERSION",
    "EncoderClassifier",
    "Prediction",
    "ROLE_LABELS",
    "RoleDecision",
    "SetContext",
    "SetRoleClassifier",
    "UPLOADED_ROLES",
    "contexts_for",
    "decide",
    "label_from_prose",
    "serialize",
]
