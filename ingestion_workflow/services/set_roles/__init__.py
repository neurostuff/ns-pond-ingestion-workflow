"""What each coordinate set is for: a result, an anchor, a reference, a display.

Its own step, between the parse and the space stage, rather than a side answer
of the prose model: it reads table sets and prose sets, each with the context
of its origin, and decides the role and whether the coordinates come from
another publication. `table_context` and `prose_context` build the two inputs
(they share only `common`'s helpers and `labels`' study_schema role fields), `model` is the
fine-tuned encoder, `classifier` decides when its answer overrides the
proposal. `labeling` and `export` make its training data, and the role-bearing
rows for the two extractors. The roles stage (`pipeline.stages.roles`) runs it;
experiments/role_classifier holds the job scripts.
"""

from .classifier import EncoderClassifier, Prediction, SetRoleClassifier, decide
from .labels import ANCHOR_KINDS, COORDINATE_ROLES, UPLOADED_ROLES, RoleDecision, SetRole
from .prose_context import PROSE_CONTEXT_VERSION, ProseSetContext
from .table_context import TABLE_CONTEXT_VERSION, TableSetContext

__all__ = [
    "ANCHOR_KINDS",
    "COORDINATE_ROLES",
    "EncoderClassifier",
    "PROSE_CONTEXT_VERSION",
    "Prediction",
    "ProseSetContext",
    "RoleDecision",
    "SetRole",
    "SetRoleClassifier",
    "TABLE_CONTEXT_VERSION",
    "TableSetContext",
    "UPLOADED_ROLES",
    "decide",
]
