"""What each coordinate set is for: a result, an anchor, a reference or other.

Its own step, between the parse and the space stage, rather than a side answer
of the prose model: it reads table sets and prose sets, each with the context
of its origin, and decides the role, whether the coordinates are human template
millimetres, and where they come from. `table_context` and `prose_context` build
the two inputs (they share only `common`'s helpers and `labels`' study_schema
role fields); `decision` is the one model that reads both, an encoder with five
heads loaded in the stage's process; `classifier` turns its answer into the
recorded role, with no default. `geometry` refuses, before any model is asked,
a set whose numbers cannot be template brain coordinates.
`labeling` and `export` make its training data, and the role-bearing rows for
the two extractors. The roles stage (`pipeline.stages.roles`) runs it;
experiments/role_classifier holds the job scripts.
"""

from .classifier import Prediction, decide
from .labels import (
    ANCHOR_KINDS,
    COORDINATE_ROLES,
    COORDINATE_SOURCES,
    SPACES,
    OWN_ROLES,
    RoleDecision,
    SetRole,
)
from .prose_context import PROSE_CONTEXT_VERSION, ProseSetContext
from .table_context import TABLE_CONTEXT_VERSION, TableSetContext

__all__ = [
    "ANCHOR_KINDS",
    "COORDINATE_ROLES",
    "COORDINATE_SOURCES",
    "PROSE_CONTEXT_VERSION",
    "Prediction",
    "ProseSetContext",
    "RoleDecision",
    "SPACES",
    "SetRole",
    "TABLE_CONTEXT_VERSION",
    "TableSetContext",
    "OWN_ROLES",
    "decide",
]
