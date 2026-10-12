"""The request the served NuExtract3 role models answer.

Both role models read one request shape: the instruction, then a blank line,
then the set's serialised context (`table_context.serialize` or
`prose_context.serialize`) as the user message; the role template in the chat
template's `template` block; a strict JSON schema on the answer. They were
trained and evaluated on exactly these strings, so they are kept here verbatim
and never edited: retrained models with another instruction come with a new
`PROMPT` and version. The prompt also records the role vocabulary and the
context versions the models were trained on; `check` refuses it when its
vocabulary is not study_schema's or its contexts are not the ones this code builds.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Tuple

from .labels import ANCHOR_KINDS, COORDINATE_ROLES
from .prose_context import PROSE_CONTEXT_VERSION
from .table_context import TABLE_CONTEXT_VERSION

#: The context version of each origin this code builds: `table` sets and `text` (prose) sets.
CONTEXT_VERSIONS = {"table": TABLE_CONTEXT_VERSION, "text": PROSE_CONTEXT_VERSION}

_V5_INSTRUCTION = (
    "Decide what this coordinate set is to the study that reports it.\n"
    "result: the study's own finding (a contrast, correlation or effect it tested), "
    "including peaks shown in a figure and figure slice or crosshair positions. Also a "
    "result: a location reported WITH the statistics of the effect measured there, even "
    "when it is used as an ROI or seed, borrowed from another study or atlas, or taken "
    "from the authors' own earlier study or dataset; an fNIRS channel or electrode with "
    "its effect statistics; a set mixing this study's peaks with cited ones. When in "
    "doubt, choose result.\n"
    "anchor: a location placed or chosen to define the analysis, given WITHOUT statistics "
    "of an effect measured there: an ROI, a seed (connectivity/PPI), a stimulation target "
    "(TMS, tDCS, DBS, ultrasound), or a network node. An MRS voxel is an anchor ROI.\n"
    "localization: where a recording or measurement site sits (electrode, contact, "
    "channel, recording site), and MEG/EEG dipole or source fits.\n"
    "reference: coordinates quoted from another study for comparison, without statistics "
    "this study measured there.\n"
    "other: a brain coordinate that is none of these (simulated sources, lesion overlap, "
    "atlas examples).\n"
    "Give anchor_kind only for an anchor."
)


#: The role and anchor-kind values, in the order the models' template lists them.
_TRAINED_ROLES = ("result", "anchor", "localization", "reference", "other")
_TRAINED_KINDS = ("roi", "seed", "stimulation_target", "node")


@dataclass(frozen=True)
class RolePrompt:
    version: str
    instruction: str
    roles: Tuple[str, ...] = _TRAINED_ROLES
    anchor_kinds: Tuple[str, ...] = _TRAINED_KINDS
    #: The context version of each origin the models were trained on.
    context_versions: Mapping[str, int] = field(
        default_factory=lambda: {"table": 2, "text": 4}
    )

    def content(self, text: str) -> str:
        """The user message for one serialised context."""
        return self.instruction + "\n\n" + text

    def template(self) -> str:
        return json.dumps({"role": list(self.roles), "anchor_kind": list(self.anchor_kinds)})

    def schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "role": {"enum": list(self.roles)},
                "anchor_kind": {"anyOf": [{"enum": list(self.anchor_kinds)}, {"type": "null"}]},
            },
            "required": ["role"],
        }

    def check(self, origin: str) -> None:
        """Refuse this prompt for `origin` when its models would read other inputs or words."""
        if (self.roles, self.anchor_kinds) != (COORDINATE_ROLES, ANCHOR_KINDS):
            raise ValueError(
                f"prompt {self.version} was trained on roles {list(self.roles)} and anchor kinds "
                f"{list(self.anchor_kinds)}, not study_schema's {list(COORDINATE_ROLES)} and "
                f"{list(ANCHOR_KINDS)}"
            )
        trained, built = self.context_versions.get(origin), CONTEXT_VERSIONS[origin]
        if trained != built:
            raise ValueError(
                f"prompt {self.version}'s models were trained on {origin} context version "
                f"{trained}; this code builds {built}; retrain them"
            )


#: The prompt both served role models were trained and evaluated with.
PROMPT = RolePrompt("v5", _V5_INSTRUCTION)
