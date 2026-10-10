"""The prompt, template and schema for coordinates written in prose.

Base NuExtract3 reads prose with an instruction and an object-form template;
the table extractor's tuple form is something only its fine-tune learned.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Optional

from study_schema.models.paper_parse import AnchorKind, CoordinateRole

from ingestion_workflow.models.statistics import STATISTIC_KINDS

PROSE_PROMPT_VERSION = "2026-10-06.n5"

#: The current nu-prose model's own role vocabulary: its template and the
#: dataset it was trained on use it. Nothing past `study_schema_role` sees it.
ROLES = ("result", "roi", "seed", "target", "prior_study", "figure", "other")

_A = CoordinateRole.anchor.value
#: LEGACY ADAPTER -- nu-prose answers in its own role words; this reads them into
#: study_schema's fields, where they are only the roles stage's proposal (the
#: roles stage decides). Keep it while nu-prose emits these words. The model's `other` is "anything
#: else" and does not separate a real brain coordinate that fits no role
#: (study_schema's `other`) from numbers that are not coordinates; the second is
#: most of it, so it maps to not-coordinates. The real ones come from the roles
#: classifier, not from this adapter.
_STUDY_SCHEMA_ROLES = {
    "result": (CoordinateRole.result.value, None, False),
    "roi": (_A, AnchorKind.roi.value, False),
    "seed": (_A, AnchorKind.seed.value, False),
    "target": (_A, AnchorKind.stimulation_target.value, False),
    "prior_study": (CoordinateRole.reference.value, None, True),
    "figure": (CoordinateRole.display.value, None, False),
}


def study_schema_role(role: Optional[str]) -> Dict[str, Any]:
    """A nu-prose role as study_schema's `role`, `anchor_kind` and `from_prior_study`.

    No role is a result; `other` (or anything outside `ROLES`) is `role` None: not
    coordinates, even where it was a real coordinate (see `_STUDY_SCHEMA_ROLES`).
    """
    fields = _STUDY_SCHEMA_ROLES.get(role or "result", (None, None, False))
    return dict(zip(("role", "anchor_kind", "from_prior_study"), fields))

INSTRUCTION = (
    "List every brain coordinate in the passage, including region-of-interest centres, seeds, "
    "stimulation targets and coordinates quoted from other studies, and label each with its role. "
    "Put each coordinate under the analysis or contrast it was reported for, and if one coordinate "
    "is reported for several contrasts, list it under each of them. With each coordinate give the "
    "statistic reported for it (t, Z, F, r, ...) and its value, and the cluster size. "
    "Roles: result = a location this study found (a peak, cluster, local maximum or centre of "
    "gravity of an effect), including effects found inside a region of interest or connected to a "
    "seed; roi = a region defined before the analysis and used to extract or restrict data; seed = "
    "the region whose signal seeds a connectivity or PPI analysis; target = where the brain was "
    "stimulated (TMS, tDCS, ultrasound, DBS) or a lesion or electrode was placed; prior_study = "
    "coordinates quoted from other studies only for comparison; figure = a location used only to "
    "display or illustrate (slice position, crosshairs, an example voxel); other = a brain "
    "coordinate that is none of these (a simulated source position or lesion centre, a worked-example "
    "voxel of an atlas). "
    "Ignore numbers that are not brain coordinates, such as voxel sizes or molecular docking grids. "
    "Name each analysis after the contrast, comparison, correlation or model its coordinates are a "
    "result of, in the paper's own words: 'faces > houses' when a direction is stated and 'faces vs "
    "houses' when it is not, 'group x condition interaction', 'main effect of load', 'positive "
    "correlation with craving', 'parametric modulation by reward value', 'PPI with amygdala seed: "
    "threat > safe'. Add the group, subset or covariate when it separates analyses ('patients < "
    "controls, covarying for IQ'). Resolve references such as 'the reverse contrast' or 'this "
    "comparison' to the contrast they mean. Never name an analysis after a section heading, a "
    "method or a region ('Whole-brain analyses', 'ROI analysis', 'Results', 'left amygdala'). A seed "
    "or ROI is 'amygdala seed' or 'insula ROI' unless the passage says which contrast defined it. "
    "Examples: 'Compared with controls, patients showed reduced activation in the ACC (4, 30, 22), "
    "which remained when IQ was covaried (6, 28, 24). The reverse contrast showed greater activation "
    "in the precuneus (-6, -60, 40)' has three analyses: 'patients < controls' (4, 30, 22), "
    "'patients < controls, covarying for IQ' (6, 28, 24) and 'patients > controls' (-6, -60, 40). "
    "'Whole-brain analyses: activity in the striatum (12, 10, -4) increased with reward magnitude' "
    "has one: 'parametric modulation by reward magnitude'."
)
WITH_CONTEXT = (
    " The passage may be preceded and followed by context from the same article; list only the "
    "coordinates in the Passage, and use the context to name their analyses."
)

TEMPLATE = json.dumps({
    "space": ["MNI", "TAL"],
    "analyses": [{
        "name": "string",
        "measure": ["voxels", "mm^3"],
        "points": [{
            "x": "number", "y": "number", "z": "number",
            "statistic": list(STATISTIC_KINDS), "value": "number", "cluster_size": "integer",
            "role": list(ROLES),
        }],
    }],
}, ensure_ascii=False)

#: Just above the most any labelled passage holds (26 analyses, 14 points in
#: one, a 205-character name). Without caps a looping answer repeats whole
#: analyses until the token budget cuts it off as invalid JSON.
MAX_ANALYSES = 30
MAX_POINTS = 24
MAX_NAME = 240


def schema() -> Dict[str, Any]:
    """`TEMPLATE` as a JSON schema for constrained decoding, every key required."""
    template = json.loads(TEMPLATE)
    analysis = template["analyses"][0]
    point = analysis["points"][0]

    def slot(declared: Any) -> Dict[str, Any]:
        if isinstance(declared, list):
            return {"enum": [*declared, None]}
        return {"type": [declared if declared == "integer" else "number", "null"]}

    point_schema = {
        "type": "object",
        "properties": {k: ({"type": "number"} if k in "xyz" else slot(v)) for k, v in point.items()},
        "required": list(point),
        "additionalProperties": False,
    }
    return {
        "type": "object",
        "properties": {
            "space": slot(template["space"]),
            "analyses": {
                "type": "array",
                "maxItems": MAX_ANALYSES,
                "items": {
                    "type": "object",
                    "properties": {
                        "name": {"type": ["string", "null"], "maxLength": MAX_NAME},
                        "measure": slot(analysis["measure"]),
                        "points": {"type": "array", "maxItems": MAX_POINTS, "items": point_schema},
                    },
                    "required": ["name", "measure", "points"],
                    "additionalProperties": False,
                },
            },
        },
        "required": ["space", "analyses"],
        "additionalProperties": False,
    }


__all__ = ["INSTRUCTION", "MAX_ANALYSES", "MAX_NAME", "MAX_POINTS", "PROSE_PROMPT_VERSION",
           "ROLES", "TEMPLATE", "study_schema_role", "WITH_CONTEXT", "schema"]
