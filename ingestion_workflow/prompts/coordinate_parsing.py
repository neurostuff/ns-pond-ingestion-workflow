"""Analysis-boundary rules for coordinate-table parsing.

Vendored from autonima (neurostuff/autonima, `autonima/coordinates/prompts.py`
at 12a3d75), whose benchmark scores these rules against ground-truth brain
maps. Only the boundary rules are taken: they decide how many distinct
analyses a table reports, which is the part autonima measured. The field-level
rules (cluster size, subpeak/seed/deactivation flags, statistic typing) stay in
this repo's prompt, because autonima's schema has no such fields.

Kept as a copy rather than an import because autonima cannot currently be
installed alongside this package: it requires `pubget>=0.0.8`, which resolves
to the PyPI release and pins scikit-learn 1.3.0, which fails to build on
Python 3.13. See neurostuff/autonima#75. Re-sync when that is resolved;
a change in COORDINATE_PARSING_PROMPT_VERSION upstream is the signal one is due.
"""

from __future__ import annotations

from textwrap import dedent

# Mirrors autonima's constant. Included in the create_analyses cache key, so
# bumping it re-parses tables rather than serving output from the old prompt.
COORDINATE_PARSING_PROMPT_VERSION = "2026-07-30.annotations-v3"

ANALYSIS_BOUNDARY_RULES = dedent(
    """
    ANALYSIS-BOUNDARY RULES

    Use two passes. First inventory the table's analysis-defining axes and
    result blocks without extracting points. Then traverse the table and
    assign every coordinate to the appropriate combination of those axes.

    Start a separate analysis when an explicit label changes any of these:
    - contrast or comparison, including direction (A > B versus A < B);
    - activation versus deactivation, positive versus negative, or
      increase versus decrease;
    - participant group or cohort, including "all groups" and each
      separately reported subgroup;
    - treatment, task condition, session, time point, or parametric effect;
    - an analysis/contrast label encoded in a column or multi-level header.

    A table can encode analyses across columns as well as down rows. Do not
    ignore columns named Contrast, Comparison, Group, Condition, Treatment,
    Session, Effect, or similar. When multiple independent
    analysis-defining axes are present, preserve their explicit
    combinations in the analysis names.

    In a wide table, repeated statistic/X/Y/Z column groups beneath
    different top-level headers are separate analysis blocks. A single row
    may therefore contribute one coordinate to several analyses. Route
    each non-empty X/Y/Z group to its own header-defined analysis; never
    pool coordinates from different header blocks into one broad analysis.
    Blank cells in one block do not shift values into an adjacent block.

    Positive and negative statistical values may encode opposite contrast
    directions in one table. When the caption, header, or footnote defines
    what the sign means, create the corresponding directional analyses and
    route points by sign. Do not treat both signs as one map.

    Repeated blank cells inherit the most recent applicable header or
    analysis label. Continuation rows and local maxima belong to that same
    analysis until an analysis-defining label changes.

    Table exports sometimes represent a section header as a row where the
    same nonnumeric label is repeated across most or all columns. Treat
    that row as analysis context, not as data. If it names an experimental
    group, condition, contrast direction, session, or time point, start
    the corresponding analysis and combine it with any applicable parent
    context. When "all groups" and named subgroups are each explicitly
    reported with coordinates, return one analysis for every non-empty
    group block; do not pool the subgroup points into "all groups".

    Do NOT start a new analysis merely because a descriptive anatomical
    grouping changes. Brain region, lobe, cluster, hemisphere/left/right,
    local maximum/subpeak, a-priori versus non-a-priori region, and
    predicted versus non-predicted region are normally subdivisions within
    one statistical map. Split on one of these only if the table or caption
    explicitly identifies it as a distinct statistical contrast/map.
    In particular, left and right hemisphere sections alone never define
    different statistical analyses. "Predicted" and "not predicted"
    normally classify reported regions by prior hypothesis; they are not
    participant groups, contrast directions, or separate maps.

    Distinguish an anatomical ROI label from an explicit analysis-method
    block. A region merely described as an ROI stays in its current
    analysis. However, separately labeled "ROI analysis" and "whole-brain
    analysis" result blocks are distinct analyses because they report
    different statistical searches, even when their contrast is the same.
    An ROI subsection begins where its explicit header appears and applies
    only to the following rows; do not retroactively assign preceding
    whole-brain/unrestricted rows to that ROI subsection.

    If there is no explicit analysis-defining label, treat the whole table
    as one analysis. Use only labels that appear in the table, caption, or
    footnotes. Combine explicit labels when needed to make analyses
    distinguishable, but never invent a contrast or group.

    Before returning, recheck the analysis split:
    - Recheck every repeated X/Y/Z header block: each must either produce
      its own analysis or be intentionally empty.
    - If two outputs differ only by hemisphere, region, cluster,
      predicted/non-predicted status, or local-maximum labels, merge them
      unless the source explicitly defines separate statistical maps.
    - If one output pools points from different contrast, group, sign,
      ROI-analysis, whole-brain-analysis, session, or condition blocks,
      split it before returning.
    """
).strip()


__all__ = ["ANALYSIS_BOUNDARY_RULES", "COORDINATE_PARSING_PROMPT_VERSION"]
