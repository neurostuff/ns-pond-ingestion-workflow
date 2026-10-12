"""The Methods and Results of an article's text, and the cheap test run on it first.

The text is the extraction's (`pipeline.stages.passages`). `coordinate_space.sectionize`
picks its Methods and Results; without recognisable sections the whole text is used.
`may_hold_coordinates` is run on the text first, so the detector reads only texts
where a coordinate could be.
"""

from __future__ import annotations

import unicodedata

from ingestion_workflow.extractors.utils import normalize_minus
from ingestion_workflow.services.coordinate_space import sectionize
from ingestion_workflow.services.prose_passages import PATTERNS, find

KEPT_SECTIONS = ("methods", "results")

#: Bump when `normalized` changes: what it equates is what prose reuses an answer across.
NORMALIZED_VERSION = "nfkc-minus-space.1"


def normalized(text: str) -> str:
    """`text` with what a re-extraction changes and a reader would not: NFKC (ligatures,
    no-break and other spaces, full-width forms), every minus and dash as "-", and each run
    of whitespace as one space. NFKC goes first: it turns a superscript minus into U+2212.
    """
    return " ".join(normalize_minus(unicodedata.normalize("NFKC", text)).split())


def may_hold_coordinates(text: str) -> bool:
    """Whether a text could hold a coordinate in its prose.

    Wherever any of the detector's patterns matches, the detector is put to the
    text around it. A no here is final; a yes still goes through the detector.
    """
    text = " ".join(normalize_minus(text).split())
    for _, pattern in PATTERNS:
        for m in pattern.finditer(text):
            if find(text[max(m.start() - 300, 0):m.end() + 120]):
                return True
    return False


def kept_spans(text: str) -> tuple[list, str]:
    """`(start, end)` of the Methods and Results in `text`, and which were found;
    the whole text when neither section is recognised."""
    spans = [(a, b) for a, b, label in sectionize(text) if label in KEPT_SECTIONS]
    return (spans, "methods+results") if spans else ([(0, len(text))], "full text")


__all__ = ["KEPT_SECTIONS", "NORMALIZED_VERSION", "kept_spans", "may_hold_coordinates",
           "normalized"]
