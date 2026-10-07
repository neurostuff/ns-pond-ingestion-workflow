"""Whether an article is a primary study or a meta-analysis, from its title.

Neurostore stores this as `level`: 'group' for a study of its own participants,
'meta' for a meta-analysis or systematic review. Their coordinates are not
interchangeable -- a meta-analysis reports peaks pooled from other papers, so
counting them beside group studies counts those studies twice.
"""

from __future__ import annotations

import re
from typing import Optional

GROUP = "group"
META = "meta"

_DASH = r"[\s\-‐‑‒–—]?"

#: Titles that name the article as a meta-analysis or a review of studies.
#: The coordinate-based methods count too: an ALE or SDM paper is a
#: meta-analysis whether or not its title says so.
META_TITLE = re.compile(
    rf"\bmeta{_DASH}analy[szt]|\bmeta{_DASH}regression"
    r"|\bsystematic(?:\s+literature)?\s+reviews?\b|\bumbrella\s+reviews?\b"
    r"|\bactivation\s+likelihood\s+estimation\b"
    r"|\b(?:signed\s+differential|seed[\s\-]based\s+d|anisotropic\s+effect[\s\-]size\s+signed\s+differential)\s+mapping\b",
    re.I,
)


#: A primary study that took its regions from a meta-analysis: "meta-analytically
#: defined", "regions based on a meta-analysis ... a resting-state fMRI study".
USES_A_META_ANALYSIS = re.compile(
    rf"\bmeta{_DASH}analytically{_DASH}(?:defined|derived|informed|guided|selected)\b"
    rf"|\b(?:defined|derived|selected|identified)\s+(?:from|by|using)\s+(?:an?\s+|the\s+|prior\s+|previous\s+)?"
    rf"(?:[\w\-\u2010]+\s+){{0,2}}meta{_DASH}analy[szt]"
    rf"|\bregions?\s+based\s+on\s+an?\s+meta{_DASH}analy[szt]",
    re.I,
)


def is_meta(title: Optional[str]) -> bool:
    return bool(title and META_TITLE.search(title) and not USES_A_META_ANALYSIS.search(title))


def level_for(title: Optional[str], current: Optional[str] = None) -> str:
    """The level to store for an article with this title.

    'meta' when the title names a meta-analysis. Otherwise the level already
    stored, so a correction made on Neurostore is not undone, or 'group'.
    """
    if is_meta(title):
        return META
    return current if current in (GROUP, META) else GROUP


__all__ = ["GROUP", "META", "META_TITLE", "USES_A_META_ANALYSIS", "is_meta", "level_for"]
