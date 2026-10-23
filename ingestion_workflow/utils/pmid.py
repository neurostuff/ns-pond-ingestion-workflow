"""The canonical spelling of a PubMed id, shared by identifiers, sources and caches."""

from __future__ import annotations

import re
from typing import Optional

#: A PMID bare (`007212345`), after `PMID:` in either case, or in a PubMed address.
_PMID = re.compile(
    r"(?:https?://pubmed\.ncbi\.nlm\.nih\.gov/|pmid:\s*)?([0-9]+)/?",
    re.IGNORECASE,
)


def normalize_pmid(value: Optional[str]) -> Optional[str]:
    """The canonical PMID of `value`: its digits, without leading zeros.

    So two spellings of one PMID compare equal. What is not a spelling of a PMID is
    returned as given: the catalog holds such ids (the Neurosynth import's
    `<pmid>-DUPLICATE`), and rewriting them would give one article another's id.
    """
    value = str(value or "").strip()
    match = _PMID.fullmatch(value)
    return str(int(match.group(1))) if match else value or None
