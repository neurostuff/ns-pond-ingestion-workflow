"""The canonical spelling of a PubMed Central id, shared by identifiers, sources and caches."""

from __future__ import annotations

import re
from typing import Optional

#: A PMCID bare (`7212345`), prefixed in either case (`PMC7212345`, `pmc7212345`),
#: or in an article address of PMC's old or new website, the form OpenAlex sends.
_PMCID = re.compile(
    r"(?:https?://(?:www\.)?(?:ncbi\.nlm\.nih\.gov/pmc|pmc\.ncbi\.nlm\.nih\.gov)/articles/)?"
    r"(?:PMC)?([0-9]+)/?",
    re.IGNORECASE,
)


def normalize_pmcid(value: Optional[str]) -> Optional[str]:
    """The canonical PMCID of `value`: `PMC` and the number, without leading zeros.

    So two spellings of one PMCID compare equal. A value that is not a PMCID,
    PMC0 included, raises ValueError.
    """
    value = (value or "").strip()
    if not value:
        return None
    match = _PMCID.fullmatch(value)
    if match is None or int(match.group(1)) == 0:
        raise ValueError(f"not a PMCID: {value!r}")
    return f"PMC{int(match.group(1))}"
