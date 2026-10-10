"""DOI patterns and cleaning, shared by identifiers, PDFs, reference lists and notices."""

from __future__ import annotations

import re
from typing import Optional

#: A DOI inside running text; trailing punctuation is left for `clean_doi`.
DOI = re.compile(r"10\.\d{4,9}/[^\s\"'<>]+")
#: A DOI printed as a resolver URL, capturing the DOI.
DOI_URL = re.compile(r'(?i)https?://[^/\s]+/(10\.\d{4,9}/[^\s"\'<>()]+)')
_TRAILING = ".,;:"


def normalize_doi(value: Optional[str]) -> Optional[str]:
    """The bare DOI of `value`, which may be a resolver URL or `doi:`-prefixed."""
    value = (value or "").strip()
    if value.lower().startswith("http"):
        value = DOI_URL.sub(r"\1", value)
    if value.lower().startswith("doi:"):
        value = value[4:]
    return value or None


def clean_doi(doi: str) -> Optional[str]:
    """Drop the sentence punctuation a DOI was printed against.

    A closing bracket is the DOI's own only when it opened one, as in
    `10.1016/S0924-9338(02)00676-4`.
    """
    while doi:
        if doi[-1] in _TRAILING:
            doi = doi[:-1]
        elif doi[-1] in ")]" and doi.count(doi[-1]) > doi.count("(" if doi[-1] == ")" else "["):
            doi = doi[:-1]
        else:
            break
    # A suffix with no digit is a fragment: `10.1172/jci` of `10.1172/jci.insight.182331`.
    suffix = doi.partition("/")[2]
    return doi if re.search(r"\d", suffix) and not suffix.endswith("-") else None


def find_doi(text: Optional[str]) -> Optional[str]:
    """The first DOI in `text`, cleaned, or None."""
    match = DOI.search(text or "")
    return clean_doi(match.group(0)) if match else None
