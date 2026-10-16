"""DOI patterns and cleaning, shared by identifiers, PDFs, reference lists and notices."""

from __future__ import annotations

import re
import string
from typing import Optional

#: A DOI inside running text; trailing punctuation is left for `clean_doi`.
DOI = re.compile(r"10\.\d{4,9}/[^\s\"'<>]+")
#: The resolver address in front of a DOI printed as a URL.
RESOLVER = re.compile(r"(?i)https?://[^/\s]+/(?=10\.\d{4,9}/)")
_TRAILING = ".,;:"
#: DOIs are equal ignoring the case of A-Z, and of no other letter: DOI
#: Handbook 4.3.4, so `10.26321/Á...` and `10.26321/á...` are two DOIs.
_ASCII_LOWER = str.maketrans(string.ascii_uppercase, string.ascii_lowercase)


def normalize_doi(value: Optional[str]) -> Optional[str]:
    """The canonical DOI of `value`, which may be a resolver URL or `doi:`-prefixed.

    Canonical is bare and with A-Z in lower case, so two spellings of one DOI
    compare equal.
    """
    value = (value or "").strip()
    # Everything after the resolver is the DOI, brackets included: SICI and
    # Elsevier DOIs contain `(`, `)`, `<` and `>`.
    value = RESOLVER.sub("", value, count=1) if value.lower().startswith("http") else value
    if value.lower().startswith("doi:"):
        value = value[4:]
    return value.translate(_ASCII_LOWER) or None


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
