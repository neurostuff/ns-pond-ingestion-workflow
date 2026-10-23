"""Spellings of author and institution ids, and an affiliation's text without addresses."""

from __future__ import annotations

import re
from typing import Optional

_ORCID = re.compile(r"(\d{4})-?(\d{4})-?(\d{4})-?(\d{3}[\dXx])")
_OPENALEX_AUTHOR = re.compile(r"(A\d+)\s*$")
_ROR = re.compile(r"(0[a-z0-9]{6}[0-9]{2})\s*$")
_EMAIL = re.compile(r"[\w.+'-]+@[\w-]+(?:\.[\w-]+)+")
#: What prints an address in an affiliation: PubMed's "Electronic address:", "E-mail:",
#: "email", each with the punctuation around it.
_EMAIL_LABEL = re.compile(
    r"[\s.,;:]*(?:electronic address|e-?mail(?: address)?|email)\s*[:.]?\s*$", re.I)


def orcid(value: Optional[str]) -> Optional[str]:
    """The bare ORCID iD in any spelling (`https://orcid.org/...`, no hyphens)."""
    found = _ORCID.search(value or "")
    return "-".join(found.groups()).upper() if found else None


def openalex_author(value: Optional[str]) -> Optional[str]:
    """The bare OpenAlex author id (`A5054079188`), from its URL or itself."""
    found = _OPENALEX_AUTHOR.search(value or "")
    return found.group(1) if found else None


def ror(value: Optional[str]) -> Optional[str]:
    """The bare ROR id, from its URL or itself."""
    found = _ROR.search((value or "").rstrip("/"))
    return found.group(1) if found else None


def affiliation_text(text: Optional[str]) -> Optional[str]:
    """An affiliation as printed, less any email address and the words that label it.

    Addresses are kept only encrypted, so none is left in an affiliation's text: PubMed
    ends a corresponding author's with "Electronic address: x@y.org."
    """
    if not text:
        return None
    if not _EMAIL.search(text):
        return re.sub(r"\s+", " ", text).strip() or None
    cleaned = text
    while True:
        found = _EMAIL.search(cleaned)
        if found is None:
            break
        head = _EMAIL_LABEL.sub("", cleaned[:found.start()])
        cleaned = head + cleaned[found.end():]
    cleaned = re.sub(r"([;,])(?:\s*[;,])+", r"\1", re.sub(r"\s+", " ", cleaned))
    cleaned = cleaned.strip(" ,;:.")
    return cleaned or None
