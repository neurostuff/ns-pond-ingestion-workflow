"""A paper's reference list from Crossref, and the identifiers it lends to the source's own list.

Crossref holds the list its publisher deposited, in order, with a DOI on 92% of entries
(measured on 200 ns-pond papers); whole publishers deposit nothing (APA, AMA, some of
J Neurosci). Many entries are a bare DOI, so OpenAlex supplies the names and year that
author-year citations are matched on.
"""

from __future__ import annotations

import re
import unicodedata
from typing import Dict, List, Optional


def _year(value) -> Optional[int]:
    m = re.search(r"\b(1[5-9]\d\d|20\d\d)", str(value or ""))
    return int(m.group(1)) if m else None


def _doi(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    value = re.sub(r"^(https?://(dx\.)?doi\.org/|doi:)", "", value.strip(), flags=re.I)
    return value.lower() or None


def _fold(s: str) -> str:
    s = unicodedata.normalize("NFKD", s or "")
    return "".join(c for c in s if not unicodedata.combining(c)).lower()


def surname(name: str, given_first: bool) -> str:
    """'Smith, J.' / 'Silberstein SD' (Crossref) / 'Jane A. Smith' (OpenAlex, `given_first`)."""
    name = (name or "").strip()
    if "," in name:
        return name.split(",")[0].strip()
    toks = name.split()
    while len(toks) > 1 and re.fullmatch(r"(?:[A-Z]\.?-?){1,3}", toks[-1]):
        toks.pop()  # trailing initials
    if not toks:
        return ""
    return toks[-1] if given_first else " ".join(toks)


def from_crossref(message: Optional[Dict]) -> List[dict]:
    """Crossref's `reference` array as reference entries, in its order."""
    refs = []
    for i, e in enumerate((message or {}).get("reference") or []):
        text = e.get("unstructured") or " ".join(
            str(e[k]) for k in ("author", "year", "article-title", "journal-title", "volume", "first-page") if e.get(k))
        refs.append({
            "id": f"cr{i + 1}",
            "position": i + 1,
            "provider": "crossref",
            "label": None,  # Crossref keeps no printed number; numbered markers index the position
            "key": e.get("key"),
            "text": text or e.get("DOI") or "",
            "doi": _doi(e.get("DOI")),
            "pmid": None,
            "id_providers": {"doi": "crossref"} if e.get("DOI") else {},
            "pmcid": None,
            "title": e.get("article-title") or None,
            "year": _year(e.get("year")),
            "authors": [surname(e["author"], given_first=False)] if e.get("author") else [],
        })
    return refs


def enrich(refs: List[dict], openalex) -> int:
    """Names, year and PMID from OpenAlex for entries that have a DOI; returns how many it filled."""
    wanted = [r["doi"] for r in refs if r["doi"] and (not r["authors"] or not r["year"] or not r["pmid"])]
    if not wanted or openalex is None:
        return 0
    found = openalex.get_cited_works(wanted)
    filled = 0
    for r in refs:
        work = found.get(r["doi"] or "")
        if not work:
            continue
        before = (bool(r["authors"]), bool(r["year"]), bool(r["pmid"]))
        if not r["authors"]:
            r["authors"] = [surname(a, given_first=True) for a in work["authors"] if a]
        r["year"] = r["year"] or work.get("year")
        if not r["pmid"] and work.get("pmid"):
            r["pmid"] = work["pmid"]
            r.setdefault("id_providers", {})["pmid"] = "openalex"
        filled += before != (bool(r["authors"]), bool(r["year"]), bool(r["pmid"]))
    return filled


def tie(own: List[dict], listed: List[dict]) -> Dict[str, dict]:
    """The source's entry id -> the Crossref entry it is.

    By DOI, then the publisher's own key (Crossref often keeps the XML's `B12`), then first
    author and year when only one entry has them, then the same place in a list of about
    the same length with the same initial.
    """
    by_doi = {r["doi"]: r for r in listed if r["doi"]}
    by_key = {r["key"]: r for r in listed if r.get("key")}
    out: Dict[str, dict] = {}
    for i, r in enumerate(own):
        doi = _doi(r.get("doi"))
        if doi and doi in by_doi:
            out[r["id"]] = by_doi[doi]
            continue
        if r["id"] in by_key:
            out[r["id"]] = by_key[r["id"]]
            continue
        first = _fold((r.get("authors") or [""])[0])
        if first and r.get("year"):
            hits = [c for c in listed if c["year"] == r["year"] and c["authors"]
                    and (first in _fold(c["authors"][0]) or _fold(c["authors"][0]) in first)]
            if len(hits) == 1:
                out[r["id"]] = hits[0]
                continue
        if abs(len(own) - len(listed)) <= 2 and i < len(listed):
            c = listed[i]
            if first and c["authors"] and _fold(c["authors"][0])[:1] == first[:1]:
                out[r["id"]] = c
    return out


def fill_identifiers(own: List[dict], listed: List[dict]) -> int:
    """Copy DOIs and PMIDs from Crossref's entries onto the source's own; returns how many entries gained one.

    Each copied id records where it came from in `id_providers`.
    """
    by_id = {r["id"]: r for r in own}
    gained = 0
    for rid, c in tie(own, listed).items():
        r = by_id[rid]
        changed = False
        for key in ("doi", "pmid"):
            if not r.get(key) and c.get(key):
                r[key] = c[key]
                r.setdefault("id_providers", {})[key] = c.get("id_providers", {}).get(key, "crossref")
                changed = True
        gained += changed
    return gained
