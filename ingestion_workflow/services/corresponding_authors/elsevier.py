"""Elsevier full text: the authors a held copy cross-refers to its correspondence note.

The Elsevier API leaves the emails out (no `ce:e-address` in 200 copies read on
2026-10-22, nor in Scopus's `correspondence` for 67 of them), so these are names alone;
the ScienceDirect page ACE holds prints the emails (`ace.sciencedirect`).
"""

from __future__ import annotations

from pathlib import Path
from typing import List

from lxml import etree
from study_schema.models.paper_parse import CorrespondingAuthor, MetadataSource

from .common import authors, squash

_CE = "http://www.elsevier.com/xml/common/dtd"
_XML = etree.XMLParser(recover=True, huge_tree=True, resolve_entities=False)


def _ce(tag: str) -> str:
    return f"{{{_CE}}}{tag}"


def from_elsevier(content_xml: Path) -> List[CorrespondingAuthor]:
    """The authors an Elsevier full-text record cross-refers to a `ce:correspondence`."""
    root = etree.parse(str(content_xml), _XML).getroot()
    corresp = {el.get("id") for el in root.iter(_ce("correspondence"))}
    found = []
    for author in root.iter(_ce("author")):
        refs = {r for x in author.iter(_ce("cross-ref")) for r in (x.get("refid") or "").split()}
        if refs & corresp:
            parts = [author.findtext(_ce("given-name")), author.findtext(_ce("surname"))]
            found.append((squash(" ".join(squash(p) for p in parts if p)), None))
    return authors(found, MetadataSource.elsevier)
