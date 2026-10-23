"""Who a paper names to correspond with, and their email, from every source that says.

One extractor per source: a held copy's JATS (`jats`), Elsevier full text (`elsevier`),
PDF text (`pdf`) or publisher's page (`ace`, one reader per layout); PubMed's record
(`pubmed`); OpenAlex's (`openalex`). A held copy is read from what its download and
extraction already hold: nothing is downloaded for this. The first source to pair one
with an email speaks for the paper; without one, the first to name any (`decide`).
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Optional, Sequence

from lxml import etree
from study_schema.models.paper_parse import CorrespondingAuthor

from .ace import from_page
from .common import fold, surname
from .elsevier import from_elsevier
from .jats import from_jats
from .openalex import from_openalex
from .pdf import from_pdf_text
from .pubmed import from_pubmed

__all__ = ["decide", "from_held", "from_jats", "from_elsevier", "from_page",
           "from_pdf_text", "from_pubmed", "from_openalex"]

#: Sources whose copy is a PDF, read from the text its extraction gave.
PDF_SOURCES = ("pdf", "wiley")
_XML = etree.XMLParser(recover=True, huge_tree=True, resolve_entities=False)


def from_held(source: str, files: Iterable[Path],
              text: Optional[Path] = None) -> List[CorrespondingAuthor]:
    """What a held copy marks: the page an ACE download holds, a PDF's extracted `text`,
    its JATS `article.xml`, or its Elsevier `content.*.xml`. Empty for any other copy."""
    files = list(files)
    if source == "ace":
        page = next((p for p in files if p.suffix == ".html" and p.exists()), None)
        return from_page(page.read_bytes()) if page is not None else []
    if source in PDF_SOURCES:
        return from_pdf_text(text) if text is not None and text.exists() else []
    by_name = {path.name: path for path in files}
    if "article.xml" in by_name and by_name["article.xml"].exists():
        return from_jats(etree.parse(str(by_name["article.xml"]), _XML).getroot())
    content = next((p for name, p in by_name.items()
                    if name.startswith("content.") and name.endswith(".xml")), None)
    if content is not None and content.exists():
        return from_elsevier(content)
    return []


def decide(stated: Sequence[List[CorrespondingAuthor]],
           authors: Sequence[str]) -> List[CorrespondingAuthor]:
    """The paper's corresponding authors from each source's list, best source first: the
    first that pairs one with an email, else the first that names any. Each name is
    spelt as `authors` spells the one author whose surname and first initial it shares."""
    named = [found for found in stated if found]
    chosen = next((found for found in named if any(a.email for a in found)),
                  named[0] if named else [])
    return [a.model_copy(update={"name": _as_listed(a.name, authors)}) if a.name else a
            for a in chosen]


def _as_listed(name: str, authors: Sequence[str]) -> str:
    key = (surname(name), fold(name)[:1])
    same = [listed for listed in authors if (surname(listed), fold(listed)[:1]) == key]
    return same[0] if len(same) == 1 else name
