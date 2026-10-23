"""A publisher's page ACE holds: the corresponding authors it marks, with their emails.

Each module reads one publisher's layout, the shape its pages were seen in on beast on
2026-10-23. The readers that pair a name and an address by the page's structure come
first; the `citation_author_email` tags, then a note read as text, after them. The first
reader to give an address speaks for the page; without one, the first to name anyone.
A `mailto:` anywhere else -- a journal's office, a lead contact -- is not read.
"""

from __future__ import annotations

from typing import List

from lxml import etree
from lxml import html as lxml_html
from study_schema.models.paper_parse import MetadataSource

from ingestion_workflow.models.metadata import CorrespondingAuthor

from ..common import Found, authors, squash
from . import atypon, meta, notes, oup, sciencedirect, springer_nature, tandf

_UTF8_HTML = lxml_html.HTMLParser(encoding="utf-8")

READERS = (
    sciencedirect.from_json,
    sciencedirect.from_list,
    springer_nature.from_correspondence_list,
    springer_nature.from_author_items,
    springer_nature.from_contacts,
    atypon.from_properties,
    atypon.from_accordion,
    oup.from_cards,
    tandf.from_entries,
    meta.from_meta,
    notes.from_notes,
)


def from_page(page: bytes) -> List[CorrespondingAuthor]:
    try:
        root = lxml_html.document_fromstring(page, parser=_UTF8_HTML)
    except etree.ParserError:  # an empty page
        return []
    listed = _listed(root)
    named: List[Found] = []
    for read in READERS:
        found = read(root)
        if any(email for _, email in found):
            return authors(found, MetadataSource.publisher_page, listed)
        named = named or found
    return authors(named, MetadataSource.publisher_page, listed)


#: Where a page lists its authors, for an address that spells one of them (`authors`).
_AUTHOR_NAMES = (
    "//meta[@name='citation_author' or @name='dc.creator' or @name='DC.creator']/@content",
    "//div[contains(concat(' ', @class, ' '), ' entryAuthor ')]/a[contains(@class, 'author')][1]",
    "//*[contains(@class, 'info-card-name')]",
)


def _listed(root) -> List[str]:
    names = []
    for path in _AUTHOR_NAMES:
        for found in root.xpath(path):
            text = squash(found if isinstance(found, str) else found.xpath("string(./text())"))
            if text:
                names.append(" ".join(reversed(text.split(", ", 1))))
    names += [squash(" ".join(squash(" ".join(person.xpath(f".//*[@property='{p}']//text()")))
                              for p in ("givenName", "familyName")))
              for person in root.xpath("//*[@property='author']")]
    return list(dict.fromkeys(n for n in names if n))
