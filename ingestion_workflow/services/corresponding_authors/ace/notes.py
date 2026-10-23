"""A correspondence note a page prints as text -- J Neurosci's `li.corresp`, Cambridge's
`div.corresp`, an Atypon `core-authors-notes`, an LWW author block -- read as a PDF's is
(`..notes`). Each smallest element whose text holds a cue and an address is one note,
whose address is the `mailto:` link's where the text runs on past it."""

from __future__ import annotations

from typing import List

from ..common import Found, emails
from ..notes import _CUE, from_text

_BLOCKS = "//p | //li | //div | //section | //span | //td"
_BREAKS = ("br", "p", "div", "li", "td", "tr", "section", "h1", "h2", "h3", "h4", "h5", "h6")


def from_notes(root) -> List[Found]:
    # A line or block break is a space ("Dong Soo Lee<br/>Department"); inline markup is
    # not, or an address set in spans would be split.
    for el in root.iter(*_BREAKS):
        el.tail = " " + (el.tail or "")
    # So is the edge between siblings of one kind with nothing between them: Mary Ann
    # Liebert sets each line of a note in its own <i>.
    for el in root.iter():
        after = el.getnext()
        if (after is not None and after.tag == el.tag and el.tag != "wbr"
                and not (el.tail or "").strip()):
            el.tail = " " + (el.tail or "")
    found = []
    for block in root.xpath(_BLOCKS):
        text = block.text_content()
        if "@" not in text or not _CUE.search(text) or len(text) > 3000:
            continue
        if any("@" in c.text_content() and _CUE.search(c.text_content())
               for c in block.iterchildren() if isinstance(c.tag, str)):
            continue  # a smaller block holds the note
        # The address a link gives, where the text runs it into what follows.
        linked = [e for href in block.xpath(".//a[starts-with(@href, 'mailto:')]/@href")
                  for e in emails(href[len("mailto:"):])]
        found += [(name, next((e for e in linked if email and email.startswith(e)), email))
                  for name, email in from_text(text)]
    return found
