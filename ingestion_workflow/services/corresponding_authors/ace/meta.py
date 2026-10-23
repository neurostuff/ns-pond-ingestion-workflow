"""Highwire's `citation_author_email` tags, each the address of the `citation_author`
before it, as Springer's pages print them. A name written "Surname, Given" is turned."""

from __future__ import annotations

from typing import List

from ..common import Found, emails, squash


def from_meta(root) -> List[Found]:
    found, name = [], None
    for meta in root.xpath("//meta[@name='citation_author' or @name='citation_author_email']"):
        if meta.get("name") == "citation_author":
            surname, comma, given = squash(meta.get("content")).partition(",")
            name = squash(f"{given} {surname}") if comma else surname
            continue
        found += [(name, email) for email in emails(meta.get("content"))[:1]]
    return found
