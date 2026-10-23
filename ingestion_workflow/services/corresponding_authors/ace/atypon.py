"""Atypon-hosted journals (Wiley, SAGE, PNAS, the American Physiological Society), in two
layouts.

The current one marks each author up as `span[property=author]` with their `givenName`,
`familyName` and, for a corresponding author, `a[property=email]`. The older one opens
an accordion per author, `div.author-info`, whose `p.author-type` says "Corresponding
Author" and whose `p.author-name` is the name; their own address is the `mailto:` in its
account list, while the correspondence note it may repeat holds every corresponding
author's.
"""

from __future__ import annotations

from typing import List

from ..common import Found, emails, squash, surname


def from_properties(root) -> List[Found]:
    found = []
    for person in root.xpath("//*[@property='author'][.//a[@property='email']]"):
        name = " ".join(squash(" ".join(person.xpath(f".//*[@property='{p}']//text()")))
                        for p in ("givenName", "familyName"))
        link = person.xpath(".//a[@property='email']")[0]
        found += [(name, email) for email in emails(link.get("href", "")[len("mailto:"):])[:1]]
    return found


def from_accordion(root) -> List[Found]:
    found = []
    for info in root.xpath("//div[contains(concat(' ', @class, ' '), ' author-info ')]"):
        kind = squash(" ".join(info.xpath(".//*[contains(@class, 'author-type')]//text()")))
        if "corresponding" not in kind.lower():
            continue
        name = squash(" ".join(info.xpath(".//*[contains(@class, 'author-name')][1]//text()")))
        own = [e for href in info.xpath(".//a[contains(@class, 'sm-account__link')]"
                                        "[starts-with(@href, 'mailto:')]/@href")
               for e in emails(href[len("mailto:"):])]
        if not own:  # their address in the note, after their name
            last = surname(name)
            for p in info.xpath(".//p[.//a[starts-with(@href, 'mailto:')]]"):
                before = p.getprevious()
                said = squash(before.text_content() if before is not None else "")
                if last and last in said.lower():
                    own += emails(p.text_content())
        found.append((name, own[0] if own else None))
    return found
