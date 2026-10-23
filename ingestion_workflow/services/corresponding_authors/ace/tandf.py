"""Taylor & Francis: each `div.entryAuthor` names the author in `a.author` and gives a
corresponding author's address in its `corr-email`."""

from __future__ import annotations

from typing import List

from ..common import Found, emails, squash


def from_entries(root) -> List[Found]:
    found = []
    for entry in root.xpath("//div[contains(concat(' ', @class, ' '), ' entryAuthor ')]"):
        links = entry.xpath(".//*[contains(@class, 'corr-email')]"
                            "//a[starts-with(@href, 'mailto:')]")
        if links:
            name = squash(entry.xpath("string(.//a[contains(@class, 'author')][1]/text())"))
            found += [(name, e) for e in emails(links[0].get("href", "")[len("mailto:"):])[:1]]
    return found
