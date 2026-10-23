"""Springer and Nature, in their three layouts.

The current one names the corresponding authors in "Correspondence to <a
href="mailto:...">Name</a>" (`#corresponding-author-list`), the link's text the name. An
older Springer page lists each author as an `li` with `authors__name` and an "Email
author" link; the oldest gives a `Contact` block with its `AuthorName` and address.
"""

from __future__ import annotations

from typing import List

from ..common import Found, emails, squash


def _address(link) -> List[str]:
    return emails(link.get("href", "")[len("mailto:"):])


def from_correspondence_list(root) -> List[Found]:
    return [(squash(link.text_content()), email)
            for link in root.xpath("//*[@id='corresponding-author-list']"
                                   "//a[starts-with(@href, 'mailto:')]")
            for email in _address(link)[:1]]


def from_author_items(root) -> List[Found]:
    found = []
    for item in root.xpath("//li[.//*[contains(@class, 'authors__name')]]"):
        links = item.xpath(".//a[starts-with(@href, 'mailto:')]")
        if links:
            name = squash(" ".join(item.xpath(".//*[contains(@class, 'authors__name')]//text()")))
            found += [(name, email) for email in _address(links[0])[:1]]
    return found


def from_contacts(root) -> List[Found]:
    found = []
    for contact in root.xpath("//div[contains(concat(' ', @class, ' '), ' Contact ')]"):
        name = squash(" ".join(contact.xpath(".//*[contains(@class, 'AuthorName')]//text()")))
        links = contact.xpath(".//a[starts-with(@href, 'mailto:')]")
        if links:
            found += [(name, email) for email in _address(links[0])[:1]]
    return found
