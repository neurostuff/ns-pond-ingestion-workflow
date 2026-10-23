"""ScienceDirect, in either of its layouts.

The current one carries the article's `ce:` markup as JSON, an author's `e-address`
beside their `cross-ref` to the correspondence; it holds the address for 11 of 80
pages read on 2026-10-23, and the names for all of them. The older one lists each
author as an `li` holding their name, a "Corresponding author" link and a `mailto:`
labelled "E-mail the corresponding author" -- which a second corresponding author may
have alone.
"""

from __future__ import annotations

import json
from typing import List

from ..common import Found, emails, squash


def from_json(root) -> List[Found]:
    nodes = []
    for script in root.xpath("//script[@type='application/json']"):
        try:
            _walk(json.loads(script.text_content()), nodes)
        except ValueError:
            continue
    corresp = {(n.get("$") or {}).get("id") for n in nodes if n["#name"] == "correspondence"}
    found = []
    for author in (n for n in nodes if n["#name"] == "author"):
        children = [c for c in author.get("$$") or [] if isinstance(c, dict)]
        parts = {c.get("#name"): c for c in children}
        refs = {(c.get("$") or {}).get("refid") for c in children if c.get("#name") == "cross-ref"}
        if not refs & corresp:
            continue
        name = " ".join(squash(parts[k].get("_")) for k in ("given-name", "surname") if k in parts)
        address = emails((parts.get("e-address") or {}).get("_"))
        found.append((name, address[0] if address else None))
    return found


def _walk(value, nodes: list) -> None:
    if isinstance(value, dict):
        if isinstance(value.get("#name"), str):
            nodes.append(value)
        for child in value.values():
            _walk(child, nodes)
    elif isinstance(value, list):
        for child in value:
            _walk(child, nodes)


def from_list(root) -> List[Found]:
    found = []
    for item in root.xpath("//ul[contains(concat(' ', @class, ' '), ' authorGroup ')]/li"):
        if not item.xpath(".//a[contains(@title, 'Corresponding author')"
                          " or contains(concat(' ', @class, ' '), ' auth_corr ')"
                          " or contains(@title, 'corresponding author')"
                          " or .//img[contains(@alt, 'corresponding author')]]"):
            continue
        name = squash(" ".join(item.xpath(".//a[contains(@class, 'authorName')]//text()")))
        address = [e for href in item.xpath(".//a[starts-with(@href, 'mailto:')]/@href")
                   for e in emails(href[len("mailto:"):])]
        found.append((name, address[0] if address else None))
    return found
