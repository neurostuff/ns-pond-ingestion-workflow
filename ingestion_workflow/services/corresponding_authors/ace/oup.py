"""Oxford Academic: each author's card, `div.info-card-author`, names them in
`info-card-name` and, for a corresponding author, prints the correspondence note in
`info-author-correspondence`, whose address is theirs."""

from __future__ import annotations

from typing import List

from ..common import Found, emails, fold, squash, surname
from ..notes import from_text


def from_cards(root) -> List[Found]:
    found = []
    for card in root.xpath("//div[contains(concat(' ', @class, ' '), ' info-card-author ')]"):
        note = card.xpath(".//div[contains(@class, 'info-author-correspondence')]")
        if not note:
            continue
        names = card.xpath(".//div[contains(@class, 'info-card-name')]")
        name = squash(names[0].xpath("string(./text())")) if names else None
        found.append((name or None, _theirs(name, note[0])))
    return found


def _theirs(name, note):
    """The address in a card's note that is the card's author's: the only one, the one
    spelling their surname, or the one the note pairs with their name. A note can be
    shared: "Foxe or Molholm, ... foxe.j@... or molholm.s@..." sits in both cards."""
    found = emails(note.text_content())
    if len(found) <= 1:
        return found[0] if found else None
    last = surname(name or "")
    spelt = [e for e in found if last and last in fold(e.split("@")[0])]
    paired = [e for n, e in from_text(note.text_content()) if n and surname(n) == last]
    return (spelt or paired or [None])[0]
