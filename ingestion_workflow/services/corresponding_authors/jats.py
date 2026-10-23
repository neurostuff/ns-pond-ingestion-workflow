"""JATS: the authors a held copy's front matter marks, with the emails it pairs them with."""

from __future__ import annotations

import re
from typing import List, Optional

from lxml import etree
from study_schema.models.paper_parse import CorrespondingAuthor, MetadataSource

from .common import authors, emails, fold, squash, surname


def from_jats(root: etree._Element) -> List[CorrespondingAuthor]:
    """The authors a JATS article's front matter marks as corresponding.

    A marked author is one with `@corresp="yes"` or an `xref` to a `<corresp>` note. Their
    email is their own `<email>`, else the one in that note that is theirs (`_from_note`).
    An email in a note that no marked author takes belongs to the one author whose surname
    the note prints before it, as Frontiers' "Correspondence: Nina Levar levar.n@..."
    does without marking anyone; else it is kept without a name.
    """
    meta = root.find(".//article-meta")
    if meta is None:
        return []
    notes = {note.get("id"): note for note in meta.iter("corresp")}
    contribs = [c for c in meta.iter("contrib") if c.get("contrib-type", "author") == "author"]
    rids = {id(c): [rid for x in c.iter("xref") if x.get("ref-type") == "corresp"
                    for rid in (x.get("rid") or "").split()] for c in contribs}
    marked = [c for c in contribs if c.get("corresp") == "yes" or rids[id(c)]]
    if len(notes) == 1:  # `@corresp` with no xref points at the only note there is
        rids = {key: found or list(notes) for key, found in rids.items()}
    sharing = {rid: [c for c in marked if rid in rids[id(c)]] for rid in notes}
    names = [n for n in map(_name, contribs) if n]
    found, taken = [], set()
    for contrib in marked:
        name = _name(contrib)
        others = {surname(n) for n in names if n != name} - {surname(name or "")}
        own = [e for el in contrib.iter("email") for e in emails("".join(el.itertext()))]
        email = own[0] if own else None
        for rid in [] if email else rids[id(contrib)] or list(notes):
            # A note the author does not point at is theirs only by the name it prints.
            shared = sharing.get(rid) or []
            email = _from_note(notes.get(rid), name, others, taken,
                               shared.index(contrib) if contrib in shared else None,
                               len(shared))
            if email:
                break
        if name or email:
            taken.add(email)
            found.append((name, email))
    for note in notes.values():
        text, start = " ".join(note.itertext()), 0
        for email in emails(text):
            at = text.find(email, start)
            before, start = fold(text[start:at]), at + len(email)
            if email in taken:
                continue
            taken.add(email)
            named = [n for n in names
                     if surname(n) and re.search(rf"\b{re.escape(surname(n))}\b", before)]
            found.append((named[0] if len(named) == 1 else None, email))
    return authors(found, MetadataSource.jats)


def _name(contrib) -> Optional[str]:
    name = contrib.find("name")
    if name is not None:
        parts = [name.findtext("given-names"), name.findtext("surname")]
        return squash(" ".join(squash(p) for p in parts if p)) or None
    for tag in ("string-name", "collab"):
        el = contrib.find(tag)
        if el is not None:
            return squash("".join(el.itertext())) or None
    return None


def _from_note(note, name: Optional[str], others: set, taken: set,
               position: Optional[int], sharers: int) -> Optional[str]:
    """The email in `note` that belongs to `name`, the `position`th of the `sharers` marked
    authors pointing at it (None: they do not point at it).

    Theirs is the note's only email, when they point at it; else the one whose address
    holds their surname; else the one the note follows their surname with, and no other
    author's (`others`) since the email before it -- "Correspondence to: Ding Ding; email:
    ...", not "Zhuo or Wang or Tu, e-mail: a or b or c"; else the one followed by their
    initials, as in "x@ncu.example (L.X.)"; else, when they point at it and it holds one
    email for each of them, the one in their place."""
    if note is None:
        return None
    text = " ".join(note.itertext())
    found = emails(text)
    if position is not None and len(found) == 1:
        return None if found[0] in taken else found[0]
    last = surname(name or "")
    initials = "".join(w[0] for w in re.findall(r"[a-z]+", fold(name or "")))
    if last:
        for email in found:
            if email not in taken and last in fold(email.split("@")[0]):
                return email
    start = 0
    for email in found:
        at = text.find(email, start)
        before, after, start = fold(text[start:at]), text[at + len(email):], at + len(email)
        if email in taken:
            continue
        words = set(re.findall(r"[a-z]+", before))
        if last in words and not words & others:
            return email
        tag = re.match(r"\s*\(([^)@]{1,12})\)", after)
        if initials and tag and re.sub(r"[^a-z]", "", fold(tag.group(1))) == initials:
            return email
    if position is not None and len(found) == sharers and found[position] not in taken:
        return found[position]
    return None
