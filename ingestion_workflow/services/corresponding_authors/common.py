"""What every extractor shares: finding emails and names in text, and the record it makes."""

from __future__ import annotations

import re
import unicodedata
from typing import Iterable, List, Optional, Sequence, Tuple

from study_schema.models.paper_parse import CorrespondingAuthor, MetadataSource

#: (name, email): one corresponding author as an extractor reads them, either part absent.
Found = Tuple[Optional[str], Optional[str]]

_EMAIL = re.compile(r"[\w.+'-]+@[\w-]+(?:\.[\w-]+)+")
#: Addresses a page or a journal prints for itself: permissions, support, an editorial
#: office, PsycNET's data@apa.org. An author's address never starts with one of these.
_NOT_AN_AUTHOR = re.compile(
    r"^(?:permissions?|support|help|info|editor\w*|journals?|service\w*|subscri\w*|feedback"
    r"|webmaster|customer\w*|contact|reprints?|advert\w*|data|psycinfo|noreply|no-reply"
    r"|office|admin\w*|press|media|enquir\w*|inquir\w*|submissions?|orders?)@",
    re.I,
)


def squash(text: Optional[str]) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def emails(text: Optional[str]) -> List[str]:
    """The addresses in `text`, in order and once each, without a page's own."""
    found = (e.rstrip(".") for e in _EMAIL.findall(text or ""))
    return [e for e in dict.fromkeys(found) if not _NOT_AN_AUTHOR.match(e)]


def fold(text: str) -> str:
    return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode().lower()


def surname(name: str) -> str:
    words = re.findall(r"[a-z]+", fold(name))
    return words[-1] if words else ""


def authors(found: Iterable[Found], by: MetadataSource,
            listed: Sequence[str] = ()) -> List[CorrespondingAuthor]:
    """The records for what an extractor read, each name and address once.

    A name that runs on past a `listed` author's is cut to theirs. An address whose
    local part spells one other author's surname, of those `listed` or
    read, is that author's: Taylor & Francis can print Hanlon's address in another
    author's entry. An address read beside several names stays with the one it spells,
    else the first, and the others keep their names alone.
    """
    found = list(dict.fromkeys((_as_listed(squash(name), listed) or None, email)
                               for name, email in found))
    names = list(dict.fromkeys([*listed, *(name for name, _ in found if name)]))
    settled, holder = [], {}
    for name, email in found:
        if email:
            spelt = _spelt_by(email, names)
            if spelt and (not name or surname(spelt) != surname(name)):
                if (spelt, email) in found:  # theirs already: this author shares it
                    email = None
                else:
                    name = spelt
        settled.append((name, email))
    for name, email in settled:
        if email and email not in holder:
            holder[email] = next((n for n, e in settled if e == email and n
                                  and surname(n) in fold(email.split("@")[0])), name)
    out = [(name, email if email and holder[email] == name else None)
           for name, email in settled]
    return [CorrespondingAuthor(name=name, email=email, stated_by=by)
            for name, email in dict.fromkeys(out) if name or email]


def _as_listed(name: str, listed: Sequence[str]) -> str:
    """A name read on into what follows it ("Liron Rabany Olin Neuropsychiatry"), cut to
    the listed author it begins with."""
    starts = [n for n in listed if n and fold(name).startswith(fold(n) + " ")]
    return max(starts, key=len) if starts else name


def _spelt_by(email: str, names: Sequence[str]) -> Optional[str]:
    local = fold(email.split("@")[0])
    spelt = [n for n in names if len(surname(n)) > 2 and surname(n) in local]
    return spelt[0] if len({surname(n) for n in spelt}) == 1 else None
