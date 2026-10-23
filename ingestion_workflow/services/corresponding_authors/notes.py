"""Correspondence notes in running text: a PDF's front matter, or a note a page prints.

A note opens with a cue -- "*Correspondence to:", "Correspondence should be addressed
to", "Address for correspondence:", "Reprint requests should be sent to", "Authors for
correspondence:" -- and its emails follow within a few lines. Each email belongs to the
name that opens the stretch of text since the cue or the email before it:

    Correspondence to: Dr. Gui Xue, National Key Laboratory ... E-mail: xue.g@mail.example
    ... addressed to Vanessa Scholz at scholz.v@ukw.example or Lorenz Deserno at deserno.l@...
    Authors for correspondence: Yuping Ning, E-mail: a@...; Xiaomei Zhong, E-mail: b@...
    Correspondence: M. Peciña (pecina.m@upmc.example)

Names that open one stretch together ("Feliberto de la Cruz and Karl-Jürgen Bär, ...
Email: a and b") are paired with its emails in order when they are as many. Elsevier's
"E-mail address: x (A. Name)" names the address after it. An email whose name cannot be
read this way is kept without one, never given a guess.

Springer's PDFs print no cue. Its current layout stars the author above their address,
"* Christine Lochner xq1002@sun.example"; its older one marks them "(*)" in the author list,
"R. R. Wetherill (*) : A. R. Childress ... e-mail: xq1008@...", with the address after
their affiliation. Each shape has its reader below, and all of them are read.
"""

from __future__ import annotations

import re
from typing import List, Optional

from .common import Found, emails, squash

_CUE = re.compile(
    r"(?:to\s+whom\s+(?:all\s+)?correspondence\s+(?:should|may)\s+be\s+(?:addressed|sent)"
    r"|(?:send|address(?:es)?)\s+(?:for\s+)?correspondence(?:\s+(?:to|at))?"
    r"|authors?\s+for\s+correspondence"
    r"|corresponding\s+authors?(?:\s+(?:at|information|contact|address))?"
    r"|reprint\s+requests?\s+(?:should\s+be\s+(?:sent|addressed)\s+)?to"
    r"|correspondence(?:\s+(?:and\s+(?:reprint\s+)?requests(?:\s+for\s+\w+)?\s+)?(?:should\s+be\s+)?"
    r"(?:addressed|sent|directed)?\s*to"
    r"|\s+concerning\s+this\s+article\s+should\s+be\s+addressed\s+to)?)"
    r"[\s:.*⁎]*",
    re.I,
)
#: How far an email may sit from the cue, or from the email before it.
_REACH = 400
_TITLES = re.compile(r"^(?:(?:Prof(?:essor)?|Dr|Mr|Mrs|Ms|Assoc\.?\s+Prof)\.?\s+)+")
_PARTICLES = {"van", "de", "la", "von", "der", "den", "di", "da", "du", "le", "del", "dos",
              "das", "y", "bin", "al", "el", "ter", "ten", "zu"}
_NOT_A_NAME = re.compile(
    r"\b(?:department|university|universit|institute|laborator|school|cent(?:er|re)|hospital"
    r"|clinic|faculty|college|division|e-?mail|tel|fax|phone|box|street|road|avenue|address"
    r"|above|below|authors?|correspondence|requests?|reprints?|unit|program|group|research"
    r"|klinik\w*|institut\w*|neurolog\w*|psychiatr\w*|psycholog\w*|medic\w*|scien\w*"
    r"|foundation|trust|council|ministry|academy|labs?|service|health|national|state)\b",
    re.I,
)
#: A diacritic a PDF's text set apart from its letter: "Mu¨nte", "Ru¨ schemeyer".
_SPLIT_DIACRITIC = re.compile(r"(?<=[A-Za-z])[¨´`ˆ˜]\s?(?=[a-z])")


def from_text(text: str) -> List[Found]:
    """Every (name, email) the correspondence notes in `text` give, each address once."""
    # A PDF's text can break an address over a line: "c.gerloff@ uni-tuebingen.example", "c. hyatt@".
    text = re.sub(r"\b([a-z][\w-]*\.)\s+(?=[\w.+'-]+@)", r"\1", re.sub(r"@\s+", "@", text))
    found: List[Found] = []
    taken = set()
    for read in (_cued, _starred, _marked):
        for name, email in read(text):
            if email not in taken:
                taken.add(email)
                found.append((name, email))
    return found


def _cued(text: str) -> List[Found]:
    return [pair for cue in _CUE.finditer(text) for pair in _note(text, cue.end())]


_PERSON = r"((?:[A-Z][\w'.-]*\s+){1,4}[A-Z][\w'-]+)"
_ADDRESS = r"([\w.+'-]+@[\w-]+(?:\.[\w-]+)+)"


def _starred(text: str) -> List[Found]:
    """Springer's "* Christine Lochner xq1002@sun.example"."""
    return [(_name(m.group(1)), email)
            for m in re.finditer(rf"(?:^|\s)[*⁎]\s*{_PERSON}\s+{_ADDRESS}", text)
            for email in emails(m.group(2))]


def _marked(text: str) -> List[Found]:
    """Springer's "R. R. Wetherill (*) : ... e-mail: xq1008@...": the address that follows."""
    found = []
    for m in re.finditer(rf"{_PERSON}\s*\(\*\)", text):
        after = re.search(rf"e-?mail:?\s*{_ADDRESS}", text[m.end():m.end() + _REACH], re.I)
        if after:
            found += [(_name(m.group(1)), email) for email in emails(after.group(1))]
    return found


def _note(text: str, start: int) -> List[Found]:
    reached, last = [], start
    for match in re.finditer(r"[\w.+'-]+@[\w-]+(?:\.[\w-]+)+", text[start:start + 4 * _REACH]):
        if start + match.start() - last > _REACH:
            break
        reached.append(match)
        last = start + match.end()
    pairs, names, last = [], [], start
    for i, match in enumerate(reached):
        at, end = start + match.start(), start + match.end()
        found = emails(match.group())
        segment, last = text[last:at], end
        if not found:
            continue
        email = found[0]
        after = _named_after(text[end:end + 80])
        opening = _names(segment, len(reached) - i)
        if after:
            pairs.append((after, email))
        elif len(opening) == 1:
            pairs.append((opening[0], email))
        elif pairs and not names and not re.sub(r"[\W_]|\b(?:and|or)\b", "", segment):
            pairs.append((pairs[-1][0], email))  # a second address: "E-mails: a; b"
        else:
            names += opening
            pairs.append((None, email))
    unnamed = [i for i, (name, _) in enumerate(pairs) if name is None]
    if names and len(names) == len(unnamed):
        for i, name in zip(unnamed, names):
            pairs[i] = (name, pairs[i][1])
    return pairs


def _named_after(text: str) -> Optional[str]:
    """The name in brackets right after an email, as "x@y.org (A. Name)" prints it."""
    match = re.match(r"\s*\(([^()@]{3,60})\)", text)
    return _name(match.group(1)) if match else None


def _names(segment: str, ahead: int = 1) -> List[str]:
    """The names a stretch of a note opens with: one, or several joined by and/or, or a
    comma-separated list of as many as the `ahead` emails left to pair."""
    head = _SPLIT_DIACRITIC.sub("", squash(segment))
    head = re.sub(r"^[\s,;:.*⁎†‡§)\]]*(?:(?:and|or)\s+)?", "", head)
    head = _TITLES.sub("", head)
    # "Corresponding authors: Yi-Zhuang Zou, Shu-Ping Tan. E-mail: ...": a list of names
    # is commas between parts that are each a whole name; "Lee, PhD, Department" is not.
    listed = re.split(r"\s*(?:[;(]|\bat\b|\be-?mails?\b|(?<=[a-z]{2})\.(?=\s+(?![A-Z]\.)))", head,
                      maxsplit=1, flags=re.I)[0]
    leading = []
    for part in re.split(r"\s*,\s*|\s+(?:and|or|&)\s+", listed):
        if not _name(part):
            break
        leading.append(_name(part))
    if 1 < len(leading) <= ahead:
        return leading
    # A name ends at a comma, semicolon or bracket, at "at" or "E-mail", at a full stop
    # after a word ("Jesus Pujol. MRI Department", not "J. Billington"), or where an
    # institution begins ("Annalena Venneri Department of Neuroscience").
    head = re.split(r"\s*(?:[,;(]|\bat\b|\be-?mails?\b|(?<=[a-z]{2})\.(?=\s+(?![A-Z]\.)))", head,
                    maxsplit=1, flags=re.I)[0]
    cut = _NOT_A_NAME.search(head)
    head = head[:cut.start()] if cut else head
    parts = [p for p in re.split(r"\s+(?:and|or|&)\s+", head) if p.strip()]
    names = [_name(p) for p in parts]
    return names if names and all(names) else []


def _name(text: str) -> Optional[str]:
    """`text` as a person's name, or None when it is not one."""
    text = _TITLES.sub("", squash(text)).strip(" .,;:")
    words = text.split()
    if not 1 < len(words) <= 6 or _NOT_A_NAME.search(text) or re.search(r"\d|@", text):
        return None
    for word in words:
        if word.lower() in _PARTICLES:
            continue
        if not (word[0].isupper() or re.match(r"^[A-Z]\.", word)):
            return None
    return text
