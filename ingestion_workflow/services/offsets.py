"""Text edits as offset maps, so the spans stored against a text move with it.

An edit replaces old[a:b] with new[c:d] (a deletion when c == d, an insertion when
a == b). Between edits the two texts agree, shifted. A span is remapped by moving
its ends: an end strictly inside replaced text has no place in the new text, and
the span is lost (None) rather than shifted onto other characters.
"""

from __future__ import annotations

import hashlib
import re
from bisect import bisect_left
from difflib import SequenceMatcher
from typing import Callable, Iterable, List, Optional, Sequence, Tuple, Union

Edit = Tuple[int, int, int, int]  # old start, old end, new start, new end
Span = Tuple[int, int]

#: Longest run either side of a changed block that is diffed character by character;
#: a longer one is kept as one replacement, and spans inside it are lost.
CHAR_DIFF = 20000


def sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


class OffsetMap:
    """Where each position of an old text is in a new one."""

    def __init__(self, edits: Iterable[Edit] = ()) -> None:
        merged: List[List[int]] = []
        for a, b, c, d in sorted(edits):
            if a == b and c == d:
                continue
            if merged and merged[-1][1] >= a:  # touching edits are one: no position between them
                merged[-1][1], merged[-1][3] = b, d
            else:
                merged.append([a, b, c, d])
        self.edits: Tuple[Edit, ...] = tuple(tuple(e) for e in merged)
        self._starts = [e[0] for e in self.edits]

    def __bool__(self) -> bool:
        return bool(self.edits)

    def __eq__(self, other) -> bool:
        return isinstance(other, OffsetMap) and self.edits == other.edits

    def __repr__(self) -> str:  # pragma: no cover - display only
        return f"OffsetMap({list(self.edits)!r})"

    def inverse(self) -> "OffsetMap":
        return OffsetMap((c, d, a, b) for a, b, c, d in self.edits)

    def position(self, p: int, end: bool = False) -> Optional[int]:
        """Where old position `p` is in the new text; None strictly inside replaced text.

        Text inserted at `p` falls after a span's start there and before its end.
        """
        i = bisect_left(self._starts, p)
        if i < len(self.edits) and self.edits[i][0] == p:
            a, b, c, d = self.edits[i]
            return c if end or a < b else d
        if i == 0:
            return p
        a, b, c, d = self.edits[i - 1]
        if p < b:
            return None
        return p - b + d

    def span(self, start: int, end: int) -> Optional[Span]:
        """The span in the new text, or None when either end is lost."""
        if start == end:
            p = self.position(start, end=True)
            return None if p is None else (p, p)
        a, b = self.position(start), self.position(end, end=True)
        return None if a is None or b is None else (a, b)

    def touches(self, start: int, end: int) -> bool:
        """Whether an edit changes any character of old[start:end], or inserts inside it."""
        i = bisect_left(self._starts, end)
        return any((a < end and start < b) or (a == b and start < a < end)
                   for a, b, _, _ in self.edits[max(0, i - 1):i])


class Chain:
    """Several maps applied in order, as a pipeline of text transforms produces them."""

    def __init__(self, maps: Sequence[OffsetMap] = ()) -> None:
        self.maps = list(maps)

    def position(self, p: int, end: bool = False) -> Optional[int]:
        for m in self.maps:
            if p is None:
                return None
            p = m.position(p, end)
        return p

    def span(self, start: int, end: int) -> Optional[Span]:
        for m in self.maps:
            got = m.span(start, end)
            if got is None:
                return None
            start, end = got
        return start, end

    def inverse(self) -> "Chain":
        return Chain([m.inverse() for m in reversed(self.maps)])


def _refine(old: str, new: str, a: int, c: int) -> List[Edit]:
    """The edits turning old into new, character by character, at offsets a and c."""
    if not old or not new or len(old) > CHAR_DIFF or len(new) > CHAR_DIFF:
        return [(a, a + len(old), c, c + len(new))]
    return [(a + i1, a + i2, c + j1, c + j2)
            for tag, i1, i2, j1, j2 in SequenceMatcher(None, old, new, autojunk=False).get_opcodes()
            if tag != "equal"]


def sub(pattern: Union[str, "re.Pattern"], repl: Union[str, Callable], text: str,
        flags: int = 0) -> Tuple[str, OffsetMap]:
    """`re.sub`, and the map from `text` to its result."""
    rx = re.compile(pattern, flags) if isinstance(pattern, str) else pattern
    out, edits, at, shift = [], [], 0, 0
    for m in rx.finditer(text):
        new = repl(m) if callable(repl) else m.expand(repl)
        out.append(text[at:m.start()])
        out.append(new)
        old = m.group()
        if new != old:
            edits += _refine(old, new, m.start(), m.start() + shift)
        shift += len(new) - len(old)
        at = m.end()
    out.append(text[at:])
    return "".join(out), OffsetMap(edits)


def diff(old: str, new: str) -> OffsetMap:
    """The map from `old` to `new`, read from their differences: line by line, then
    character by character inside each changed block."""
    if old == new:
        return OffsetMap()
    a_lines, b_lines = old.splitlines(keepends=True), new.splitlines(keepends=True)
    a_at, b_at = _starts(a_lines), _starts(b_lines)
    edits: List[Edit] = []
    for tag, i1, i2, j1, j2 in SequenceMatcher(None, a_lines, b_lines, autojunk=False).get_opcodes():
        if tag != "equal":
            edits += _refine(old[a_at[i1]:a_at[i2]], new[b_at[j1]:b_at[j2]], a_at[i1], b_at[j1])
    return OffsetMap(edits)


def _starts(lines: List[str]) -> List[int]:
    at = [0]
    for line in lines:
        at.append(at[-1] + len(line))
    return at


__all__ = ["Chain", "OffsetMap", "diff", "sha256", "sub"]
