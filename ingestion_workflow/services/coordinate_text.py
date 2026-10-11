"""Where a coordinate triple is printed in text.

One matcher for every producer of text positions: the parse places a prose
point at the characters printing it (`paper_parse`), and the set-role
classifier reads the sentence(s) around those characters (`set_roles.
prose_context`). Training rows and ingested papers therefore see the same
LOCAL text for the same passage.
"""

from __future__ import annotations

import re
from functools import lru_cache
from typing import Iterable, List, Optional, Sequence, Tuple

Span = Tuple[int, int]
Triple = Tuple[float, float, float]

#: Hyphen-minus, minus sign, en dash, hyphen, figure dash: every minus papers print.
MINUS = "-−–‐‒"

# A sign may be spaced from its value ("x −  24.2", "y + 16.3").
_GAP = r"[^\S\n]{0,2}"
# A positive value is not the tail of a longer number, nor of a negative one
# ("-42" or "− 42" is not 42: the mirror hemisphere).
_POSITIVE = rf"(?<![{MINUS}\d.])(?<![{MINUS}]\s)(?<![{MINUS}]\s\s)(?:\+{_GAP})?"
# A standard deviation printed after a value ("−54 ± 5").
_SPREAD = r"(?:\s*(?:±|\+/?[" + MINUS + r"])\s*\d+(?:\.\d+)?)?"
# Commas, spaces, axis labels and "and" between the values ("12, and z = 16",
# "[–55,  13 , – 4 ]"), a run of spaces counting as one character, but not a
# line break, which in the parsed text ends a table row; a comma may end a
# line, and so may "=" ("y = \n−64"). A minus may be glued to the value before
# it ("[0 46–4]").
_SEPARATOR = (
    r"(?:(?:[^\S\n]++|[^\d\s]){1,12}?"
    r"|(?:[^\S\n]++|[^\d\s]){0,12}?[,;=][^\S\n]*\n\s*"
    rf"|(?=[{MINUS}]))"
)
# The last value is not the head of a longer number ("6" is not "6.5" or "60").
_END = r"(?!\.?\d)"


def _forms(value: float, rounded: bool) -> List[str]:
    magnitude = abs(float(value))
    if magnitude.is_integer():
        # A one-digit value may be padded to two ("+ 10-98 + 04").
        pad = "0?" if magnitude < 10 else ""
        return [pad + re.escape(f"{magnitude:g}") + r"(?:\.0+)?"]
    # As printed, with any trailing digits the stored value lost ("4.50", "4.53").
    forms = [re.escape(f"{magnitude:g}") + r"\d*"]
    if rounded:
        # A table's decimals printed rounded in the prose: one place, or none.
        forms += [re.escape(f"{magnitude:.1f}") + r"0*", str(int(magnitude + 0.5))]
    return forms


def _number(value: float, rounded: bool) -> str:
    forms = "|".join(dict.fromkeys(_forms(value, rounded)))
    sign = rf"[{MINUS}]{_GAP}" if float(value) < 0 else _POSITIVE
    return rf"{sign}(?:{forms}){_END}{_SPREAD}"


@lru_cache(maxsize=4096)
def pattern(point: Triple, rounded: bool = False) -> re.Pattern:
    """The regex printing `point`'s x, y and z, in that order."""
    return re.compile(_SEPARATOR.join(_number(v, rounded) for v in point))


def triple(point) -> Optional[Triple]:
    """(x, y, z) of a point given as a mapping or an object; None when it has no numbers."""
    try:
        if hasattr(point, "get"):
            return tuple(float(point[axis]) for axis in "xyz")
        return tuple(float(getattr(point, axis)) for axis in "xyz")
    except (AttributeError, KeyError, TypeError, ValueError):
        return None


def find_all(point: Triple, text: str, start: int = 0, end: Optional[int] = None) -> List[Span]:
    """Every place `text[start:end]` prints `point`.

    The printed values first; a decimal point rounded in print only when no
    copy prints it as stored.
    """
    end = len(text) if end is None else end
    decimal = any(not float(v).is_integer() for v in point)
    for rounded in (False, True)[: 1 + decimal]:
        found = [m.span() for m in pattern(point, rounded).finditer(text, start, end)]
        if found:
            return found
    return []


#: Furthest a point found outside its passages may sit from one of them.
NEAR = 3000


def find_point(
    point: Triple, text: str, windows: Sequence[Span] = (), taken: Iterable[Span] = ()
) -> Optional[Span]:
    """The characters printing a point's x, y and z.

    Within one of `windows` first, at the first copy not in `taken`, or the
    first copy when every one is. A window cut differently from the text may
    not hold it, so then anywhere in the text: where it is printed once, or the
    copy nearest one of the windows.
    """
    taken = set(taken)
    inside = [s for a, b in windows for s in find_all(point, text, a, b)]
    if inside:
        return next((s for s in inside if s not in taken), inside[0])
    found = find_all(point, text)
    if len(found) == 1:
        return found[0]
    if found and windows:
        near = min((min(abs(s - a), abs(s - b)), (s, e)) for s, e in found for a, b in windows)
        if near[0] <= NEAR:
            return near[1]
    return None


def find_points(points: Sequence, text: str) -> List[Optional[Span]]:
    """Where each of a set's points is printed in `text`, a repeated point at its next copy."""
    taken: set = set()
    out: List[Optional[Span]] = []
    for point in points:
        xyz = triple(point)
        out.append(find_point(xyz, text, [(0, len(text))], taken) if xyz else None)
        if out[-1]:
            taken.add(out[-1])
    return out
