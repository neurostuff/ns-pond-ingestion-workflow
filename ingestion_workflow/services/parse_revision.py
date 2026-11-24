"""A paper's new coordinate parse as a revision of the one Neurostore holds.

Neurostore keys analyses by content (name, space, points with their values), so a new
parse's analysis whose key changed is a new analysis, and what is stored against the old
one (the extraction record's claims) stays behind in history. A revision (`revision_of`
set) gives each analysis of the revised parse a verdict, and Neurostore copies the claims
along the replacing ones (study_schema `AnalysisVerdict`).

`revise` assigns them, conservatively, within each table (or the text):

    accept   the same key
    edit     one analysis each side, with the same coordinates and a name equal after
             `loose_name` (a statistic, space or spelling corrected); with the same
             coordinates, which no other analysis on either side has, the same role, and
             another name whose contrast does not run the other way (renamed: v23 prefixes
             a table's section, "Go/No-go task — Controls > ADHD");
             among several with the same coordinates, the one whose name ends with the
             other's; or with an equal name and coordinates corrected, at least
             EDIT_OVERLAP of them shared
    split    the old analysis's coordinates are exactly the new parts' together
    merge    the new analysis's coordinates are exactly the old parts' together
    omit     anything else old; each new analysis left over is an `add`

Coordinates are compared as printed to 0.01 mm, ignoring each point's statistic, which the
key counts but a misread label does not change the analysis. No verdict is `relabel`: that
is a reviewer's correction, and Neurostore keeps it against later parses.
"""

from __future__ import annotations

import copy
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from study_schema.keys import normalize_name

#: The share of the larger side's coordinates an edit by name must keep.
EDIT_OVERLAP = 0.5
#: Parts tried for one exact split or merge: the search is exponential in them.
MAX_PARTS = 16
#: The verdicts that leave every claim where Neurostore can reach it.
CARRYING = ("accept", "edit", "split", "merge")

REASONS = {
    "accept": "the same key",
    "edit_points": "the same coordinates and name; a statistic, space or spelling differs",
    "edit_name": "the same name; coordinates corrected",
    "renamed": "the same coordinates, which no other analysis has, under another name",
    "context": "the same coordinates, under its name with the table's section added",
    "split": "its coordinates are exactly those of the analyses it was split into",
    "merge": "its coordinates are part of the analysis it was merged into",
    "omit": "no analysis of the new parse is matched to it",
    "add": "no analysis of the revised parse is matched to it",
}

Coords = Tuple[str, str, str]


#: Kept by `loose_name`: they say which way a contrast runs.
SIGNS = set("<>+-=≤≥≠×/")


def loose_name(name: Optional[str]) -> str:
    """study_schema's `normalize_name` with punctuation dropped too, but for the signs of
    a contrast: `IFG (AC>DD)` and `IFG AC > DD` are one name, `A > B` and `A < B` two."""
    return "".join(ch for ch in normalize_name(name or "") if ch.isalnum() or ch in SIGNS)


def directions(name: Optional[str]) -> str:
    """The `<` and `>` of a name, in order: `A > B` and `A < B` run opposite ways."""
    return "".join(ch for ch in loose_name(name) if ch in "<>≤≥").translate(
        str.maketrans("≤≥", "<>"))


def _same_way(a: str, b: str) -> bool:
    """Neither name's contrast runs against the other's."""
    return not (a and b) or a == b


def coordinates(analysis: Mapping[str, Any]) -> Counter:
    """The analysis's points as printed (x, y, z to 0.01 mm), counted; no statistic."""
    return Counter(
        tuple(f"{round(float(c), 2) + 0.0:.2f}" for c in point["coordinates"])
        for point in analysis.get("points") or ())


def _group(analysis: Mapping[str, Any]) -> Tuple[str, Optional[str]]:
    return analysis.get("origin") or "", analysis.get("table_id")


@dataclass
class Revision:
    """The verdicts of a new parse on the one it revises, and the new analyses' sources."""

    verdicts: List[Dict[str, Any]]
    #: New key -> the old keys a split or a merge made it from.
    derived_from: Dict[str, List[str]] = field(default_factory=dict)

    def counts(self) -> Counter:
        return Counter(v["verdict"] for v in self.verdicts)

    @property
    def carries_all(self) -> bool:
        """Every old analysis's claims reach the new parse, and it has nothing new."""
        return all(v["verdict"] in CARRYING for v in self.verdicts)

    def key_map(self) -> Dict[str, List[str]]:
        """Old key -> the new keys that take its place; [] for an omitted one."""
        return {v["key"]: list(v.get("replaced_by") or ())
                for v in self.verdicts if v["verdict"] != "add"}

    @property
    def added(self) -> List[str]:
        return [v["key"] for v in self.verdicts if v["verdict"] == "add"]


def _exact_cover(target: Counter, parts: Sequence[Tuple[str, Counter]]) -> Optional[List[str]]:
    """The keys of two or more `parts` whose coordinates add up to `target` exactly."""
    parts = [(k, c) for k, c in parts if c and not (c - target)][:MAX_PARTS]
    need = sum(target.values())

    def search(i: int, left: Counter, chosen: List[str]) -> Optional[List[str]]:
        if not left:
            return list(chosen) if len(chosen) >= 2 else None
        for j in range(i, len(parts)):
            key, held = parts[j]
            if not (held - left):
                found = search(j + 1, left - held, chosen + [key])
                if found:
                    return found
        return None

    if sum(sum(c.values()) for _, c in parts) < need:
        return None
    return search(0, Counter(target), [])


def revise(old: Mapping[str, Any], new: Mapping[str, Any]) -> Revision:
    """`new`'s verdicts on every analysis of `old`, and an `add` for each of its own that
    none replaces. Both are CoordinateParse documents (dicts)."""
    old_all = list(old.get("analyses") or ())
    new_all = list(new.get("analyses") or ())
    new_keys = {a["key"] for a in new_all}
    verdicts: Dict[str, Dict[str, Any]] = {}
    derived: Dict[str, List[str]] = {}
    used: set = set()

    def judge(key: str, verdict: str, replaced_by: Iterable[str], reason: str) -> None:
        verdicts[key] = {"key": key, "verdict": verdict, "replaced_by": list(replaced_by),
                         "reason": REASONS[reason]}

    for analysis in old_all:
        if analysis["key"] in new_keys:
            judge(analysis["key"], "accept", [analysis["key"]], "accept")
            used.add(analysis["key"])

    groups: Dict[Tuple[str, Optional[str]], Tuple[list, list]] = {}
    for analysis in old_all:
        if analysis["key"] not in verdicts:
            groups.setdefault(_group(analysis), ([], []))[0].append(analysis)
    for analysis in new_all:
        if analysis["key"] not in used:
            groups.setdefault(_group(analysis), ([], []))[1].append(analysis)

    for olds, news in groups.values():
        olds = [(a["key"], loose_name(a.get("name")), coordinates(a), a.get("role"),
                 directions(a.get("name"))) for a in olds]
        news = [(a["key"], loose_name(a.get("name")), coordinates(a), a.get("role"),
                 directions(a.get("name"))) for a in news]

        def unique_pairs(sig):
            """(old, new) where each is the only one left on its side with this signature."""
            mine = Counter(sig(o) for o in olds if o[0] not in verdicts)
            theirs = Counter(sig(n) for n in news if n[0] not in used)
            by_sig = {sig(n): n for n in news if n[0] not in used}
            return [(o, by_sig[sig(o)]) for o in olds if o[0] not in verdicts
                    and mine[sig(o)] == 1 and theirs.get(sig(o)) == 1]

        def frozen(c: Counter):
            return tuple(sorted(c.items()))

        for o, n in unique_pairs(lambda a: (a[1], frozen(a[2]))):
            judge(o[0], "edit", [n[0]], "edit_points")
            used.add(n[0])

        for o, n in unique_pairs(lambda a: frozen(a[2]) or None):
            if o[2] and o[3] == n[3] and _same_way(o[4], n[4]):
                judge(o[0], "edit", [n[0]], "renamed")
                used.add(n[0])

        for o in olds:
            if o[0] in verdicts or not o[2]:
                continue
            twins = [n for n in news if n[0] not in used and n[2] == o[2]
                     and (n[1].endswith(o[1]) or o[1].endswith(n[1])) and n[1] and o[1]]
            rivals = [p for p in olds if p[0] not in verdicts and p[2] == o[2]
                      and any(n[1].endswith(p[1]) or p[1].endswith(n[1]) for n in twins)]
            if len(twins) == 1 and len(rivals) == 1:
                judge(o[0], "edit", [twins[0][0]], "context")
                used.add(twins[0][0])

        for o in olds:
            if o[0] in verdicts or not o[2]:
                continue
            parts = _exact_cover(o[2], [(n[0], n[2]) for n in news if n[0] not in used])
            if parts:
                judge(o[0], "split", parts, "split")
                used.update(parts)
                for key in parts:
                    derived.setdefault(key, []).append(o[0])

        for n in news:
            if n[0] in used or not n[2]:
                continue
            parts = _exact_cover(n[2], [(o[0], o[2]) for o in olds if o[0] not in verdicts])
            if parts:
                for key in parts:
                    judge(key, "merge", [n[0]], "merge")
                derived[n[0]] = list(parts)
                used.add(n[0])

        for o, n in unique_pairs(lambda a: a[1]):
            shared = sum((o[2] & n[2]).values())
            larger = max(sum(o[2].values()), sum(n[2].values()))
            if larger == 0 or shared / larger >= EDIT_OVERLAP:
                judge(o[0], "edit", [n[0]], "edit_name")
                used.add(n[0])

    ordered = []
    for analysis in old_all:
        key = analysis["key"]
        if key not in verdicts:
            judge(key, "omit", [], "omit")
        ordered.append(verdicts[key])
    for analysis in new_all:
        if analysis["key"] not in used:
            ordered.append({"key": analysis["key"], "verdict": "add", "replaced_by": [],
                            "reason": REASONS["add"]})
    return Revision(ordered, derived)


def as_revision(new: Mapping[str, Any], revised_parse_id: str,
                revision: Revision) -> Dict[str, Any]:
    """`new` as a revision of the parse `revised_parse_id`: `revision_of`, its verdicts,
    and `derived_from` on the analyses a split or a merge made. Keys are untouched."""
    out = copy.deepcopy(dict(new))
    out["revision_of"] = revised_parse_id
    out["verdicts"] = [
        {k: v for k, v in verdict.items() if k != "replaced_by" or verdict["verdict"] != "add"}
        for verdict in revision.verdicts]
    for analysis in out.get("analyses") or ():
        if analysis["key"] in revision.derived_from:
            analysis["derived_from"] = list(revision.derived_from[analysis["key"]])
    return out


__all__ = ["CARRYING", "EDIT_OVERLAP", "Revision", "as_revision", "coordinates",
           "loose_name", "revise"]
