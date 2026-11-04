"""Find the data sources a paper's text names, by the tier of each alias.

Tiers, most to least trusted: `name` (a full name), `safe` (an acronym that names nothing
else), `context` (an acronym counted only near a dataset word), `defined` (counted only when
the paper also gives the full name) and `qualified` (an ordinary word, counted only beside a
dataset word, an OpenNeuro accession, or the full name).
"""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from typing import Dict, Iterable, List, Tuple

TIERS = ("name", "safe", "context", "defined", "qualified")

SEP = r"[\s\-‐‑–—]+"
CUE = re.compile(
    r"\b(?:data(?:sets?|base)?|cohorts?|samples?|stud(?:y|ies)|consortium|participants?|subjects?|"
    r"obtained|downloaded|release|initiative|project|repository|archive|sites?|scans?|imaging|"
    r"wave|baseline|follow-up)\b",
    re.I,
)
CUE_WINDOW = 80
# at most two words between the name and the dataset word: "Sherlock movie-watching data"
QUALIFIER_AFTER = re.compile(
    r"(?:[\s(\-]+[\w'’\-]+){0,2}?[\s(\-]+"
    r"(?:data ?sets?|data|corpus|collection|database|repository|challenge|platform)\b",
    re.I,
)
QUALIFIER_BEFORE = re.compile(
    r"\b(?:data ?sets?|corpus|collection|database|repository|platform)\b[^.\n]{0,20}$",
    re.I,
)
OPENNEURO_ACCESSION = re.compile(r"\bds\d{6}\b")
QUALIFIER_WINDOW = 100
SNIPPET = 120
MAX_SNIPPETS = 3

SECTIONS = [
    ("references", r"references?|bibliography|literature cited|works cited|reference list"),
    (
        "data_availability",
        r"data availability(?: statement)?|data and code availability|code availability|"
        r"availability of data(?: and materials?)?|data sharing(?: statement)?|"
        r"data accessibility|resource availability|data and software availability",
    ),
    (
        "acknowledgments",
        r"acknowledge?ments?|funding(?: information| sources?)?|financial support|"
        r"sources? of funding",
    ),
    (
        "methods",
        r"(?:materials? and |patients and |subjects and |online |star ?)?methods?"
        r"(?: and materials?)?|methodology|experimental procedures?|participants|subjects|"
        r"patients|study population|study design",
    ),
    ("results", r"results(?: and discussion)?"),
    ("discussion", r"(?:general )?discussion"),
    ("conclusion", r"conclusions?|concluding remarks"),
    ("intro", r"introduction|background"),
    ("abstract", r"abstract|summary"),
]
HEADING = re.compile(
    r"^(?:#{1,6}\s*)?\**(?:\d+(?:\.\d+)*\.?\s+|[IVX]+\.\s+)?(?:"
    + "|".join(f"(?P<{k}>{p})" for k, p in SECTIONS)
    + r")\**\s*:?\**$",
    re.I,
)
#: A data source named in these sections was used; in these, only mentioned.
USED = {"front", "abstract", "methods", "results", "data_availability", "acknowledgments"}
MENTIONED = {"intro", "discussion", "conclusion"}
#: A match's status, from the sections it is named in.
STATUSES = ("used", "mentioned", "references_only", "unsectioned")


def key(s: str) -> str:
    """The form two names are compared in: lower case, letters and digits only."""
    return re.sub(r"[^a-z0-9]", "", s.lower().replace("’", "'").replace("'s", "s"))


def alias_pattern(alias: str) -> str:
    tokens = [re.escape(t).replace("'", "['’]?") for t in re.split(SEP, alias.strip()) if t]
    return SEP.join(tokens)


def sections(text: str) -> List[Tuple[int, str]]:
    """[(start, section)] boundaries; the first is 'front' when any heading is found, else
    'body'."""
    marks, pos = [], 0
    for line in text.splitlines(keepends=True):
        s = line.strip()
        if s and len(s) <= 60 and len(s.split()) <= 6:
            m = HEADING.match(s)
            if m:
                marks.append((pos, m.lastgroup))
        pos += len(line)
    return [(0, "front" if marks else "body")] + marks


def section_at(marks: List[Tuple[int, str]], offset: int) -> str:
    sec = marks[0][1]
    for start, name in marks:
        if start > offset:
            break
        sec = name
    return sec


def snippet(text: str, start: int, end: int) -> str:
    return re.sub(r"\s+", " ", text[max(0, start - SNIPPET): end + SNIPPET])


def qualified(text: str, start: int, end: int) -> bool:
    # the accession must be in the same sentence: neighbouring sentences cite other datasets
    lo = max(text.rfind(".", 0, start) + 1, text.rfind("\n", 0, start) + 1,
             start - QUALIFIER_WINDOW)
    stops = [i for i in (text.find(". ", end), text.find("\n", end)) if i >= 0]
    hi = min(stops + [end + QUALIFIER_WINDOW])
    return bool(QUALIFIER_AFTER.match(text, end)
                or QUALIFIER_BEFORE.search(text, max(0, start - 40), start)
                or OPENNEURO_ACCESSION.search(text, lo, hi))


class Dictionary:
    """The aliases searched for, compiled: built once, then `find` per text."""

    def __init__(self, aliases: Iterable[Tuple[str, str, str]]) -> None:
        """`aliases`: (data source id, alias, tier). A full name keyed like another
        names the first data source given it."""
        self.names: Dict[str, str] = {}
        self.acronyms: Dict[str, List[Tuple[str, str]]] = {}
        full_names = set()
        for data_source_id, alias, tier in aliases:
            if tier not in TIERS:
                raise ValueError(f"unknown alias tier {tier!r} for {data_source_id}")
            if tier == "name":
                self.names.setdefault(key(alias), data_source_id)
                full_names.add(alias_pattern(alias))
            # one string may name several data sources; each keeps its tier
            elif (data_source_id, tier) not in self.acronyms.setdefault(alias, []):
                self.acronyms[alias].append((data_source_id, tier))
        name_alts = sorted(full_names, key=len, reverse=True)
        acr_alts = sorted((re.escape(a) for a in self.acronyms), key=len, reverse=True)
        never = r"(?!x)x"
        self.name_re = re.compile(r"(?<!\w)(?:" + ("|".join(name_alts) or never) + r")(?!\w)",
                                  re.I)
        self.acr_re = re.compile(r"(?<![\w\-])(?:" + ("|".join(acr_alts) or never) + r")(?!\w)")

    def find(self, text: str) -> dict:
        """{sectioned, matches}: each data source the text names, with its status, its
        hits by section, the forms it is named by, and its first hits as examples."""
        marks = sections(text)
        sectioned = marks[0][1] == "front"

        raw = []  # (data source, form, tier, start, end, section)
        for m in self.name_re.finditer(text):
            found = self.names.get(key(m.group()))
            # all-lowercase multi-word matches are prose ("adolescent brain and cognitive ...")
            if found and not (m.group().islower() and re.search(r"\s", m.group())):
                raw.append((found, m.group(), "name", m.start(), m.end(),
                            section_at(marks, m.start())))
        for m in self.acr_re.finditer(text):
            for found, tier in self.acronyms[m.group()]:
                raw.append((found, m.group(), tier, m.start(), m.end(),
                            section_at(marks, m.start())))
        defined = {h[0] for h in raw if h[2] == "name" and h[5] != "references"}

        hits = defaultdict(list)
        for found, form, tier, start, end, sec in raw:
            if tier == "defined" and found not in defined:
                continue
            if (tier == "context" and found not in defined
                    and not CUE.search(text, max(0, start - CUE_WINDOW),
                                       min(len(text), end + CUE_WINDOW))):
                continue
            if tier == "qualified" and found not in defined and not qualified(text, start, end):
                continue
            hits[found].append((form, tier, start, end, sec))

        matches = []
        for found, hs in sorted(hits.items()):
            secs = Counter(h[4] for h in hs)
            if not sectioned:
                status = "unsectioned"
            elif USED & secs.keys():
                status = "used"
            elif MENTIONED & secs.keys():
                status = "mentioned"
            else:
                status = "references_only"
            ranked = sorted(hs, key=lambda h: (h[4] not in USED, h[4] == "references", h[2]))
            matches.append({
                "data_source_id": found,
                "status": status,
                "n_hits": len(hs),
                "sections": dict(secs.most_common()),
                "forms": sorted({h[0] for h in hs}),
                "examples": [{"form": form, "tier": tier, "section": sec, "start": start,
                          "end": end, "snippet": snippet(text, start, end)}
                         for form, tier, start, end, sec in ranked[:MAX_SNIPPETS]],
            })
        return {"sectioned": sectioned, "matches": matches}
