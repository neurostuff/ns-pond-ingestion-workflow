"""Read every synthetic unit as the labeller and the encoder do; fail on any label it carries.

    python experiments/role_classifier/synth/leakcheck.py DIR     # DIR holds units.jsonl

Run it on the generator's output and again on the rewrite pass's. The label lives in
truth.jsonl only. A unit may hold only the keys real labelling units hold (`UNIT_KEYS`),
its sets only a name and points, its points only xyz, stat and cluster, and it must say
its labels are the dataset's (`labels_from: dataset`) so the bulk labelling job skips it.
Every text field and everything the labeller's prompt and the encoder's inputs render
must be free of label syntax: a label field name, `role =`, `role is <role>`, a
`[PROPOSED]` hint, or a set name that is a role value with a qualifier ("reference (prior
study)", "anchor: roi", "Reference - prior study", "Anchor, ROI"). Text is NFKC-normalised,
stripped of zero-width characters and casefolded before matching, so a fullwidth colon or
an invisible space does not hide a label. Cue words a real passage carries ("reference",
"localization accuracy", "ground truth", "Other-race faces", "Other > Self") are not a leak, so the
patterns match label syntax rather than words.
"""

import collections
import json
import re
import sys
import unicodedata
from pathlib import Path

from ingestion_workflow.services.set_roles import labeling
from ingestion_workflow.services.set_roles.labels import COORDINATE_ROLES

UNIT_KEYS = frozenset(
    {
        "unit_id",
        "origin",
        "article_id",
        "source",
        "dataset",
        "labels_from",
        "stratum",
        "title",
        "abstract",
        "table_id",
        "table_label",
        "caption",
        "footer",
        "table_serialised",
        "citing",
        "heading",
        "text",
        "before",
        "after",
        "sets",
    }
)
_ROLES = "|".join(sorted({*COORDINATE_ROLES, "display"}, key=len, reverse=True))
_ZERO_WIDTH = dict.fromkeys(map(ord, "\u00ad\u180e\u200b\u200c\u200d\u200e\u200f\u2060\ufeff"))
#: Label syntax in any rendered or raw text, after `normalize`.
LABEL_SYNTAX = re.compile(
    r"\[\s*proposed\s*\]"
    r"|\b(?:role|anchor[ _]kind|from[ _]prior[ _]study|hard[ _]negative|synthetic[ _]set)\s*[:=]"
    rf"|\brole\s*(?:->|=>|\u2192|\bis\b|\bwas\b)\s*[\"'(\[]?\s*(?:{_ROLES})\b"
    r"|[\"'](?:role|anchor_kind|from_prior_study)[\"']"
)
#: A set name, after `normalize`, that is a role on its own or with a qualifier after it. A hyphen
#: qualifies only with a space or `>` after it and `>` alone not at all: "other-race faces" and
#: "other > self" are real names. Anchor kinds alone are
#: real names ("ROI", "Seed regions"), at the rate `role_units.Names` reproduces.
LABEL_NAME = re.compile(rf"^\s*(?:{_ROLES})\s*(?:$|[:=(\[/|,\u2013\u2014\u2192]|-(?:\s|$|>))")


def normalize(text):
    """NFKC, no zero-width characters, casefolded: the form both patterns match."""
    return unicodedata.normalize("NFKC", text).translate(_ZERO_WIDTH).casefold()


def label_name(name):
    return bool(name) and bool(LABEL_NAME.search(normalize(name)))


def label_syntax(text):
    return bool(text) and bool(LABEL_SYNTAX.search(normalize(text)))


def _strings(value):
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for v in value.values():
            yield from _strings(v)
    elif isinstance(value, (list, tuple)):
        for v in value:
            yield from _strings(v)


def leaks(unit):
    """The reasons one unit carries its label; empty when it carries none."""
    out = []
    if unit.get("labels_from") != "dataset" or not str(unit.get("dataset") or "").startswith(
        "synthetic"
    ):
        out.append("labels_from")
    extra = set(unit) - UNIT_KEYS
    if extra:
        out.append(f"unit keys {sorted(extra)}")
    for s in unit.get("sets") or []:
        if set(s) != {"name", "points"}:
            out.append(f"set keys {sorted(s)}")
        if label_name(s.get("name")):
            out.append("set name")
        for p in s.get("points") or []:
            if isinstance(p, dict) and not set(p) <= {"xyz", "stat", "cluster"}:
                out.append(f"point keys {sorted(p)}")
    raw = [unit[k] for k in unit if k != "sets"] + [s.get("name") for s in unit.get("sets") or []]
    if any(label_syntax(t) for t in _strings(raw)):
        out.append("text field")
    _, prompt, _ = labeling.render(unit)
    rendered = [prompt] + [labeling.serialize(unit, ctx) for ctx in labeling.contexts(unit)]
    if any(label_syntax(t) for t in rendered):
        out.append("rendered input")
    return sorted(set(out))


def check(units):
    """(unit count, {reason: units with it})."""
    found = collections.Counter()
    for u in units:
        found.update(leaks(u))
    return len(units), dict(found)


if __name__ == "__main__":
    n, found = check(labeling.read_units(Path(sys.argv[1]) / "units.jsonl"))
    print(n, "units; label leaks by reason:", json.dumps(found))
    sys.exit(1 if found else 0)
