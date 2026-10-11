"""Render every synthetic unit through the labeller's and the encoder's own code; fail on a label leak.

    python experiments/role_classifier/synth/leakcheck.py DIR     # DIR holds units.jsonl

A unit's sets hold only a name and points (xyz, stat, cluster); the label lives in truth.jsonl. The
rendered inputs must show none of the label fields, and the unit must say its labels are the dataset's
(`labels_from: dataset`) so the bulk labelling job skips it. Cue words that a real
passage would carry ("reference", "localization accuracy") are not a leak, so the forbidden words
are the names of label fields.
"""
import collections
import json
import sys
from pathlib import Path

from ingestion_workflow.services.set_roles import labeling

FIELD_WORDS = ("anchor_kind", "from_prior_study", "synthetic", "truth", '"role"', "hard_negative")


def check(units):
    """(unit count, {field word: n}); raises on a structural leak."""
    found = collections.Counter()
    for u in units:
        assert u.get("dataset") == "synthetic-v4" and u.get("labels_from") == "dataset", u["unit_id"]
        for s in u["sets"]:
            assert set(s) == {"name", "points"}, (u["unit_id"], sorted(s))
            for p in s["points"]:
                assert not isinstance(p, dict) or set(p) <= {"xyz", "stat", "cluster"}, (u["unit_id"], p)
        _, prompt, _ = labeling.render(u)
        for ctx in labeling.contexts(u):
            enc = labeling.serialize(u, ctx)
            for word in FIELD_WORDS:
                if word in enc.lower() or word in prompt.lower():
                    found[word] += 1
    return len(units), dict(found)


if __name__ == "__main__":
    n, found = check(labeling.read_units(Path(sys.argv[1]) / "units.jsonl"))
    print(n, "units; label field words in rendered inputs:", found)
    sys.exit(1 if found else 0)
