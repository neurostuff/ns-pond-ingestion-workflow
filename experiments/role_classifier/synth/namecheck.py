"""How well a set's name alone predicts its class, in generated units and in the real labels.

    python experiments/role_classifier/synth/namecheck.py GEN_DIR \
        --units TABLE_UNITS.jsonl PROSE_UNITS.jsonl --labels LABEL_DIR [LABEL_DIR ...] \
        [--id-map slug_dbids.json]

GEN_DIR holds role_units.py's units.jsonl and truth.jsonl. Per origin, over the classes
(role, and anchor kind) the generated units hold, a bag-of-tokens logistic regression on
the name alone, class-weighted, is scored by balanced accuracy in article-grouped 5-fold
cross-validation: once on the generated sets, once on the real labelled train sets. A
name the generator ties to its class lets a classifier skip the context, so generated
names must not predict the class much better than real ones: exit 1 when the generated
score is more than MAX_GAIN above the real one. Also printed: each class's share of
names of each shape (`role_units.shape`), real and generated.
"""

import argparse
import collections
import hashlib
import json
import math
import random
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))  # role_units: a sibling script

import role_units as ru  # noqa: E402
from ingestion_workflow.services.set_roles import labeling  # noqa: E402

MAX_GAIN = 0.10
FOLDS = 5


def tokens(name):
    return set(re.findall(r"[a-z]+|\d+|[^\sa-z\d]", (name or "").lower())) or {"<unnamed>"}


def fit(rows, epochs=20, rate=0.3, seed=0):
    """A softmax regression over name tokens by SGD; each class weighs as much as the others."""
    classes = sorted({c for _, _, c in rows})
    n = collections.Counter(c for _, _, c in rows)
    weight = {c: len(rows) / (len(classes) * n[c]) for c in classes}
    w = {c: collections.defaultdict(float) for c in classes}
    data = [(tokens(name), c) for _, name, c in rows]
    order = random.Random(seed)
    for epoch in range(epochs):
        order.shuffle(data)
        step = rate / (1 + epoch)
        for x, y in data:
            p = _probs(w, x)
            for c in classes:
                g = step * weight[y] * (p[c] - (c == y))
                for t in x | {"<bias>"}:
                    w[c][t] -= g
    return w


def _probs(w, x):
    s = {c: sum(wc.get(t, 0.0) for t in x | {"<bias>"}) for c, wc in w.items()}
    top = max(s.values())
    e = {c: math.exp(v - top) for c, v in s.items()}
    z = sum(e.values())
    return {c: v / z for c, v in e.items()}


def predict(w, name):
    p = _probs(w, tokens(name))
    return max(sorted(p), key=p.get)


def balanced_accuracy(rows, folds=FOLDS):
    """Mean per-class recall of the name-only classifier, in folds grouped by article."""
    fold = lambda group: int(hashlib.sha1(str(group).encode()).hexdigest(), 16) % folds  # noqa: E731
    hit, seen = collections.Counter(), collections.Counter()
    for k in range(folds):
        train = [r for r in rows if fold(r[0]) != k]
        test = [r for r in rows if fold(r[0]) == k]
        if not test or len({c for _, _, c in train}) < 2:
            continue
        w = fit(train)
        for _, name, c in test:
            seen[c] += 1
            hit[c] += predict(w, name) == c
    return sum(hit[c] / seen[c] for c in seen) / len(seen) if seen else None


def generated_rows(units, truth):
    """(origin, (seed article, name, class)) of every generated set."""
    by_id = {u["unit_id"]: u for u in units}
    for t in truth:
        u = by_id[t["unit_id"]]
        name = u["sets"][ru.set_index(t["set_id"])].get("name")
        yield u["origin"], (u["article_id"], name, ru.role_class(t["role"], t["anchor_kind"]))


def real_rows(units, labels):
    """(origin, (article, name, class)) of every labelled set of a real train unit."""
    for sid, lab in labels.items():
        u, i = units.get(lab["unit_id"]), ru.set_index(sid)
        if u and lab.get("role") and i < len(u["sets"]):
            yield (
                u["origin"],
                (
                    u["article_id"],
                    u["sets"][i].get("name"),
                    ru.role_class(lab["role"], lab.get("anchor_kind")),
                ),
            )


def _shares(rows):
    out = collections.defaultdict(collections.Counter)
    for _, name, c in rows:
        out[c][ru.shape(name)] += 1
    return {
        c: {k: round(v / sum(n.values()), 3) for k, v in n.most_common()}
        for c, n in sorted(out.items())
    }


def compare(gen_units, truth, real_units, labels):
    """Per origin: classes, set counts, name-only balanced accuracy and shape shares."""
    gen, real = collections.defaultdict(list), collections.defaultdict(list)
    for origin, row in generated_rows(gen_units, truth):
        gen[origin].append(row)
    for origin, row in real_rows(real_units, labels):
        real[origin].append(row)
    out = {}
    for origin, rows in sorted(gen.items()):
        classes = sorted({c for _, _, c in rows})
        mine = [r for r in real[origin] if r[2] in classes]
        out[origin] = {
            "classes": classes,
            "sets": {"real": len(mine), "generated": len(rows)},
            "real": balanced_accuracy(mine),
            "generated": balanced_accuracy(rows),
            "shapes": {"real": _shares(mine), "generated": _shares(rows)},
        }
    return out


def too_telling(result, max_gain=MAX_GAIN):
    """The origins whose generated names predict the class max_gain better than real names do."""
    return [
        o
        for o, r in result.items()
        if r["real"] is not None
        and r["generated"] is not None
        and r["generated"] > r["real"] + max_gain
    ]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("gen_dir", type=Path)
    ap.add_argument("--units", type=Path, nargs="+", required=True)
    ap.add_argument("--labels", type=Path, nargs="+", required=True)
    ap.add_argument("--id-map", type=Path)
    a = ap.parse_args()
    units, labels = ru.load_units(a.units), labeling.latest_labels(a.labels)
    id_map = json.loads(a.id_map.read_text()) if a.id_map else None
    result = compare(
        labeling.read_units(a.gen_dir / "units.jsonl"),
        [
            json.loads(line)
            for line in (a.gen_dir / "truth.jsonl").read_text().splitlines()
            if line.strip()
        ],
        ru.train_units(units, labels, id_map),
        labels,
    )
    print(json.dumps(result, indent=1))
    bad = too_telling(result)
    print(
        "name-only balanced accuracy:",
        {
            o: (round(r["real"], 3), round(r["generated"], 3))
            for o, r in result.items()
            if r["real"] is not None and r["generated"] is not None
        },
        "(real, generated); over the limit:",
        bad or "none",
    )
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
