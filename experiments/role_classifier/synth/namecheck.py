"""Whether generated set names, captions and footers tell a set's class the way real ones do.

    python experiments/role_classifier/synth/namecheck.py GEN_DIR [GEN_DIR ...] \
        --units TABLE_UNITS.jsonl PROSE_UNITS.jsonl --labels LABEL_DIR [LABEL_DIR ...] \
        --id-map slug_dbids.json

Each GEN_DIR holds the units.jsonl and truth.jsonl of one role_units.py run (one --seed); at
least MIN_RUNS of them, and every number is the mean over runs. For each field (a set's name;
a table's caption; a table's footer) and origin, a bag-of-tokens logistic regression on that
field alone, class-weighted, is fit and scored over the classes both the generated and the real
labelled train sets hold (MIN_REAL or more real sets). Beside each generated run is a null run:
as many sets per class as the run holds, each a copy of a random real labelled set of the class
and as many to a copy as the run's sets that share a text (`null_rows`): what a generator that
copied real sets at their real rates would make. Both go
through the same code:

  real_cv      fit on real, scored on real, in 5-fold cross-validation
  gen_cv       fit and scored on the generated sets, in 5-fold cross-validation
  null_cv      the same on the null run
  real_on_gen  fit on real sets, scored on the generated ones
  gen_on_real  fit on generated sets, scored on the real ones; null_on_real for the null run

The folds are grouped by article. A generated name, caption or footer is a real one, so a
generated set is grouped with the real article its text came from (`by_source`): no model
scores a string from an article it was fit on, in or across domains. A null set keeps its
real article.

Per class the score is recall; over classes, balanced accuracy. Generated fields carry the
real cues at the real strength when they score as the null run does: gen_cv not above null_cv
(no extra cue), gen_on_real not below null_on_real (no lost cue), real_on_gen near real_cv
(a model of real data reads them as it reads real sets). Exit 1 when, for any field and origin,
a balanced accuracy is more than MAX_GAIN off, or a class's recall more than MAX_CLASS_SHIFT off
(`failures`). Comparing with the null rather than with real_cv takes out what only the sample
sizes move: a class's recall rises as the other classes' sets are cut to the generated run's
numbers (table reference captions: 0.00 in real_cv, 0.5 with result cut to as many sets as the
run holds). The tolerances are wider per class because a sparse real class has 5-25 sets. A class with fewer than MIN_REAL real sets or MIN_ARTICLES
articles has no real rate to hold it to and is left out (`unchecked`); one with fewer than
MIN_CLASS_REAL sets or MIN_CLASS_ARTICLES articles counts towards the balanced accuracy but is
not held to a per-class rate (`class_unchecked`). Also printed: each class's share of names of
each shape (`role_units.shape`), real and generated, the class each real cue word leans to in
generated names, and the spread of every number over the runs.
"""

import argparse
import collections
import hashlib
import json
import math
import random
import re
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))  # role_units: a sibling script

import role_units as ru  # noqa: E402
from ingestion_workflow.services.set_roles import labeling  # noqa: E402

MAX_GAIN = 0.10
MAX_CLASS_SHIFT = 0.20
MIN_REAL = 5
MIN_ARTICLES = 3
#: A class is held to MAX_CLASS_SHIFT only with this many real sets and articles: below them its
#: recall moved by 0.3 and more between two sets of 4 runs on the real labels.
MIN_CLASS_REAL, MIN_CLASS_ARTICLES = 15, 5
NULLS = 3  # null runs beside each generated run
#: Two sets of 4 runs disagreed (one failed a footer class at 0.53 against 0.22, the other passed it);
#: 8 runs did not.
MIN_RUNS = 8
FOLDS = 5
#: What each check reads off a unit and its set, and the origins that have it.
FIELDS = {
    "name": (lambda u, i: u["sets"][i].get("name"), ("table", "text")),
    "caption": (lambda u, i: u.get("caption"), ("table",)),
    "footer": (lambda u, i: u.get("footer"), ("table",)),
}


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


def recalls(w, rows):
    """{class: recall} of a fitted model on rows."""
    hit, seen = collections.Counter(), collections.Counter()
    for _, text, c in rows:
        seen[c] += 1
        hit[c] += predict(w, text) == c
    return {c: hit[c] / seen[c] for c in seen}


def fold(group, folds=FOLDS):
    return int(hashlib.sha1(str(group).encode()).hexdigest(), 16) % folds


def _key(text):
    return ru._TABLE_LABEL.sub("", " ".join((text or "").split())).lower()


def by_source(gen_rows, real_rows):
    """Generated rows grouped by the real article their text was copied from, so that no model
    scores a real string whose article it was fit on. A text several real articles hold (an empty
    footer, "BA, Brodmann area") goes to one of them, picked by the generated row's own article, so
    its copies spread over the folds as the real ones do. A text no real set has stays with its own
    unit's article, so a template string that recurs across units is seen in and out of a fold, as
    a shortcut would be."""
    source = collections.defaultdict(set)
    for a, text, _ in real_rows:
        source[_key(text)].add(str(a))
    out = []
    for g, t, c in gen_rows:
        arts = sorted(source.get(_key(t), ()))
        out.append((arts[fold(g, len(arts))] if arts else g, t, c))
    return out


def cv_recalls(rows, folds=FOLDS):
    """{class: recall} in folds grouped by article (`by_source` for generated rows)."""
    hit, seen = collections.Counter(), collections.Counter()
    for k in range(folds):
        train = [r for r in rows if fold(r[0]) != k]
        test = [r for r in rows if fold(r[0]) == k]
        if not test or len({c for _, _, c in train}) < 2:
            continue
        w = fit(train)
        for _, text, c in test:
            seen[c] += 1
            hit[c] += predict(w, text) == c
    return {c: hit[c] / seen[c] for c in seen}


def cross_recalls(train_rows, test_rows, folds=FOLDS):
    """{class: recall} on test_rows of models fit on train_rows, each scoring only its held-out fold."""
    hit, seen = collections.Counter(), collections.Counter()
    for k in range(folds):
        train = [r for r in train_rows if fold(r[0]) != k]
        test = [r for r in test_rows if fold(r[0]) == k]
        if not test or len({c for _, _, c in train}) < 2:
            continue
        w = fit(train)
        for _, text, c in test:
            seen[c] += 1
            hit[c] += predict(w, text) == c
    return {c: hit[c] / seen[c] for c in seen}


def null_rows(real_rows, clusters, seed=0):
    """Copies of random real rows, one per (class, k) of `clusters`, each copied k times: a generated
    table's sets share its one caption, so its null holds one real caption k times, not k captions."""
    rng = random.Random(seed)
    by = collections.defaultdict(list)
    for r in real_rows:
        by[r[2]].append(r)
    return [x for c, k in clusters if by[c] for x in [rng.choice(by[c])] * k]


def clusters(gen_rows):
    """(class, k) for each run of k generated rows that share an article, a text and a class."""
    return [(c, k) for (_, _, c), k in sorted(collections.Counter((str(g), _key(t), c) for g, t, c in gen_rows).items())]


def equal(rows, n_real, seed=0):
    """Rows cut, per class, to as many as the real sets hold: a generated run holds more sets of a
    sparse class than real data does, which alone would move every class's recall."""
    rng = random.Random(seed)
    by = collections.defaultdict(list)
    for r in rows:
        by[r[2]].append(r)
    return [x for c, rs in sorted(by.items()) for x in rng.sample(rs, min(len(rs), n_real[c]))]


def balanced(rec):
    return sum(rec.values()) / len(rec) if rec else None


def generated_rows(units, truth, field="name"):
    """(origin, (seed article, text, class)) of every generated set."""
    get, origins = FIELDS[field]
    by_id = {u["unit_id"]: u for u in units}
    for t in truth:
        u = by_id[t["unit_id"]]
        if u["origin"] in origins:
            yield u["origin"], (u["article_id"], get(u, ru.set_index(t["set_id"])), ru.role_class(t["role"], t["anchor_kind"]))


def real_rows(units, labels, field="name"):
    """(origin, (article, text, class)) of every labelled set of a real train unit."""
    get, origins = FIELDS[field]
    for sid, lab in labels.items():
        u, i = units.get(lab["unit_id"]), ru.set_index(sid)
        if u and lab.get("role") and i < len(u["sets"]) and u["origin"] in origins:
            yield u["origin"], (u["article_id"], get(u, i), ru.role_class(lab["role"], lab.get("anchor_kind")))


def _shares(rows):
    out = collections.defaultdict(collections.Counter)
    for _, name, c in rows:
        out[c][ru.shape(name)] += 1
    return {c: {k: round(v / sum(n.values()), 3) for k, v in n.most_common()} for c, n in sorted(out.items())}


def _mean(values):
    """{class: mean} over runs, and {class: half the range}: how far one run strays."""
    keys = sorted({k for v in values for k in v})
    mean = {k: statistics.fmean(v[k] for v in values if k in v) for k in keys}
    spread = {k: (max(v[k] for v in values if k in v) - min(v[k] for v in values if k in v)) / 2 for k in keys}
    return mean, spread


def tied_words(rows, cues):
    """{word: class its generated names lean to} for each real cue word in 3 or more generated names,
    by the word's class-balanced rate."""
    n = collections.Counter(c for _, _, c in rows)
    by = collections.defaultdict(collections.Counter)
    for _, name, c in rows:
        for t in ru._words(name):
            if t in cues:
                by[t][c] += 1
    out = {}
    for t, cnt in by.items():
        if sum(cnt.values()) >= 3:
            rate = {c: v / n[c] for c, v in cnt.items()}
            out[t] = max(sorted(rate), key=rate.get)
    return out


def compare(runs, real_units, labels, fields=tuple(FIELDS), cues=None):
    """{field: {origin: scores}} over generated runs [(units, truth), ...] and the real train sets.

    `cues` ({origin: {word: class}}, `role_units.Names.cues`) adds, for names, the class each real
    cue word leans to in the generated names.
    """
    out = {}
    for field in fields:
        real = collections.defaultdict(list)
        for origin, row in real_rows(real_units, labels, field):
            real[origin].append(row)
        gens = []
        for units, truth in runs:
            g = collections.defaultdict(list)
            for origin, row in generated_rows(units, truth, field):
                g[origin].append(row)
            gens.append(g)
        out[field] = {}
        for origin in sorted({o for g in gens for o in g}):
            n_real = collections.Counter(c for _, _, c in real[origin])
            articles = collections.defaultdict(set)
            for a, _, c in real[origin]:
                articles[c].add(a)
            gen_classes = {c for g in gens for _, _, c in g[origin]}
            classes = sorted(c for c in gen_classes if n_real[c] >= MIN_REAL and len(articles[c]) >= MIN_ARTICLES)
            mine = [r for r in real[origin] if r[2] in classes]
            if len(classes) < 2:
                continue
            real_cv = cv_recalls(mine)
            per_run = collections.defaultdict(list)
            for run, g in enumerate(gens):
                raw = [r for r in g[origin] if r[2] in classes]
                rows = equal(by_source(raw, mine), n_real, run)
                per_run["gen_cv"].append(cv_recalls(rows))
                per_run["real_on_gen"].append(cross_recalls(mine, rows))
                per_run["gen_on_real"].append(cross_recalls(rows, mine))
                nulls = [equal(null_rows(mine, clusters(raw), run * NULLS + j), n_real, run) for j in range(NULLS)]
                per_run["null_cv"].append(_mean([cv_recalls(x) for x in nulls])[0])
                per_run["null_on_real"].append(_mean([cross_recalls(x, mine) for x in nulls])[0])
            r = {"classes": classes,
                 "unchecked": {c: f"{n_real[c]} real sets from {len(articles[c])} articles" for c in sorted(gen_classes - set(classes))},
                 "class_unchecked": {c: f"{n_real[c]} real sets from {len(articles[c])} articles" for c in classes
                                     if n_real[c] < MIN_CLASS_REAL or len(articles[c]) < MIN_CLASS_ARTICLES},
                 "sets": {"real": len(mine), "generated": statistics.fmean(len(g[origin]) for g in gens)},
                 "real_cv": real_cv, "spread": {}}
            for k, values in per_run.items():
                r[k], r["spread"][k] = _mean(values)
            r["balanced"] = {k: balanced(r[k]) for k in ("real_cv", *per_run)}
            # A run whose sets all fall in one fold has no cross-validated score (None).
            scored = {k: [b for b in map(balanced, v) if b is not None] for k, v in per_run.items()}
            r["spread"]["balanced"] = {k: (max(b) - min(b)) / 2 for k, b in scored.items() if b}
            if field == "name":
                r["shapes"] = {"real": _shares(mine), "generated": _shares([x for g in gens for x in g[origin] if x[2] in classes])}
                if cues is not None:
                    real_cues = {t: c for t, c in cues.get(origin, {}).items() if c in classes}
                    gen = tied_words([x for g in gens for x in g[origin] if x[2] in classes], real_cues)
                    r["cue_words"] = {t: {"real": real_cues[t], "generated": c} for t, c in sorted(gen.items())}
            out[field][origin] = r
    return out


#: (generated, baseline, the side generated must not stray to, the failure): the checks `failures` makes.
#: A model fit on all real sets scores per-class recall whatever the test sets' number, so real_on_gen is
#: held to real_cv; the other two depend on how many sets each class has and are held to the null run.
CHECKS = (
    ("gen_cv", "null_cv", 1, "generated tells its class at {g:.2f}, real sets copied at their rates at {n:.2f} (an extra cue)"),
    ("gen_on_real", "null_on_real", -1, "generated teaches {g:.2f}, real sets copied at their rates {n:.2f} (a real cue is lost)"),
    ("real_on_gen", "real_cv", 0, "a real model reads generated at {g:.2f}, real sets at {n:.2f}"),
)


def failures(result, max_gain=MAX_GAIN, max_shift=MAX_CLASS_SHIFT):
    """Why the generated fields do not carry the real cues at the real strength; empty when they do.

    Per class and over classes (balanced accuracy), generated sets must score as the null run
    does and a model of real data must read them as it reads real sets (`CHECKS`): no extra cue,
    no lost cue.
    """
    out = []
    for field, origins in result.items():
        for origin, r in origins.items():
            held = [c for c in r["classes"] if c not in r.get("class_unchecked", {})]
            for k, tol in [("all", max_gain)] + [(c, max_shift) for c in held]:
                for gen, null, sign, why in CHECKS:
                    g, n = (r["balanced"][gen], r["balanced"][null]) if k == "all" else (r[gen].get(k), r[null].get(k))
                    if None in (g, n):
                        continue
                    off = g - n if sign > 0 else n - g if sign < 0 else abs(g - n)
                    if off > tol:
                        out.append(f"{field} {origin} {k}: " + why.format(g=g, n=n))
    return out


def _read(gen_dir):
    truth = [json.loads(line) for line in (gen_dir / "truth.jsonl").read_text().splitlines() if line.strip()]
    return labeling.read_units(gen_dir / "units.jsonl"), truth


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("gen_dirs", type=Path, nargs="+", help=f"role_units.py outputs, one per --seed; at least {MIN_RUNS}")
    ap.add_argument("--units", type=Path, nargs="+", required=True)
    ap.add_argument("--labels", type=Path, nargs="+", required=True)
    ap.add_argument("--id-map", type=Path, required=True)
    a = ap.parse_args()
    if len(a.gen_dirs) < MIN_RUNS:
        ap.error(f"pass at least {MIN_RUNS} runs: the per-class recalls of fewer runs stray past the tolerances")
    units, labels = ru.load_units(a.units), labeling.latest_labels(a.labels)
    train = ru.train_units(units, labels, json.loads(a.id_map.read_text()))
    result = compare([_read(d) for d in a.gen_dirs], train, labels, cues=ru.Names(train, labels).cues)
    print(json.dumps(result, indent=1))
    for field, origins in result.items():
        for origin, r in origins.items():
            print(field, origin, "balanced", {k: round(v, 3) for k, v in r["balanced"].items()})
    bad = failures(result)
    print("\n".join(bad) or "every field carries the real cues within the tolerances")
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
