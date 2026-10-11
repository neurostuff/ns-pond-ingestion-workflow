"""Synthetic prose passages with known coordinates, roles and statistics.

Vocabulary (region phrases, contrast names, filler sentences) is harvested from
the silver *training* contexts only, and split in two so the synthetic test set
uses phrases the synthetic training set never contains. Hard cases are
overweighted on purpose: prior_study, results reported inside an ROI or in a
figure legend, a z statistic beside the z coordinate, and lures that look like
coordinates but are not.

Usage: synth_prose.py silver.jsonl gold-candidates.jsonl out_dir n_train n_test

Extended for role label version 4 (study_schema roles, no `display`): see `role_units.py`,
which builds whole labelling units (table and prose) seeded from real units, with the
sparse roles' cues and hard negatives, and reuses this module's surface forms
(`coord_text`, `stat_text`, `cluster_text`, `wrap`). `main` writes each point's role in
study_schema's fields through `V4_ROLE`.
"""
import json
import random
import re
import sys
from pathlib import Path

from ingestion_workflow.services import prose_passages as pc

MINUS_FORMS = ["-", "−", "−", "–", "− ", "− "]
SPACES = ["MNI", "TAL"]


#: This generator's point roles -> (role, anchor_kind, from_prior_study) in study_schema's
#: vocabulary. `figure` (a slice or crosshair position) was `display`; v4 makes it `other`.
V4_ROLE = {
    "result": ("result", None, False),
    "roi": ("anchor", "roi", False),
    "seed": ("anchor", "seed", False),
    "target": ("anchor", "stimulation_target", False),
    "prior_study": ("reference", None, True),
    "figure": ("other", None, False),
}


# -- vocabulary ---------------------------------------------------------------

REGION_RX = re.compile(
    r"\b((?:left|right|bilateral)\s+(?:[a-z]+\s+){0,3}(?:gyrus|cortex|lobule|insula|amygdala|hippocampus|"
    r"thalamus|putamen|caudate|precuneus|cerebellum|nucleus|operculum|pole|sulcus|cingulate|striatum|"
    r"junction|area|DLPFC|IFG|MTG|STG|SMA|TPJ|ACC|PCC|vmPFC|mPFC|OFC|IPL|SPL|IPS|FEF|AI))\b",
    re.I,
)


def harvest(rows):
    regions, contrasts, fillers = set(), set(), set()
    for r in rows:
        for m in REGION_RX.finditer(r["text"]):
            phrase = " ".join(m.group(1).split())
            if 2 <= len(phrase.split()) <= 5:
                regions.add(phrase[0].lower() + phrase[1:])
        for p in r["points"]:
            name = (p.get("analysis") or "").strip()
            name = re.split(r"\s+[\u2014\u2013-]\s+|[,;:(]", name)[0].strip(" []")
            if (p.get("role") == "result" and 8 <= len(name) <= 50 and not re.search(r"\d", name)
                    and re.search(r">|<|\bvs\.?\b|\bversus\b|interaction|main effect|correlat", name, re.I)):
                contrasts.add(name)
        for sent in re.split(r"(?<=[.!?])\s+(?=[A-Z])", r["text"]):
            sent = " ".join(sent.split())
            if not 60 <= len(sent) <= 260:
                continue
            # A filler must hold no coordinate at all: anything left unlabelled
            # teaches the model to skip coordinates.
            flat = re.sub(rf"[{pc.MINUS}]\s*", "-", sent)
            if (pc.find(sent) or re.search(r"\b[xyz]\s*[=:]", sent, re.I)
                    or re.search(r"-?\d+(?:\.\d+)?\s*[,;/ ]\s*-?\d+(?:\.\d+)?\s*[,;/ ]\s*-?\d+", flat)
                    or re.search(r"\b(MNI|Talairach)\b", sent)):
                continue
            fillers.add(sent)
    return sorted(regions), sorted(contrasts), sorted(fillers)


def halves(items, rng):
    items = list(items)
    rng.shuffle(items)
    cut = len(items) // 2
    return items[:cut], items[cut:]


# -- surface forms ----------------------------------------------------------------

def num(v, rng, decimals):
    s = f"{abs(v):.{decimals}f}" if decimals else str(int(abs(v)))
    if v < 0:
        return rng.choice(MINUS_FORMS) + s
    return ("+" + s) if rng.random() < 0.05 else s


SPACE_WORD = {"MNI": "MNI", "TAL": "Talairach", None: None}
_PASSAGE_SPACE = [None]


def coord_text(xyz, rng, style=None):
    decimals = 1 if rng.random() < 0.15 else 0
    x, y, z = (num(v, rng, decimals) for v in xyz)
    space = SPACE_WORD[_PASSAGE_SPACE[0]]
    styles = ["labelled", "paren", "bracket_space", "bracket_comma", "prefixed", "headed", "slash", "labelled_semicolon"]
    weights = [22, 18, 10, 12, 12 if space else 0, 8, 4, 6]
    style = style or rng.choices(styles, weights=weights)[0]
    if style == "labelled":
        return f"{rng.choice(['', space + ' '] if space else [''])}x = {x}, y = {y}, z = {z}"
    if style == "labelled_semicolon":
        return f"x = {x}; y = {y}; z = {z}"
    if style == "paren":
        return f"({x}, {y}, {z})"
    if style == "bracket_space":
        return f"[{x} {y} {z}]"
    if style == "bracket_comma":
        return f"[{x}, {y}, {z}]"
    if style == "prefixed":
        return f"{space}: {x}, {y}, {z}"
    if style == "headed":
        return f"(x, y, z) = ({x}, {y}, {z})"
    return f"{x}/{y}/{z}"


def wrap(c):
    """A coordinate in running text: in parentheses unless it already has brackets."""
    return c if c[:1] in "([" else f"({c})"


def realistic_xyz(rng):
    x = rng.choice([-1, 1]) * rng.randint(2, 66)
    y = rng.randint(-100, 66)
    z = rng.randint(-44, 74)
    return [x, y, z]


STATS = [("T", "t = {v}"), ("T", "t({df}) = {v}"), ("T", "T = {v}"), ("Z", "Z = {v}"), ("Z", "Z-score = {v}"),
         ("Z", "z = {v}"), ("F", "F({a}, {b}) = {v}"), ("F", "F = {v}"), ("R", "r = {v}"), ("P", "p = {v}")]


def stat_text(rng):
    kind, fmt = rng.choice(STATS)
    if kind == "R":
        v = round(rng.choice([-1, 1]) * rng.uniform(0.25, 0.8), 2)
    elif kind == "P":
        v = round(rng.choice([0.001, 0.003, 0.008, 0.012, 0.021, 0.034, 0.041]), 3)
    else:
        v = round(rng.uniform(3.1, 9.5), 2)
    return kind, v, fmt.format(v=v, df=rng.randint(14, 60), a=1, b=rng.randint(20, 90))


def cluster_text(rng):
    if rng.random() < 0.5:
        n = rng.randint(8, 2400)
        return [n, "voxels"], rng.choice([f"k = {n}", f"{n} voxels", f"cluster size = {n} voxels", f"k = {n} voxels"])
    n = rng.randint(64, 9000)
    return [n, "mm^3"], rng.choice([f"{n} mm3", f"{n} mm³", f"cluster volume = {n} mm3"])


# -- passages -----------------------------------------------------------------

def result_sentence(rng, V, n_points, in_roi=False, legend=False):
    contrast = rng.choice(V["contrasts"])
    parts, points = [], []
    for _ in range(n_points):
        xyz = realistic_xyz(rng)
        region = rng.choice(V["regions"])
        bits, stat, cluster = [coord_text(xyz, rng)], None, None
        if rng.random() < 0.75:
            kind, v, s = stat_text(rng)
            stat = [kind, v]
            bits.append(s)
        if rng.random() < 0.5:
            cluster, c = cluster_text(rng)
            bits.append(c)
        rng.shuffle(bits) if rng.random() < 0.2 else None
        parts.append(f"the {region} ({'; '.join(bits)})")
        points.append({"xyz": xyz, "role": "result", "stat": stat, "cluster": cluster, "analysis": contrast})
    listing = parts[0] if len(parts) == 1 else ", ".join(parts[:-1]) + " and " + parts[-1]
    if legend:
        lead = rng.choice(["(A) Regions showing", "Figure 3. Clusters showing", "(B) Brain areas with"])
        text = f"{lead} a significant effect for {contrast} in {listing}."
    elif in_roi:
        text = f"Within the a priori region of interest, {contrast} revealed a significant effect in {listing}."
    else:
        verb = rng.choice(["revealed increased activation in", "was associated with activity in", "showed significant clusters in", "yielded peaks in"])
        text = f"The contrast {contrast} {verb} {listing}."
    return text, points


def roi_sentence(rng, V, role):
    xyz = realistic_xyz(rng)
    region = rng.choice(V["regions"])
    author = rng.choice(["Smith", "Garcia", "Chen", "Muller", "Rossi", "Kim", "Novak"])
    year = rng.randint(1998, 2023)
    c = coord_text(xyz, rng)
    if role == "roi":
        text = rng.choice([
            f"A {rng.choice([6, 8, 10])}-mm spherical ROI was centred on the {region} {wrap(c)}, based on {author} et al. ({year}).",
            f"Region-of-interest analyses used a mask of the {region} centred at {c}.",
        ])
    elif role == "seed":
        text = rng.choice([
            f"The {region} seed {wrap(c)} was used for whole-brain functional connectivity analysis.",
            f"For the PPI analysis, the time course was extracted from a sphere around the {region} peak {wrap(c)}.",
        ])
    elif role == "target":
        text = rng.choice([
            f"rTMS was delivered to the {region} at {c} using neuronavigation.",
            f"The stimulation target was the {region} {wrap(c)}, localised for each participant.",
        ])
    elif role == "prior_study":
        text = rng.choice([
            f"This location is close to the peak reported by {author} et al. ({year}) in the {region} {wrap(c)}.",
            f"Previous work found a similar effect in the {region} ({c}; {author} et al., {year}).",
        ])
    else:  # figure
        text = rng.choice([f"Crosshairs are placed at {c}.", f"Sagittal and coronal slices are shown at {c}."])
    return text, [{"xyz": xyz, "role": role, "stat": None, "cluster": None, "analysis": f"{region} {role}"}]


def lure_sentence(rng):
    a = rng.randint(3, 60)
    return rng.choice([
        f"These findings agree with earlier reports [ {a} , {a + rng.randint(2, 9)} , {a + rng.randint(10, 20)} ].",
        f"Peak effects were described previously [ {a} , {a + 3} – {a + 5} ].",
        f"Images were resampled to a voxel size of x = {rng.choice([2, 3, 3.64])}, y = {rng.choice([2, 3, 3.64])}, z = {rng.choice([2, 3, 3.26])} mm.",
        f"Accuracy was {rng.randint(70, 95)}% across blocks ({rng.randint(60, 80)}, {rng.randint(80, 90)}, {rng.randint(90, 99)}).",
        f"The docking grid box was centred at ({rng.uniform(-20, 20):.1f}, {rng.uniform(-20, 20):.1f}, {rng.uniform(-20, 20):.1f}) within the binding pocket.",
    ])


def passage(rng, V):
    """A context: a filler sentence, one or two coordinate sentences, a filler sentence."""
    _PASSAGE_SPACE[0] = rng.choice(["MNI", "MNI", "TAL", None])
    kind = rng.choices(
        ["result", "result_roi", "result_legend", "roi", "seed", "target", "prior_study", "figure", "mixed", "lure"],
        weights=[22, 8, 7, 10, 8, 5, 12, 5, 13, 10])[0]
    sents, points, lures = [], [], []
    if kind == "lure":
        lures.append(lure_sentence(rng)); sents.append(lures[-1])
    elif kind in ("result", "result_roi", "result_legend"):
        t, p = result_sentence(rng, V, rng.choice([1, 1, 2, 3, 4]), in_roi=kind == "result_roi", legend=kind == "result_legend")
        sents.append(t); points += p
    elif kind == "mixed":
        first = rng.choice(["seed", "roi", "prior_study"])
        t, p = roi_sentence(rng, V, first); sents.append(t); points += p
        t, p = result_sentence(rng, V, rng.choice([1, 2, 3])); sents.append(t); points += p
        if rng.random() < 0.3:
            lures.append(lure_sentence(rng)); sents.append(lures[-1])
    else:
        t, p = roi_sentence(rng, V, kind); sents.append(t); points += p
    text = " ".join([rng.choice(V["fillers"])] + sents + [rng.choice(V["fillers"])])
    space = _PASSAGE_SPACE[0]
    word = SPACE_WORD[space]
    if space and points and word not in " ".join(sents):
        # Name the space once, in the passage's own words, when no coordinate did.
        text = text.replace(sents[0], sents[0].rstrip(".") + f" ({word} coordinates).", 1)
    if not points or (word and word not in text):
        space = None
    return {"text": text, "space": space, "points": points, "kind": kind, "lures": lures, "label_source": "synthetic"}


def main():
    silver_path, gold_path, out_dir, n_train, n_test = sys.argv[1], sys.argv[2], Path(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5])
    gold_articles = {json.loads(l)["article_id"] for l in open(gold_path)}
    silver = [json.loads(l) for l in open(silver_path)]
    assert not gold_articles & {r["article_id"] for r in silver}, "silver overlaps the gold set"
    rng = random.Random(20261002)
    regions, contrasts, fillers = harvest(silver)
    vocab = {}
    for name, items in (("regions", regions), ("contrasts", contrasts), ("fillers", fillers)):
        tr, te = halves(items, rng)
        vocab.setdefault("train", {})[name] = tr
        vocab.setdefault("test", {})[name] = te
    out_dir.mkdir(parents=True, exist_ok=True)
    for split, n in (("train", n_train), ("test", n_test)):
        r = random.Random(f"{split}-20261002")
        with (out_dir / f"synthetic_{split}.jsonl").open("w") as fh:
            for i in range(n):
                row = passage(r, vocab[split])
                row["id"] = f"synth-{split}-{i}"
                for p in row["points"]:
                    p.update(zip(("role", "anchor_kind", "from_prior_study"), V4_ROLE[p["role"]]))
                fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    (out_dir / "synthetic_vocab_sizes.json").write_text(json.dumps(
        {s: {k: len(v) for k, v in d.items()} for s, d in vocab.items()}, indent=1))
    print({s: {k: len(v) for k, v in d.items()} for s, d in vocab.items()})


if __name__ == "__main__":
    main()
