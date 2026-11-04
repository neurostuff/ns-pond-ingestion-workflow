"""Pooled contrast sets: what a contrast means across papers (the Astra build's astra_contrast_docs.py,
astra_contrast_match.py S2, astra_contrast_s5.py and astra_contrast_groups.py), split into a fit and an assignment.

Similarity of two contrasts = the better of
    same orientation  min(cos(target, target), cos(baseline, baseline))
    flipped           min(cos(target, baseline), cos(baseline, target))
(MiniLM on condition names + descriptions), each zeroed by its cannot-links: contradicting atoms on one axis in that
orientation, different phases, or different tasks (canonical differs AND task-description cosine < TASK_COS).
Lexical matches (S2: Jaccard >= .5 on each side's stemmed words + atoms, passing the S5 rule) are pulled up to T5.

fit     average-linkage clustering within each task family, cut at 1 - T5, so a set's members are similar on
        average, not through a chain; sets need >= 2 papers. Each member's `orient` is its orientation relative to
        the set's medoid, the set stated in its majority direction.
assign  a new contrast joins the same-family set whose average similarity to its members, after cannot-links, is
        >= T5 (the linkage's own merge rule), oriented against the medoid; else it stays unassigned until a refit.
"""
import json
import re
from collections import defaultdict
from itertools import combinations
from pathlib import Path

import numpy as np

from .taskcat.conditions import normalize_condition

T5 = 0.60
TASK_COS = 0.50
#: The build's lexical (S2) pass compared the first 3000 contrasts of a family only; the fit keeps that cap.
S2_CAP = 3000
EMBEDDING_MODEL = "sentence-transformers/all-MiniLM-L6-v2"
STOP = set("""a an the of and or with for to in on at by from vs versus than minus condition conditions trial trials block
blocks task tasks stimulus stimuli event events period phase run runs session type types all each other no non""".split())
BASE = {"control_state", "rest", "fixation"}


def v(x):
    return x.get("value") if isinstance(x, dict) else x


def text(x):
    x = v(x)
    return "; ".join(map(str, x)) if isinstance(x, list) else (str(x) if x is not None else "")


def spans(x):
    return [(s["start_char"], s["end_char"]) for st in ((x or {}).get("evidence") or {}).get("sets") or []
            for s in st.get("spans") or [] if s.get("start_char") is not None]


# ------------------------------------------------------------------ documents (astra_contrast_docs.py)
def documents(record, study, contrasts, body):
    """One document per directed contrast of one record: each side's condition names + descriptions + stimulus
    content, the task name/description, and the paper's text around the analysis evidence (`excerpt`)."""
    body = body or ""
    A = {a.get("local_id"): a for a in record.get("analyses") or [] if isinstance(a, dict)}
    C, T = {}, {}
    for t in record.get("tasks") or []:
        T[t.get("local_id")] = t
        for c in t.get("conditions") or []:
            C[c.get("local_id")] = c
    out = []
    for k in contrasts:
        if k["shape"] == "undirected":
            continue
        a = A.get(k["analysis"]) or {}
        sp = sorted(set(spans(a.get("definition")) + spans(a.get("name"))))[:3]
        exc = " […] ".join(body[max(0, s - 250):e + 250].replace("\n", " ") for s, e in sp)[:2500]

        def side(items):
            res = []
            for s in items:
                c = C.get(s["condition"]) or {}
                res.append({"name": s["name"], "level": s.get("level"), "kind": s["kind"], "atoms": s["atoms"],
                            "phases": s.get("phases", []), "token": s.get("token"),
                            "description": text(c.get("description"))[:400],
                            "stimulus": text(c.get("stimulus_content"))[:200]})
            return res
        t = T.get(k["task"]) or {}
        out.append({
            "id": f"{study}/{k['analysis']}", "study": study, "analysis": k["analysis"], "key": k["key"],
            "family": k["family"], "canonical": k["canonical"], "shape": k["shape"], "sign": k["sign"],
            "effect_kind": k["effect_kind"], "tested_across": k.get("tested_across"),
            "covariates": k.get("covariates"), "measure_family": k.get("measure_family"),
            "measure_type": k.get("measure_type"), "outcome": k.get("outcome"),
            "spatial_scope": k.get("spatial_scope"), "population": k.get("population"),
            "task_name": text(t.get("name")), "task_description": text(t.get("description"))[:500],
            "analysis_name": k["name"], "definition": text(a.get("definition"))[:600],
            "target": side(k["target"]), "baseline": side(k["baseline"]), "excerpt": exc})
    return out


# ------------------------------------------------------------------ per-contrast features
def stem(w):
    for suf in ("ings", "ing", "ies", "es", "s", "ed"):
        if w.endswith(suf) and len(w) - len(suf) >= 3:
            return w[: -len(suf)]
    return w


def words(side):
    out = set()
    for s in side:
        out |= {a for a in s["atoms"]}
        out |= {stem(w) for w in re.findall(r"[a-z][a-z0-9+-]+", s["name"].lower()) if w not in STOP and len(w) > 2}
        if s["kind"] in BASE:
            out.add("KIND:" + s["kind"])
    return out


def jac(a, b):
    return len(a & b) / len(a | b) if a and b else 0.0


def side_text(side):
    return " ; ".join(f"{s['name']}: {s['description'][:200]}" if s["description"] else s["name"] for s in side) or "none"


def side_axes(side):
    out = {}
    for s in side:
        for a in s["atoms"]:
            ax, val = a.split(":", 1); out.setdefault(ax, val)
    return out


def atoms(side):
    return {a for s in side for a in s["atoms"]}


def features(d):
    """What the similarity and its cannot-links read of one two-sided document, JSON-ready."""
    ta, ba = atoms(d["target"]), atoms(d["baseline"])
    return {"id": d["id"], "study": d["study"], "family": d["family"] or "", "canonical": d["canonical"],
            "sign": d["sign"],
            "target_axes": side_axes(d["target"]), "baseline_axes": side_axes(d["baseline"]),
            "phases": sorted({p for s in d["target"] + d["baseline"] for p in normalize_condition(s["name"])["phases"]}),
            "target_atoms": sorted(ta), "side_only": bool(ta | ba) and all(a.startswith("side:") for a in ta | ba),
            "target_words": sorted(words(d["target"])), "baseline_words": sorted(words(d["baseline"])),
            "task_key": [d["study"], d["task_name"]], "task_text": d["task_name"] + ". " + d["task_description"]}


def two_sided(docs):
    return [d for d in docs if d["target"] and d["baseline"]]


class Encoder:
    """MiniLM at one hub revision, on CPU. `revision` None takes the cached (or the hub's current) snapshot; the
    snapshot's commit is `self.revision`, which a fit records so assignment encodes with the same weights."""

    def __init__(self, revision=None):
        from huggingface_hub import snapshot_download
        from sentence_transformers import SentenceTransformer

        try:                            # a cached snapshot needs no network, even at a pinned revision
            path = Path(snapshot_download(EMBEDDING_MODEL, revision=revision, local_files_only=True))
        except Exception:  # noqa: BLE001 - not cached: fetch it
            path = Path(snapshot_download(EMBEDDING_MODEL, revision=revision))
        self.revision = path.name
        self.model = SentenceTransformer(str(path), device="cpu")

    def __call__(self, texts):
        if not texts:
            return np.zeros((0, 384), np.float32)
        return self.model.encode(list(texts), batch_size=256, normalize_embeddings=True,
                                 show_progress_bar=False).astype(np.float32)

    def sides(self, docs):
        return self([side_text(d["target"]) for d in docs]), self([side_text(d["baseline"]) for d in docs])


# ------------------------------------------------------------------ cannot-links
def conflict(A, B):
    """[n, m] bool: some axis has a value on both sides and the values differ. A, B: per-member {axis: value}."""
    out = np.zeros((len(A), len(B)), bool)
    if not len(A) or not len(B):
        return out
    for ax in set().union(*A, *B):
        codes = {}
        a = np.array([codes.setdefault(x.get(ax), len(codes)) if x.get(ax) is not None else -1 for x in A])
        b = np.array([codes.setdefault(x.get(ax), len(codes)) if x.get(ax) is not None else -1 for x in B])
        out |= (a[:, None] >= 0) & (b[None, :] >= 0) & (a[:, None] != b[None, :])
    return out


def lexical_ok(fi, fj):
    """The S5 rule a lexical (S2) match must pass: same family, two papers, neither only a side comparison, the
    same phases, and no target atom contradicting the other's on its axis."""
    if not fi["family"] or fi["family"] != fj["family"] or fi["study"] == fj["study"] or fi["side_only"] \
            or fj["side_only"] or fi["phases"] != fj["phases"]:
        return False
    return not any(x.split(":")[0] == y.split(":")[0] and x != y for x in fi["target_atoms"] for y in fj["target_atoms"])


def lexical(fi, fj):
    """S2 same-orientation: Jaccard >= .5 on both sides' words."""
    return jac(set(fi["target_words"]), set(fj["target_words"])) >= 0.5 and \
        jac(set(fi["baseline_words"]), set(fj["baseline_words"])) >= 0.5


def similarity(F_a, Et_a, Eb_a, Ek_a, F_b, Et_b, Eb_b, Ek_b, T=T5, TT=TASK_COS):
    """(same, flip) [n_a, n_b] after the cannot-links (the lexical pull is the caller's)."""
    same = np.minimum(Et_a @ Et_b.T, Eb_a @ Eb_b.T).astype(np.float32)
    flip = np.minimum(Et_a @ Eb_b.T, Eb_a @ Et_b.T).astype(np.float32)
    TA = [f["target_axes"] for f in F_a]; BA = [f["baseline_axes"] for f in F_a]
    TB = [f["target_axes"] for f in F_b]; BB = [f["baseline_axes"] for f in F_b]
    same[conflict(TA, TB) | conflict(BA, BB)] = 0
    flip[conflict(TA, BB) | conflict(BA, TB)] = 0
    pa = [tuple(f["phases"]) for f in F_a]; pb = [tuple(f["phases"]) for f in F_b]
    pid = {p: k for k, p in enumerate(sorted(set(pa) | set(pb)))}
    diff_phase = np.array([pid[p] for p in pa])[:, None] != np.array([pid[p] for p in pb])[None, :]
    same[diff_phase] = 0; flip[diff_phase] = 0
    if TT > 0:
        ca = np.array([f["canonical"] or f"none:a{k}" for k, f in enumerate(F_a)], dtype=object)
        cb = np.array([f["canonical"] or f"none:b{k}" for k, f in enumerate(F_b)], dtype=object)
        other_task = ((Ek_a @ Ek_b.T) < TT) & (ca[:, None] != cb[None, :])
        same[other_task] = 0; flip[other_task] = 0
    return same, flip


# ------------------------------------------------------------------ fit
def fit_sets(docs, encoder, T=T5, TT=TASK_COS, log=print):
    """The contrast-set fit over a corpus's documents. Returns (sets, members, arrays):

    sets     [{set, family, label, medoid, n_papers, n_contrasts}]
    members  [features + {set, orient}], in the order of `arrays`' rows
    arrays   {"target", "baseline", "task"}: float32 MiniLM embeddings of each member"""
    from scipy.cluster.hierarchy import fcluster, linkage
    from scipy.spatial.distance import squareform

    D = two_sided(docs)
    F = [features(d) for d in D]
    Et, Eb = encoder.sides(D)
    # one task-text embedding per (study, task name), as the build encoded them
    uniq = {}
    for f in F:
        uniq.setdefault(tuple(f["task_key"]), f["task_text"])
    keys_ = list(uniq)
    E_uniq = encoder([uniq[k] for k in keys_]); row = {k: i for i, k in enumerate(keys_)}
    Ek = E_uniq[[row[tuple(f["task_key"])] for f in F]] if F else np.zeros((0, 384), np.float32)
    byfam = defaultdict(list)
    for i, f in enumerate(F):
        if f["family"]:
            byfam[f["family"]].append(i)
    sets, member_rows = [], []
    for fam, ids in sorted(byfam.items(), key=lambda kv: -len(kv[1])):
        ids = np.array(ids); n = len(ids)
        if n < 2:
            continue
        Ff = [F[i] for i in ids]
        same, flip = similarity(Ff, Et[ids], Eb[ids], Ek[ids], Ff, Et[ids], Eb[ids], Ek[ids], T, TT)
        for a, b in combinations(range(min(n, S2_CAP)), 2):
            if lexical(Ff[a], Ff[b]) and lexical_ok(Ff[a], Ff[b]):
                for x, y in ((a, b), (b, a)):
                    if same[x, y] > 0:
                        same[x, y] = max(same[x, y], T)
        S = np.maximum(same, flip); np.fill_diagonal(S, 1)
        Dm = np.clip(1 - S, 0, 1); Dm = (Dm + Dm.T) / 2; np.fill_diagonal(Dm, 0)
        lab = fcluster(linkage(squareform(Dm, checks=False), "average"), t=1 - T, criterion="distance")
        del Dm
        groups = defaultdict(list)
        for k, l in enumerate(lab):
            groups[l].append(k)
        kept = 0
        for l, ks in groups.items():
            papers = {Ff[k]["study"] for k in ks}
            if len(papers) < 2:
                continue
            sub = S[np.ix_(ks, ks)]; med = ks[int(sub.mean(1).argmax())]
            orient = {k: (1 if same[k, med] >= flip[k, med] else -1) for k in ks}
            orient[med] = 1
            m = D[ids[med]]
            t, b = (" + ".join(s["name"] for s in m[x])[:60] for x in ("target", "baseline"))
            # the set stated in its majority direction; a tie goes by label order, so the label does not hang on
            # which member is the medoid
            n_rev = sum(o == -1 for o in orient.values())
            flip_set = n_rev > len(ks) / 2 or (n_rev == len(ks) / 2 and f"{b} > {t}" < f"{t} > {b}")
            if flip_set:
                orient = {k: -o for k, o in orient.items()}
            sid = f"{fam}#{l}"
            sets.append({"set": sid, "family": fam, "label": f"{b} > {t}" if flip_set else f"{t} > {b}",
                         "medoid": Ff[med]["id"], "n_papers": len(papers), "n_contrasts": len(ks)})
            for k in ks:
                member_rows.append((int(ids[k]), sid, orient[k]))
            kept += 1
        log(f"{fam:34s} {n:6d} contrasts -> {kept} pooled sets")
    rows = [r for r, _, _ in member_rows]
    members = [{**F[r], "set": sid, "orient": o} for r, sid, o in member_rows]
    arrays = {"target": Et[rows], "baseline": Eb[rows], "task": Ek[rows]}
    return sets, members, arrays


def save_sets(out, sets, members, arrays):
    out = Path(out); out.mkdir(parents=True, exist_ok=True)
    with open(out / "sets.jsonl", "w") as f:
        for s in sets:
            f.write(json.dumps(s, sort_keys=True) + "\n")
    with open(out / "members.jsonl", "w") as f:
        for m in members:
            f.write(json.dumps(m, sort_keys=True) + "\n")
    for k, a in arrays.items():
        np.save(out / f"{k}.npy", a.astype(np.float32))


class SetFit:
    """A saved contrast-set fit, loaded for assignment."""

    def __init__(self, path, T=T5, TT=TASK_COS):
        path = Path(path)
        self.T, self.TT = T, TT
        self.sets = {s["set"]: s for s in map(json.loads, open(path / "sets.jsonl"))}
        self.members = [json.loads(line) for line in open(path / "members.jsonl")]
        self.Et, self.Eb, self.Ek = (np.load(path / f"{k}.npy") for k in ("target", "baseline", "task"))
        self.by_family = defaultdict(list)
        for i, m in enumerate(self.members):
            self.by_family[m["family"]].append(i)
        self.index = {m["id"]: i for i, m in enumerate(self.members)}

    def assign(self, docs, encoder):
        """docs (one article's two-sided documents) -> {doc id: assignment or None}.

        An assignment: {set, label, orient, effective_sign, similarity, n_members}. `orient` is +1 when the
        contrast runs the way the set is stated, -1 when it is the set's reverse."""
        D = two_sided(docs)
        if not D:
            return {}
        F = [features(d) for d in D]
        Et, Eb = encoder.sides(D)
        Ek = encoder([f["task_text"] for f in F])
        out = {}
        for i, f in enumerate(F):
            ids = np.array(self.by_family.get(f["family"]) or [], dtype=int)
            if not f["family"] or not len(ids):
                out[f["id"]] = None
                continue
            M = [self.members[j] for j in ids]
            same, flip = similarity([f], Et[i:i + 1], Eb[i:i + 1], Ek[i:i + 1], M, self.Et[ids], self.Eb[ids],
                                    self.Ek[ids], self.T, self.TT)
            same, flip = same[0], flip[0]
            for k, m in enumerate(M):
                if same[k] > 0 and lexical(f, m) and lexical_ok(f, m):
                    same[k] = max(same[k], self.T)
            S = np.maximum(same, flip)
            by_set = defaultdict(list)
            for k, m in enumerate(M):
                by_set[m["set"]].append(k)
            best, score = None, -1.0
            for sid, ks in sorted(by_set.items()):
                s = float(S[ks].mean())
                if s > score:
                    best, score = sid, s
            if best is None or score < self.T:
                out[f["id"]] = None
                continue
            med = self.index[self.sets[best]["medoid"]]
            k_med = int(np.where(ids == med)[0][0])
            orient = (1 if same[k_med] >= flip[k_med] else -1) * self.members[med]["orient"]
            out[f["id"]] = {"set": best, "label": self.sets[best]["label"], "orient": orient,
                            "effective_sign": f["sign"] * orient, "similarity": round(score, 4),
                            "n_members": len(by_set[best])}
        return out
