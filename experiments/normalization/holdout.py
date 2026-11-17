"""Fit on most papers, assign the held-out rest one at a time, and compare with a full refit.

    python holdout.py split  RECORDS OUT [HOLDOUT=0.1]            -> OUT/heldout.txt (by hash of the article id)
    python holdout.py fit    RECORDS CORPUS BIB OUT NAME {part|all} -> OUT/fits/<fit_id>, OUT/<NAME>/ (corpus rows)
    python holdout.py assign RECORDS CORPUS BIB OUT FIT_DIR        -> OUT/assigned/ (held-out rows under the fit)
    python holdout.py score  OUT PART_NAME ALL_NAME [OUT_ALL2]     -> OUT/scores.json

Task labels: held-out tasks' canonical / family under the 90% fit (assigned) vs the 100% refit. Contrast sets:
held-out two-sided contrasts; pairwise co-membership of each held-out contrast with every other contrast
(precision, recall of the assigned world against the refit), ARI over the held-out contrasts (a contrast in no
set is its own singleton), and the share left unassigned. OUT_ALL2, given, also scores the 100% refit against
the Astra build it ports (labels and sets over every paper).
"""
import hashlib
import json
import os
import sys
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from corpus import astra_corpus  # noqa: E402
from ingestion_workflow.normalization import api as F  # noqa: E402

WORKERS = int(os.environ.get("WORKERS", 16))


def heldout(out):
    return set(Path(out, "heldout.txt").read_text().split())


def split(recs, out, frac=0.1):
    ids = sorted(p.name.split(".")[0] for p in Path(recs).glob("*.extraction.json"))
    cut = float(frac) * 2 ** 32
    h = [i for i in ids if int(hashlib.sha256(i.encode()).hexdigest()[:8], 16) < cut]
    Path(out).mkdir(parents=True, exist_ok=True)
    Path(out, "heldout.txt").write_text("\n".join(h) + "\n")
    print(f"{len(h)} of {len(ids)} held out")


def do_fit(recs, corpus, bib, out, name, which):
    ids = {p.name.split(".")[0] for p in Path(recs).glob("*.extraction.json")}
    if which == "part":
        ids -= heldout(out)
    R, T, B = astra_corpus(recs, corpus, bib, only=ids)
    fid = F.fit(R, T, B, Path(out) / "fits", workers=WORKERS, rows_dir=Path(out) / name,
                log=lambda m: print(m, flush=True))
    Path(out, name, "fit_id").write_text(fid + "\n")
    print(fid)


_A = {}


def _one(article):
    R, T, B, f = _A["R"], _A["T"], _A["B"], _A["fit"]
    got = F.assign(F.body_of(R[article]), article, T.get(article) or "", B.get(article), f, with_medical=False)
    return ([x for x in got["labels"].values()],
            [{"id": i, "set": a["set"] if a else None, "orient": a["orient"] if a else None,
              "similarity": a["similarity"] if a else None} for i, a in got["assigned"].items()])


def assign(recs, corpus, bib, out, fit_dir):
    import multiprocessing as mp

    H = heldout(out)
    R, T, B = astra_corpus(recs, corpus, bib, only=H)
    f = F.load(str(Path(fit_dir).resolve()))
    f.encoder  # loaded before the fork
    _A.update(R=R, T=T, B=B, fit=f)
    d = Path(out) / "assigned"; d.mkdir(parents=True, exist_ok=True)
    with ProcessPoolExecutor(WORKERS, mp_context=mp.get_context("fork")) as ex, \
            open(d / "task_labels.jsonl", "w") as fl, open(d / "contrast_sets.jsonl", "w") as fs:
        for k, (labs, sets) in enumerate(ex.map(_one, sorted(H), chunksize=8)):
            for x in labs:
                fl.write(json.dumps(x) + "\n")
            for x in sets:
                fs.write(json.dumps(x) + "\n")
            if k % 500 == 0:
                print(k, flush=True)


def jl(p):
    with open(p) as f:
        return [json.loads(line) for line in f if line.strip()]


def pairs(groups, is_h):
    """Unordered pairs inside each group with at least one held-out member."""
    n = 0
    for members in groups.values():
        h = sum(1 for m in members if is_h(m)); o = len(members) - h
        n += h * (h - 1) // 2 + h * o
    return n


def co_membership(assigned, refit, H_ids):
    """assigned / refit: contrast id -> set or None, over the same contrast ids. Precision / recall of the pairs."""
    is_h = H_ids.__contains__
    ga, gr = defaultdict(list), defaultdict(list)
    for i, s in assigned.items():
        if s:
            ga[s].append(i)
    for i, s in refit.items():
        if s:
            gr[s].append(i)
    both = defaultdict(list)
    for i in assigned:
        if assigned[i] and refit.get(i):
            both[(assigned[i], refit[i])].append(i)
    na, nr, nb = pairs(ga, is_h), pairs(gr, is_h), pairs(both, is_h)
    return {"pairs_assigned": na, "pairs_refit": nr, "pairs_both": nb,
            "precision": round(nb / na, 4) if na else None, "recall": round(nb / nr, 4) if nr else None}


def ari(a, b, ids):
    from sklearn.metrics import adjusted_rand_score

    la = [a.get(i) or f"single:{i}" for i in ids]; lb = [b.get(i) or f"single:{i}" for i in ids]
    return round(float(adjusted_rand_score(la, lb)), 4)


def labels_agree(x, y, keys):
    out = {"tasks": len(keys)}
    for field in ("canonical", "family"):
        out[f"{field}_agree"] = round(sum(x[k][field] == y[k][field] for k in keys) / len(keys), 4) if keys else None
    by = Counter(x[k]["decided_by"] for k in keys)
    out["by_route"] = {r: {"n": n, "canonical_agree": round(sum(x[k]["canonical"] == y[k]["canonical"] for k in keys
                                                                if x[k]["decided_by"] == r) / n, 4)}
                       for r, n in by.most_common()}
    return out


def score(out, part, full, out_all2=None):
    out = Path(out); H = heldout(out)
    lab = lambda rows: {(x["study"], x["task"]): x for x in rows}  # noqa: E731
    A_l, P_l, R_l = lab(jl(out / "assigned/task_labels.jsonl")), lab(jl(out / part / "task_labels.jsonl")), \
        lab(jl(out / full / "task_labels.jsonl"))
    res = {"heldout_papers": len(H), "fits": {n: (out / n / "fit_id").read_text().strip() for n in (part, full)}}
    keys = sorted(k for k in A_l if k in R_l)
    res["task_labels_heldout"] = labels_agree(A_l, R_l, keys)
    fit_keys = sorted(k for k in P_l if k in R_l)
    res["task_labels_fit_papers"] = labels_agree(P_l, R_l, fit_keys)
    A_s = {x["id"]: x["set"] for x in jl(out / "assigned/contrast_sets.jsonl")}
    P_s = {x["id"]: x["set"] for x in jl(out / part / "contrast_sets.jsonl")}
    R_s = {x["id"]: x["set"] for x in jl(out / full / "contrast_sets.jsonl")}
    H_ids = {i for i in A_s}
    world = {**P_s, **A_s}                            # the 90% fit's sets plus the held-out contrasts assigned to them
    common = [i for i in world if i in R_s]
    res["contrast_sets_heldout"] = {
        "contrasts": len(H_ids), "unassigned_share": round(sum(A_s[i] is None for i in H_ids) / len(H_ids), 4),
        "refit_not_in_set_share": round(sum(R_s.get(i) is None for i in H_ids) / len(H_ids), 4),
        **co_membership({i: world[i] for i in common}, {i: R_s[i] for i in common}, H_ids),
        "ari_heldout": ari(A_s, R_s, sorted(H_ids)),
        "ari_heldout_in_refit_sets": ari(A_s, R_s, sorted(i for i in H_ids if R_s.get(i)))}
    fit_ids = sorted(i for i in P_s if i in R_s)
    res["contrast_sets_fit_papers"] = {"contrasts": len(fit_ids), "ari": ari(P_s, R_s, fit_ids)}
    if out_all2:
        o = Path(out_all2)
        B_l = lab(jl(o / "task_labels.jsonl"))
        res["refit_vs_astra_build"] = {"task_labels": labels_agree(R_l, B_l, sorted(k for k in R_l if k in B_l))}
        B_s = {}
        for s in jl(o / "contrast_s5_v3/s5_sets.jsonl"):
            for m in s["members"]:
                B_s[m["id"]] = s["set"]
        ids = sorted(R_s)
        res["refit_vs_astra_build"]["contrast_sets"] = {
            "contrasts": len(ids), "ari": ari(R_s, B_s, ids),
            "in_set_refit": sum(1 for i in ids if R_s[i]), "in_set_build": sum(1 for i in ids if B_s.get(i))}
    (out / "scores.json").write_text(json.dumps(res, indent=1) + "\n")
    print(json.dumps(res, indent=1))


if __name__ == "__main__":
    cmd, *a = sys.argv[1:]
    {"split": split, "fit": do_fit, "assign": assign, "score": score}[cmd](*a)
