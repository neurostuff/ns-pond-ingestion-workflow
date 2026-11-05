"""Where normalize_record's time goes, per stage, on real records, and what it wrote (for a before/after check).

    python profile_assign.py RECORDS CORPUS BIB FIT_DIR IDS_FILE N OUT_JSON

Loads the fit once (timed), then runs each stage of `api.assign` + the sidecar writers for N records listed in
IDS_FILE, in one process. OUT_JSON gets per-stage seconds per paper, the cProfile top functions, and the sha256
of every normalized record, so two versions of the code can be shown to write the same records.
"""
import copy
import cProfile
import hashlib
import io
import json
import pstats
import sys
import time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from corpus import astra_corpus  # noqa: E402

from ingestion_workflow.normalization import api, citations, contrasts, labels, medical, records, sidecars  # noqa: E402


def stages(r, article, text, bib, f, t):
    def timed(name, fn, *a):
        s = time.perf_counter(); out = fn(*a); t[name] += time.perf_counter() - s
        return out
    links = timed("medical", medical.links, r, article, text)
    if hasattr(f.cb, "memo"):
        f.cb.memo = {}
    rows, _ = timed("read pass one", records.read, r, article, f.cb, f.sch)
    refs = timed("citation refs", lambda: citations.score_lineage(
        citations.task_refs(r, article, text, citations.bib_from_references(bib)), f.lineage))
    route_c = timed("citation vote", lambda: {(x["study"], x["task"]): citations.vote(x, f.index) for x in refs})
    cn = defaultdict(list)
    for k in rows["contrasts"]:
        cn[(k["study"], k["task"])].append(k["name"])
    final = timed("labels", lambda: {(x["study"], x["task"]): labels.decide(
        x, cn[(x["study"], x["task"])], route_c.get((x["study"], x["task"])), f.cb.L, f.classifier) for x in rows["tasks"]})
    rows2, _ = timed("read pass two", records.read, r, article, f.cb, f.sch, final)
    if hasattr(f.cb, "memo"):
        f.cb.memo = None
    docs = timed("contrast docs", contrasts.documents, r, article, rows2["contrasts"], text)
    assigned = timed("set assignment", f.sets.assign, docs, f.encoder)
    timed("sidecars", lambda: (sidecars.write_medical(r, links, f.fit_id),
                               sidecars.write_tasks(r, {k: x for (_s, k), x in final.items()}, f.fit_id),
                               sidecars.write_conditions(r, rows2["conditions"], f.fit_id),
                               sidecars.write_contrasts(r, rows2["contrasts"], assigned, f.fit_id, f.sets.T)))
    return r


def main(recs, corpus, bib, fit_dir, ids_file, n, out):
    ids = sorted(Path(ids_file).read_text().split())[: int(n)]
    R, T, B = astra_corpus(recs, corpus, bib, only=set(ids))
    s = time.perf_counter()
    f = api.load(str(Path(fit_dir).resolve())); f.encoder; medical.vocabularies()
    load = time.perf_counter() - s
    t = defaultdict(float); digests = {}; per_paper = []
    prof = cProfile.Profile()
    for i in ids:
        r = copy.deepcopy(R[i]); text = T[i]; b = B.get(i)
        s = time.perf_counter()
        prof.enable(); stages(api.body_of(r), i, text, b, f, t); prof.disable()
        per_paper.append(time.perf_counter() - s)
        digests[i] = hashlib.sha256(json.dumps(r, sort_keys=True).encode()).hexdigest()
    buf = io.StringIO()
    pstats.Stats(prof, stream=buf).sort_stats("cumulative").print_stats(35)
    total = sum(per_paper)
    res = {"papers": len(ids), "load_s": round(load, 1), "total_s": round(total, 1),
           "per_paper_s": {"mean": round(total / len(ids), 2), "max": round(max(per_paper), 2)},
           "stages_s_per_paper": {k: round(v / len(ids), 3) for k, v in sorted(t.items(), key=lambda kv: -kv[1])},
           "record_sha256": digests}
    Path(out).write_text(json.dumps(res, indent=1) + "\n")
    print(json.dumps({k: v for k, v in res.items() if k != "record_sha256"}, indent=1))
    print(buf.getvalue()[:9000])


if __name__ == "__main__":
    main(*sys.argv[1:])
