"""Does the ported per-article code give the Astra build's rows? A sample of records against out_all2's stages.

    python experiments/normalization/parity.py OUT_ALL2 RECORDS CORPUS BIB FIXED N [SEED]

Compared, row by row for the sampled studies: taskcat pass one (tc/tasks, tc/contrasts), the citation route's
task refs (citations_p0.3/task_refs: resolved rids and p_lineage), the medical rows (medical_condition.jsonl:
role, MONDO, ONVOC, route), and pass two under the build's own final labels (final_v3/conditions, contrasts,
analyses) and its contrast documents (contrast_docs_v3). Prints the share identical per table and the first
differences.
"""
import json
import pickle
import random
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from corpus import astra_corpus  # noqa: E402
from ingestion_workflow.normalization import citations, contrasts, medical, records  # noqa: E402
from ingestion_workflow.normalization.taskcat.identity import CardBuilder  # noqa: E402


def by_study(path, sample, key="study"):
    out = defaultdict(list)
    with open(path) as f:
        for line in f:
            x = json.loads(line)
            s = x[key] if key in x else x["id"].split("/")[0]
            if s in sample:
                out[s].append(x)
    return out


def compare(name, ours, theirs, strip=()):
    same = tot = 0; shown = 0
    for s in sorted(set(ours) | set(theirs)):
        a = [{k: v for k, v in x.items() if k not in strip} for x in ours.get(s, [])]
        b = [{k: v for k, v in x.items() if k not in strip} for x in theirs.get(s, [])]
        a = json.loads(json.dumps(a)); b = json.loads(json.dumps(b))
        tot += 1; same += a == b
        if a != b and shown < 3:
            shown += 1
            for x, y in zip(a + [None] * len(b), b + [None] * len(a)):
                if x != y:
                    keys = sorted({*(x or {}), *(y or {})})
                    print(f"  {name} {s}: " + "; ".join(f"{k}: {json.dumps((x or {}).get(k))[:120]} != {json.dumps((y or {}).get(k))[:120]}"
                                                         for k in keys if (x or {}).get(k) != (y or {}).get(k)))
                    break
    print(f"{name:24s} {same}/{tot} studies identical")


def main(out_all2, recs, corpus, bib, fixed, n, seed=0):
    o = Path(out_all2)
    ids = sorted(p.name.split(".")[0] for p in Path(recs).glob("*.extraction.json"))
    random.Random(int(seed)).shuffle(ids)
    sample = set(ids[: int(n)])
    R, T, B = astra_corpus(recs, corpus, bib, only=sample)
    cb = CardBuilder(pickle.load(open(Path(fixed) / "routeB_tfidf.pkl", "rb")))
    sch = records.schemas()
    lineage = pickle.load(open(Path(fixed) / "citation_role_lineage_clf.pkl", "rb"))
    final = {(x["study"], x["task"]): x for x in map(json.loads, open(o / "task_labels.jsonl")) if x["study"] in sample}
    ours = defaultdict(lambda: defaultdict(list))
    for s in sorted(sample):
        r = R[s]
        rows, _ = records.read(r, s, cb, sch)
        ours["tc/tasks"][s] = rows["tasks"]; ours["tc/contrasts"][s] = rows["contrasts"]
        refs = citations.score_lineage(citations.task_refs(r, s, T[s], citations.bib_from_references(B.get(s))), lineage)
        ours["task_refs"][s] = [{"task": t["task"], "source": t["source"], "bib": t["bib"],
                                 "refs": [(x["rid"], x["p_lineage"]) for x in t["refs"]]} for t in refs]
        ours["medical"][s] = [{"group": x["group"], "text": x["text"], "role": x["role"], "mondo": x["mondo"] or None,
                               "onvoc_id": x["onvoc_id"] or None, "route": x["sentinel"] or x["method"]}
                              for x in medical.rows(medical.links(r, s, T[s]))]
        rows2, _ = records.read(r, s, cb, sch, final)
        for k in ("conditions", "contrasts", "analyses"):
            ours[f"final_v3/{k}"][s] = rows2[k]
        ours["docs"][s] = contrasts.documents(r, s, rows2["contrasts"], T[s])
    theirs = {"tc/tasks": by_study(o / "tc/tasks.jsonl", sample), "tc/contrasts": by_study(o / "tc/contrasts.jsonl", sample)}
    tr = by_study(o / "citations_p0.3/task_refs.jsonl", sample)
    theirs["task_refs"] = {s: [{"task": t["task"], "source": t["source"], "bib": t["bib"],
                                "refs": [(x["rid"], x.get("p_lineage")) for x in t["refs"]]} for t in v] for s, v in tr.items()}
    md = by_study(o / "medical_condition.jsonl", sample)
    theirs["medical"] = {s: [{"group": x["group"], "text": x["text"], "role": x["role"], "mondo": x["mondo"],
                              "onvoc_id": x["onvoc_id"], "route": x["route"]} for x in v] for s, v in md.items()}
    for k in ("conditions", "contrasts", "analyses"):
        theirs[f"final_v3/{k}"] = by_study(o / f"final_v3/{k}.jsonl", sample)
    theirs["docs"] = by_study(o / "contrast_docs_v3.jsonl", sample)
    added = {"final_v3/conditions": ("atoms_from_description", "atoms_from_stimulus")}   # the port's additions
    for name in theirs:
        compare(name, ours[name], theirs[name], strip=added.get(name, ()))


if __name__ == "__main__":
    main(*sys.argv[1:])
