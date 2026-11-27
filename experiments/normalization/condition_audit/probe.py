"""Probe the production linker: heads -> MONDO (with candidates) -> ONVOC bridge; and ONVOC matches of MONDO labels."""
import json, sys
from pondie.normalization import medical_condition as mc
from pondie.vocabularies.mondo import load_mondo, onvoc_crosswalk
from pondie.vocabularies.onvoc import load_onvoc
from pondie.vocabularies.phrases import triage
vocab = load_mondo(); cw = onvoc_crosswalk(); onvoc = load_onvoc().scoped(("disorders",)); index = mc.Index(vocab).build()
heads = json.load(open(sys.argv[1]))
out = []
for h in heads:
    t = triage(h)
    found = mc.link(list(t.heads), vocab, index) if t.heads else {}
    row = {"text": h, "sentinel": t.sentinel, "heads": []}
    for hd in t.heads:
        node, method, score, others = found[hd]
        cands = [vocab.labels[n] for n in ([node] if node is not None else []) + list(others)][:4]
        b = mc.bridge(node, vocab, cw, onvoc)[:3] if node is not None else None
        row["heads"].append({"head": hd, "method": method, "score": round(score, 3), "cands": cands, "bridge": b})
    out.append(row)
for lab in sys.argv[2:]:
    c, m, o = onvoc.match(lab)
    out.append({"onvoc_match": lab, "concept": c.label if c else None, "method": m})
json.dump(out, open("probe_out.json", "w"), indent=1)
