"""The citation route: what a task cites says which paradigm it is (the Astra build's astra_citations.py).

1 markers   the record's `task.references` (verbatim markers tied to the task) with their evidence sentence; a
            marker without evidence is located in the text; tasks with none fall back to the harvest (markers
            within the sentence naming the task and the next)
2 resolve   each marker against the paper's bibliography: ns-pond's `references` artifact, its links first
            (`taskcat.citations.resolve_with_bib`)
3 role      the fixed citation-role classifier on the citing sentence, marker masked as [CIT] -> p_lineage
4 index     ref -> {canonical: papers}, pooled over TaskExtractor mentions (lexical routes only) and the fit
            corpus's tasks the lexicon names (A:name); a ref is keyed by every id it carries (doi, pmid,
            author-year-title). Built once by `build_index`, saved with the fit.
5 vote      per lineage ref (p_lineage >= THR), own paper left out: purity >= 0.5 votes purity * log(1 + papers);
            + ORIGIN_W when the ref is a curated or discovered origin of a task;
            + the labels of a cited corpus paper, weight p_lineage
"""
import json
import math
import re
from collections import Counter, defaultdict

from .taskcat.citations import find_citations, resolve_with_bib

#: The build's PLIN (astra_build_combined.sh); the script's own default was 0.5.
THR = 0.3
ORIGIN_W = 2.0
REST = {"hedtsk_rest", "hedtsk_fixate", None}
STOP = {"task", "paradigm", "test", "the", "a", "an", "of", "and", "fmri", "functional", "mri", "experiment"}
SENT = re.compile(r"(?<=[.!?])\s+(?=[A-Z(\[])")


def v(x):
    return x.get("value") if isinstance(x, dict) else x


def keys(r):
    """Every id a reference carries; matching any one joins two citations of it."""
    out = set()
    if r.get("doi"):
        out.add("doi:" + str(r["doi"]).lower().strip())
    if r.get("pmid"):
        out.add("pmid:" + str(r["pmid"]).strip())
    fa = re.sub(r"[^a-z]", "", (r.get("first_author") or "").lower()); t = re.sub(r"[^a-z0-9]", "", (r.get("title") or "").lower())[:40]
    if fa and r.get("year") and len(t) > 10:
        out.add(f"ay:{fa}:{r['year']}:{t}")
    return out


def bib_from_references(payload):
    """ns-pond's `references` artifact payload -> the bibliography shape `resolve_with_bib` reads.

    An entry's printed position is its `order`, its first listed surname its `first_author`; each in-text citation
    whose marker is in the text is an `xref`: the marker's text and the entries the source linked it to."""
    if not payload or not payload.get("references"):
        return None
    refs = [{"rid": r["id"], "order": r.get("position"), "first_author": (r.get("authors") or [None])[0],
             "year": str(r["year"]) if r.get("year") else None, "title": r.get("title"), "doi": r.get("doi"),
             "pmid": r.get("pmid")} for r in payload["references"]]
    xrefs = [{"text": (c.get("text_span") or {}).get("text") or "", "rids": list(c.get("references") or [])}
             for c in payload.get("citations") or [] if c.get("marker_in_text", True)]
    return {"refs": refs, "xrefs": [x for x in xrefs if x["text"] and x["rids"]]}


def name_variants(name):
    n = re.sub(r"\s*\([^)]*\)", "", name or "").strip()
    out = {n}
    m = re.search(r"\(([A-Za-z0-9\-/]{2,10})\)", name or "")
    if m:
        out.add(m.group(1))
    toks = [t for t in re.findall(r"[A-Za-z0-9\-/']+", n) if t.lower() not in STOP]
    if len(toks) >= 2:
        out.add(" ".join(toks[:3]))
    return [x for x in out if len(x) >= 3]


def harvest(name, text):
    """Markers in the sentence naming the task and the one after it."""
    vs = [x.lower() for x in name_variants(name)]
    got = {}
    for p in text.split("\n"):
        if len(p) < 40 or not any(x in p.lower() for x in vs):
            continue
        sents = SENT.split(p)
        for i, s in enumerate(sents):
            if any(x in s.lower() for x in vs):
                win = " ".join(sents[i:i + 2])
                for m in find_citations(win):
                    got.setdefault(m, win)
    return got


def locate(marker, name, text):
    """The citing sentence for a marker a record gives without evidence: the sentence containing the marker,
    preferring the occurrence nearest a mention of the task's name, plus the next sentence. "" when the marker is
    not in the text."""
    if not text or not marker:
        return ""
    pat = re.compile(r"\s*".join(re.escape(p) for p in marker.split()))
    hits = [m.start() for m in pat.finditer(text)]
    if not hits:
        return ""
    names = [m.start() for v_ in name_variants(name) for m in re.finditer(re.escape(v_), text, re.I)]
    pos = min(hits, key=lambda h: min((abs(h - n) for n in names), default=0))
    start = max(text.rfind(". ", 0, pos), text.rfind("\n", 0, pos)) + 1
    end = text.find(". ", pos)
    end = text.find(". ", end + 2) if end != -1 else -1
    return text[start:(end + 1 if end != -1 else pos + 400)].strip()[:600]


def evidence_text(ev):
    spans = [s.get("text") for st in ((ev or {}).get("evidence") or {}).get("sets") or [] for s in st.get("spans") or []]
    return " ".join(s for s in spans if s)


def task_refs(record, study, text, bib):
    """Each task's markers and the bibliography entries they resolve to. `bib` from `bib_from_references`."""
    text = text or ""
    rows = []
    for t in record.get("tasks") or []:
        name = str(v(t.get("name")) or "")
        ref = t.get("references") or {}
        marks = [str(m) for m in (v(ref) or []) if m]
        ctx = evidence_text(ref)
        if marks and not ctx:
            found = {m: locate(m, name, text) for m in marks}
        else:
            found = {m: ctx for m in marks}
        source = ("record" if ctx else "record+located") if found else None
        if not found:
            found = harvest(name, text); source = "harvest" if found else None
        refs = []
        for m, c in found.items():
            for g in resolve_with_bib(m, bib) if bib else []:
                refs.append({"marker": m, "rid": g.get("rid"), "doi": g.get("doi"), "pmid": g.get("pmid"),
                             "first_author": g.get("first_author"), "year": g.get("year"), "title": (g.get("title") or "")[:200],
                             "context": (c or "").replace(m, " [CIT] ")[:600]})
        rows.append({"study": study, "task": t.get("local_id"), "name": name, "source": source, "bib": bool(bib),
                     "n_markers": len(found), "refs": refs})
    return rows


def score_lineage(rows, lineage):
    """p_lineage on every resolved ref, in place. `lineage` the fixed (vectorizer, classifier) pair."""
    vec, clf = lineage
    li = list(clf.classes_).index("lineage")
    allrefs = [x for t in rows for x in t["refs"]]
    if allrefs:
        pr = clf.predict_proba(vec.transform([x["context"] for x in allrefs]))[:, li]
        for x, p in zip(allrefs, pr):                # no citing sentence, no role evidence: never lineage
            x["p_lineage"] = round(float(p), 4) if x["context"].replace("[CIT]", "").strip() else 0.0
    return rows


def build_index(rows, tasks, lineage, te_normalized, te_citations, discovered_origins, paper_ids):
    """The citation fit: ref key -> canonical -> papers, ref key -> origin tasks, and the cited corpus papers.

    rows: `task_refs` + `score_lineage` over the fit corpus; tasks: pass-one task rows; te_*: the TaskExtractor
    mentions (iterables of dicts); paper_ids: (paper, {"pmid", "doi"}) pairs, the corpus papers' own ids, so a
    cited ref can be told to be a corpus paper. Returns a JSON-ready dict."""
    vec, clf = lineage
    li = list(clf.classes_).index("lineage")
    index = defaultdict(lambda: defaultdict(set))
    plabs = defaultdict(set)                      # paper -> its lexically named, non-rest canonicals
    te = {r["mid"]: r for r in te_normalized}
    for c in te_citations:
        n = te.get(c["mid"]) or {}
        lab = n.get("canonical")
        if lab in REST or not str(n.get("route") or "").startswith("A"):
            continue
        plabs[c["dbid"]].add(lab)
        ctx = [x.get("context") or "" for x in c["refs"]]
        ps = clf.predict_proba(vec.transform(ctx))[:, li] if ctx else []
        for x, p in zip(c["refs"], ps):
            if p >= THR:
                for k in keys(x):
                    index[k][lab].add(c["dbid"])
    corpus_lab = {}
    for t in tasks:
        if t["route"] == "A:name" and t["canonical"] not in REST and t["family"] != "pseudo_tasks":
            corpus_lab[(t["study"], t["task"])] = t["canonical"]; plabs[t["study"]].add(t["canonical"])
    for t in rows:
        lab = corpus_lab.get((t["study"], t["task"]))
        if lab:
            for x in t["refs"]:
                if x.get("p_lineage", 0) >= THR:
                    for k in keys(x):
                        index[k][lab].add(t["study"])
    origin = defaultdict(set)
    for o in discovered_origins:
        for x in o["top_refs"]:
            if x.get("curated") or x.get("purity", 0) >= 0.8:
                for k in keys(x):
                    origin[k].add(o["task"])
    paper_of = {}
    for paper, ids in paper_ids:
        if paper in plabs:                        # a cited paper with no labels votes nothing
            for k in ("pmid", "doi"):
                if ids.get(k):
                    paper_of[f"{k}:{str(ids[k]).lower()}"] = paper
    return {"thr": THR, "origin_w": ORIGIN_W,
            "index": {k: {lab: sorted(ps) for lab, ps in d.items()} for k, d in index.items()},
            "origin": {k: sorted(s) for k, s in origin.items()},
            "paper_of": paper_of, "paper_labels": {p: sorted(s) for p, s in plabs.items() if s}}


def vote(t, fit):
    """One `task_refs` row (scored) -> its route-C answer against a saved index. The task's own paper is left out."""
    index, origin, paper_of, plabs = fit["index"], fit["origin"], fit["paper_of"], fit["paper_labels"]
    votes = Counter(); ev = []
    lin = [x for x in t["refs"] if x.get("p_lineage", 0) >= THR]
    for x in lin:
        ks = sorted(keys(x))                      # sorted: the build iterated a set, so its ties were by hash order
        dist = defaultdict(set)
        for k in ks:
            for lab, ps in index.get(k, {}).items():
                dist[lab] |= set(ps) - {t["study"]}
        tot = sum(len(s) for s in dist.values())
        if tot >= 2:
            lab, ps = max(dist.items(), key=lambda kv: len(kv[1])); p = len(ps) / tot
            if p >= 0.5:
                votes[lab] += p * math.log(1 + tot); ev.append(("vote", lab, len(ps), round(p, 2)))
        for lab in set().union(*(set(origin.get(k, ())) for k in ks)) if ks else ():
            votes[lab] += ORIGIN_W; ev.append(("origin", lab))
        cited = next((paper_of[k] for k in ks if k in paper_of), None)
        if cited and cited != t["study"] and plabs.get(cited):
            for lab in plabs[cited]:
                votes[lab] += x["p_lineage"] / len(plabs[cited]); ev.append(("corpus", lab, cited))
    lab, conf = (None, 0.0)
    if votes:
        lab, s = votes.most_common(1)[0]; conf = s / sum(votes.values())
    return {"study": t["study"], "task": t["task"], "label": lab, "conf": round(conf, 3),
            "top": [(k, round(s, 2)) for k, s in votes.most_common(3)], "n_refs": len(t["refs"]),
            "n_lineage": len(lin), "evidence": ev[:12], "refs": t["refs"]}


def load_jsonl(path):
    with open(path) as f:
        return [json.loads(line) for line in f if line.strip()]
