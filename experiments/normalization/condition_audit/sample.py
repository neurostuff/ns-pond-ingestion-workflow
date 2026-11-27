"""Item table from a `survey.py` groups.jsonl, and the seeded stratified sample `gold.tsv` was graded on."""
import json, random, re, collections as C
rows = [json.loads(l) for l in open("groups.jsonl")]
items = []
for r in rows:
    for i, t in enumerate(r["items"]):
        es = [e for e in r["entries"] if e["item"] == i]
        items.append({"article": r["article"], "group": r["group"], "name": r["name"], "is_healthy": r["is_healthy"],
                      "item": i, "text": t if isinstance(t, str) else json.dumps(t),
                      "mondo": [e for e in es if e["vocabulary"] == "MONDO"],
                      "onvoc": [e for e in es if e["vocabulary"] == "ONVOC"],
                      "role": next((e["value"] for e in es if e["vocabulary"] == "astra medical role"), None)})
fold = lambda s: re.sub(r"[^a-z0-9]+", " ", s.lower()).strip()
freq = C.Counter()
for k in {(x["article"], fold(x["text"])) for x in items}:
    freq[k[1]] += 1
for x in items:
    x["freq"] = freq[fold(x["text"])]
HEALTHY = re.compile(r"\b(healthy|controls?|typical(ly)?|normal|neurotypical|no |none|without|free of|absence|non-?clinical|unaffected|volunteers?)\b", re.I)
ABBR = re.compile(r"\b[a-z]{0,3}[A-Z]{2,6}[a-z]?\b")
SUB = re.compile(r"variant|subtype|\btype\b|\bbv|\bsv|\bnfv|\blv|PPA|aphasia|onset|treatment[- ]resistant|first[- ]episode|remitted|amnestic|relapsing|progressive|refractory|dominant|predominant|with(out)? aura|primary|secondary|unilateral|bilateral|mild|severe|stage|grade", re.I)
COMP = re.compile(r" with | and |comorbid|\+|/|;|,| or ", re.I)
def stratum(x):
    t = x["text"]
    if x["is_healthy"] is True or HEALTHY.search(t): return "healthy_control"
    if ABBR.search(t) and len(t) <= 40: return "abbreviation"
    if SUB.search(t): return "subtype"
    if COMP.search(t): return "composite"
    if x["freq"] >= 50: return "common"
    if x["freq"] == 1: return "rare"
    return "mid"
for x in items:
    x["stratum"] = stratum(x)
json.dump(items, open("items.json", "w"))
print(C.Counter(x["stratum"] for x in items))
rnd = random.Random(20261127)
sample = []
by = C.defaultdict(list)
for x in items: by[x["stratum"]].append(x)
for s in ["common", "rare", "healthy_control", "composite", "subtype", "abbreviation"]:
    pool = by[s]
    if s == "common":   # one item per distinct common string, so 30 different conditions
        seen = {}
        for x in rnd.sample(pool, len(pool)):
            seen.setdefault(fold(x["text"]), x)
        pool = list(seen.values())
    for x in rnd.sample(pool, 30):
        sample.append({**x, "sample": s})
for x in rnd.sample(items, 100):
    sample.append({**x, "sample": "random"})
for n, x in enumerate(sample):
    x["sid"] = n
json.dump(sample, open("sample.json", "w"), indent=0)
def show(x):
    m = " | ".join(f"{e['status'][:3]}:{e.get('label') or ''}[{e['method']}{'' if e.get('confidence') is None else ' %.2f' % e['confidence']}]" + (f" <{e['head']}>" if e.get("head") else "") for e in x["mondo"])
    o = " | ".join(f"{e['label']}[{e['method'].split(' by ')[-1]}{' R' if e.get('rollup') else ''}]" for e in x["onvoc"])
    return f"{x['sid']:3d} {x['sample'][:4]} h={str(x['is_healthy'])[0]} n={x['name']!s:.30} :: {x['text']!r:.110}\n     M: {m}\n     O: {o}"
with open("sample.txt", "w") as fh:
    for x in sample: fh.write(show(x) + "\n")
