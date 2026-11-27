"""What normalize v2 changes over a sample, against the normalization it replaces.

BEFORE is the sample's normalize artifacts as they were before the run (`snapshot`: article, blob, fit), read from
SOURCE_BLOBS; the run's artifacts are read from the catalog copy. Per task: the task and family before and after,
v4's confidence. Per analysis: the old contrast key and set, and the new key. For the articles the run held back,
which input was stale.

    python -I change_report.py CATALOG_COPY BEFORE.jsonl SOURCE_BLOBS OUT.json
"""
import collections
import gzip
import json
import sqlite3
import sys
from pathlib import Path

copy, before_path, source_blobs, out = sys.argv[1:5]
db = sqlite3.connect(f"file:{copy}?mode=ro", uri=True)
copy_blobs = Path(copy).parent / "blobs"


def v(x):
    return x.get("value") if isinstance(x, dict) else x


def payload(root, blob):
    d = json.load(gzip.open(Path(root) / blob[:2] / f"{blob}.json.gz"))
    return d.get("study", d) if "tasks" not in d and isinstance(d.get("study"), dict) else d


def entries(wrapper, vocabulary):
    return [e for e in ((wrapper or {}).get("normalized") or []) if e.get("vocabulary") == vocabulary]


def mapped(wrapper, vocabulary):
    got = [e.get("value") for e in entries(wrapper, vocabulary) if e.get("status") == "mapped"]
    return got[0] if got else None


before = {json.loads(line)["article"]: json.loads(line) for line in open(before_path)}
t = collections.Counter()
keys_new, keys_old, sets_old = collections.Counter(), collections.Counter(), collections.Counter()
conf = collections.Counter()
held_back = collections.Counter()
for article, was in sorted(before.items()):
    now = db.execute("""select a.blob, r.version from artifacts a join runs r on r.run_id=a.run_id
                        where a.article_id=? and a.stage='normalize' and a.source='ns-27b'""", (article,)).fetchone()
    if now is None or not (now[1] or "").startswith("design-"):
        t["articles held back"] += 1
        for stage in ("record", "evidence"):
            row = db.execute("select status from artifacts where article_id=? and stage=? and source='ns-27b'",
                             (article, stage)).fetchone()
            held_back[f"{stage}: {row[0] if row else 'none'}"] += 1
        continue
    t["articles normalized"] += 1
    new, old = payload(copy_blobs, now[0]), payload(source_blobs, was["blob"])
    old_tasks = {str(v(x.get("local_id"))): x for x in old.get("tasks") or [] if isinstance(x, dict)}
    for task in new.get("tasks") or []:
        if not isinstance(task, dict) or not task.get("conditions"):
            continue
        t["tasks"] += 1
        o = old_tasks.get(str(v(task.get("local_id")))) or {}
        a, b = mapped(o.get("name"), "astra canonical task"), mapped(task.get("name"), "astra canonical task")
        fa, fb = mapped(o.get("name"), "astra task family"), mapped(task.get("name"), "astra task family")
        t["task: same"] += a == b
        t["task: was none, now one"] += a is None and b is not None
        t["task: was one, now none"] += a is not None and b is None
        t["task: changed"] += a is not None and b is not None and a != b
        t["family: same"] += fa == fb
        c = mapped(task.get("name"), "ns-pond task confidence")
        conf[c or "no answer"] += 1
        t["task: no answer"] += not entries(task.get("name"), "astra task family") and bool(
            [e for e in entries(task.get("name"), "astra canonical task") if "no answer" in (e.get("method") or "")])
    old_analyses = {str(v(x.get("local_id"))): x for x in old.get("analyses") or [] if isinstance(x, dict)}
    for a in new.get("analyses") or []:
        if not isinstance(a, dict):
            continue
        t["analyses"] += 1
        k = mapped(a.get("name"), "ns-pond contrast key v2")
        keys_new[k] += 1
        o = old_analyses.get(str(v(a.get("local_id")))) or {}
        ok = mapped(o.get("name"), "astra contrast key")
        if ok:
            keys_old[ok] += 1
        osets = [e for e in (o.get("name") or {}).get("normalized") or []
                 if str(e.get("vocabulary", "")).startswith("ns-pond contrast set ") and "orientation" not in
                 e["vocabulary"] and e.get("status") == "mapped"]
        t["analyses: had a contrast set"] += bool(osets)
        for e in osets:
            sets_old[e["value"]] += 1
        t["analyses: manipulated axes"] += bool(entries(a.get("name"), "ns-pond contrast manipulated"))
        kind = mapped(a.get("name"), "ns-pond contrast kind")
        t[f"contrast kind: {kind}"] += 1


def spread(counter):
    sizes = sorted(counter.values(), reverse=True)
    return {"distinct": len(counter), "singletons": sum(s == 1 for s in sizes),
            "analyses in keys shared by 2+": sum(s for s in sizes if s > 1), "largest": sizes[:5]}


report = {"counts": dict(sorted(t.items())), "v4 task confidence": dict(conf), "held back": dict(held_back),
          "new contrast key v2": spread(keys_new), "old astra contrast key": spread(keys_old),
          "old contrast sets": spread(sets_old),
          "most common new keys": keys_new.most_common(12)}
json.dump(report, open(out, "w"), indent=1)
print(json.dumps(report, indent=1))
