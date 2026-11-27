"""The prompt-parity fixtures (`tests/normalization/data/task_design_prompts.json`) from task-design v4's val rows.

Picks rows that between them use every part of the document (paradigm, stimulus lists, model terms, analyses,
excerpt, an empty kind), and keeps for each its labelling item, the training prompt's task part, and the record
the item was built from cut to the slots the item reads, with the parse text kept only in the excerpt windows
(the test blanks the rest, so offsets hold).

Each row also keeps v4's answer for it (PRED, round 4 `pred-v4/td-v4-final-val.jsonl`) and that answer as
round 4's scorer reads it (ROUND4_DIR's `labels_lib.normalize` over `score_design.as_label`), for `design.read`.

    python -I make_prompt_fixtures.py CATALOG_SQLITE VAL_FT.jsonl TRAINING_VAL.jsonl PRED.jsonl ROUND4_DIR OUT.json [N]
"""
import gzip
import json
import sqlite3
import sys
from pathlib import Path

from ingestion_workflow.normalization import design

catalog, rows_path, items_path, pred_path, round4, out = sys.argv[1:7]
n = int(sys.argv[7]) if len(sys.argv) > 7 else 8
sys.path.insert(0, round4)
import labels_lib  # noqa: E402

preds = {p["id"]: p["pred"] for p in map(json.loads, open(pred_path))}


def as_label(t):
    """score_design.as_label: the template's shape (levels as lists) back to the label's."""
    return {"task": t.get("task"), "family": t.get("family"), "task_confidence": t.get("task_confidence"),
            "factors": [{"from_term": f.get("from_term"), "axis": f.get("axis") or "",
                         "levels": {str(x.get("level")): x.get("value") for x in f.get("levels") or []
                                    if isinstance(x, dict)}}
                        for f in t.get("factors") or [] if isinstance(f, dict)],
            "context": [x for x in t.get("context") or [] if isinstance(x, dict) and x.get("axis")],
            "conditions": [{"id": c.get("id"), "kind": c.get("kind"),
                            "atoms": [a for a in c.get("atoms") or [] if isinstance(a, dict) and a.get("axis")],
                            "expansion": c.get("expansion"), "gloss": c.get("gloss")}
                           for c in t.get("conditions") or [] if isinstance(c, dict)]}
db = sqlite3.connect(f"file:{catalog}?mode=ro", uri=True)
blobs = Path(catalog).parent / "blobs"
items = {f"{x['meta']['article']}:{x['meta']['task']}": x["input"] for x in map(json.loads, open(items_path))}


def features(it):
    return {"paradigm": bool(it.get("paradigm")), "terms": bool(it.get("terms")), "analyses": bool(it.get("analyses")),
            "excerpt": bool(it.get("excerpt")), "stimulus list": any(isinstance(c.get("stimulus"), list)
                                                                     for c in it["conditions"]),
            "no kind": any(not c.get("kind") for c in it["conditions"]), "no terms": not it.get("terms"),
            "24 conditions": len(it["conditions"]) >= 24}


rows = [json.loads(line) for line in open(rows_path)]
picked, seen = [], set()
for r in sorted(rows, key=lambda r: r["id"]):
    new = {k for k, on in features(items[r["id"]]).items() if on} - seen
    if new or len(picked) < 5:
        picked.append(r); seen |= new
    if len(picked) >= n:
        break


def bare(x):
    """Every extracted value as {"value": ...}: the item reads values only."""
    if isinstance(x, dict):
        return {"value": bare(x["value"])} if "value" in x and "extraction_status" in x else \
            {k: bare(y) for k, y in x.items()}
    return [bare(y) for y in x] if isinstance(x, list) else x


def cut(record, tid):
    """The record less what the item does not read: other tasks, terms carrying none of the task's conditions,
    analyses but for their names and tasks, evidence but for the task description's."""
    task = next(t for t in record["tasks"] if isinstance(t, dict) and str(design.v(t.get("local_id"))) == tid)
    item = design.item_of(record, task, None)
    names = {m["name"] for m in item["terms"]}
    terms = [bare(t) for me in record.get("model_estimations") or [] for t in me.get("terms") or []
             if isinstance(t, dict) and design.v(t.get("name")) in names]
    kept = {"local_id": task.get("local_id"), "name": bare(task.get("name")), "paradigm": bare(task.get("paradigm")),
            "description": task.get("description"),
            "conditions": [{k: bare(c.get(k)) for k in ("local_id", "name", "condition_kind", "description",
                                                        "stimulus_content")} for c in task.get("conditions") or []
                           if isinstance(c, dict)]}
    return {"tasks": [kept], "model_estimations": [{"terms": terms}],
            "analyses": [{"name": bare(a.get("name")), "tasks": a.get("tasks")} for a in record.get("analyses") or []
                         if isinstance(a, dict)]}


def windows(text, task):
    """The excerpt windows of `text`, as (start, text), and its length: `blanked` rebuilds the text from them."""
    keep = bytearray(len(text))
    spans = [(s.get("start_char"), s.get("end_char")) for es in ((task.get("description") or {}).get("evidence")
                                                                  or {}).get("sets") or [] for s in es.get("spans") or []]
    for a, b in [(a, b) for a, b in spans if isinstance(a, int)][:2]:
        for i in range(max(0, a - 200), min(len(text), b + 400)):
            keep[i] = 1
    out, i = [], 0
    while i < len(text):
        if keep[i]:
            j = i
            while j < len(text) and keep[j]:
                j += 1
            out.append([i, text[i:j]]); i = j
        else:
            i += 1
    return {"length": len(text), "windows": out}


def blanked(held):
    chars = [" "] * held["length"]
    for start, part in held["windows"]:
        chars[start:start + len(part)] = part
    return "".join(chars)


fixtures = []
for r in picked:
    article, tid = r["id"].split(":", 1)
    blob = db.execute("select blob from artifacts where article_id=? and stage='normalize' and status='ok'",
                      (article,)).fetchone()[0]
    d = json.load(gzip.open(blobs / blob[:2] / f"{blob}.json.gz"))
    record = d.get("study", d) if "tasks" not in d else d
    sync = db.execute("select summary from artifacts where article_id=? and stage='sync'", (article,)).fetchone()
    p = Path(json.loads(sync[0])["path"]) / "parse"
    text = (p.parent / json.loads((p / "parsed_paper.json").read_text())["text_path"]).read_bytes().decode("utf-8")
    small = cut(record, tid)
    task = small["tasks"][0]
    held = windows(text, task)
    assert design.prompt_of(design.item_of(small, task, blanked(held))) == r["prompt"], r["id"]
    fixtures.append({"id": r["id"], "features": sorted(k for k, on in features(items[r["id"]]).items() if on),
                     "item": items[r["id"]], "document": r["prompt"][len(design.prefix("task-design-v4")) + 1:],
                     "record": small, "text": held, "answer": preds[r["id"]],
                     "scored": labels_lib.normalize(as_label(json.loads(preds[r["id"]])))})
json.dump({"source": "round4 ft-v4/val_ft.jsonl (task-design v4 val)", "template": picked[0]["template"],
           "rows": fixtures}, open(out, "w"), ensure_ascii=False, indent=0)
print(len(fixtures), sorted(seen))
