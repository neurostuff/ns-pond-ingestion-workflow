"""The phase-5 gate's sample: fresh articles of the production corpus, and one task of each to label.

Articles with a normalize input (evidence, or the record when evidence was skipped) and a sync, none of them in
round 4's training splits or labelling items, drawn at random (SEED). The first N_LABEL drawn also get one task
each (a random task with conditions) written as a labelling item, built as the normalize stage builds the
model's input: the input renormalized (`normalize_assign.renormalize`), then `design.item_of`, with the old
pipeline's label as `current_label`, as the labellers saw it in rounds 3 to 5.

    python -I gate_sample.py CATALOG_SQLITE ROUND4_DIR N_CHANGE N_LABEL SEED OUT_DIR

Writes OUT_DIR/manifest.jsonl (the N_CHANGE articles, for the stage run), items.jsonl (the labelling items, with
`item`, `article`, `task`, and `prompt_key`: the answer-store key of the stage's prompt for the task) and
packets/g-<k>.jsonl (the items in labelling batches of 10).
"""
import gzip
import json
import random
import sqlite3
import sys
from pathlib import Path

from ingestion_workflow.normalization import api, design
from ingestion_workflow.pipeline.stages.offloaded import drop_siblings  # noqa: F401 - renormalize's first step
from ingestion_workflow.services import normalize_assign as na

catalog, round4, n_change, n_label, seed, out = sys.argv[1:7]
n_change, n_label = int(n_change), int(n_label)
out = Path(out)
(out / "packets").mkdir(parents=True, exist_ok=True)
blobs = Path(catalog).parent / "blobs"
db = sqlite3.connect(f"file:{catalog}?mode=ro", uri=True)

seen = set()
for split in ("train", "val", "test"):
    for line in open(f"{round4}/training/{split}.jsonl"):
        seen.add(json.loads(line)["meta"]["article"])
for items in (f"{round4}/items.jsonl", f"{round4}/../round5/items.jsonl"):
    for line in open(items):
        seen.add(json.loads(line)["article"])


def payload(blob):
    d = json.load(gzip.open(blobs / blob[:2] / f"{blob}.json.gz"))
    return d.get("study", d) if "tasks" not in d and isinstance(d.get("study"), dict) else d


rows = db.execute("""select a.article_id,
                            max(case when a.stage='evidence' then a.status end),
                            max(case when a.stage='evidence' then a.blob end),
                            max(case when a.stage='record' then a.status end),
                            max(case when a.stage='record' then a.blob end),
                            max(case when a.stage='normalize' then a.blob end)
                     from artifacts a where a.stage in ('evidence', 'record', 'normalize') and a.source='ns-27b'
                     group by a.article_id""").fetchall()
synced = {a: json.loads(s) for a, s in db.execute(
    "select article_id, summary from artifacts where stage='sync' and source='' and status='ok'")}
pool = []
for article, ev_status, ev_blob, rec_status, rec_blob, norm_blob in rows:
    read = ev_blob if ev_status == "ok" else rec_blob if ev_status == "skipped" and rec_status == "ok" else None
    if read and article not in seen and synced.get(article, {}).get("path"):
        pool.append((article, read, norm_blob))
pool.sort()
rng = random.Random(int(seed))
rng.shuffle(pool)
change = pool[:n_change]
with open(out / "manifest.jsonl", "w") as f:
    for article, _, _ in change:
        f.write(json.dumps({"article_id": article}) + "\n")

items = []
for article, read, norm_blob in change:
    if len(items) >= n_label:
        break
    record = payload(read)
    sync = synced[article]
    p = Path(sync["path"]) / "parse"
    try:
        text = (p.parent / json.loads((p / "parsed_paper.json").read_text())["text_path"]).read_bytes().decode()
    except (OSError, ValueError, KeyError):
        continue
    parse = p / "coordinate_parse.json"
    record = na.renormalize(record, parse if parse.is_file() else None, text)
    tasks = design.tasks_of(record)
    if not tasks:
        continue
    task = rng.choice(tasks)
    tid = str(design.v(task.get("local_id")))
    old = payload(norm_blob) if norm_blob else {}
    was = next((t for t in old.get("tasks") or [] if isinstance(t, dict) and str(design.v(t.get("local_id"))) == tid),
               {})
    current = next((e.get("value") for e in (was.get("name") or {}).get("normalized") or []
                    if e.get("vocabulary") == "astra canonical task" and e.get("status") == "mapped"), None)
    item = design.item_of(record, task, text)
    prompt = api.prompts(record, text, "task-design-v4")[tid]
    items.append({"item": f"G{len(items):04d}", "article": article, "task": tid, "current_label": current, **item,
                  "prompt_key": design.key_of(prompt, "task-design-v4")})
with open(out / "items.jsonl", "w") as f:
    for x in items:
        f.write(json.dumps(x, ensure_ascii=False) + "\n")
for k in range(0, len(items), 10):
    with open(out / "packets" / f"g-{k // 10}.jsonl", "w") as f:
        for x in items[k:k + 10]:
            f.write(json.dumps({key: v for key, v in x.items() if key != "prompt_key"}, ensure_ascii=False) + "\n")
print(json.dumps({"pool": len(pool), "excluded (round 4 and 5)": len(seen), "change sample": len(change),
                  "labelled": len(items)}))
