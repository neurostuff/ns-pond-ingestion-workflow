"""Record -> prompt parity of `normalization.design` against the task-design v4 training rows.

For each row of a fine-tune split (round 4 `ft-v4/val_ft.jsonl`), the article's normalize payload in the
catalog (the record the labelling items were built from, under fit-eec1437aa10b) and its parse text are read,
the prompt is built with `design.item_of` and `design.prompt_of`, and compared byte for byte with the row's.

    python -I prompt_parity.py CATALOG_SQLITE ROWS.jsonl OUT.json [FIT]
"""
import collections
import gzip
import json
import sqlite3
import sys
from pathlib import Path

from ingestion_workflow.normalization import design

catalog, rows_path, out = sys.argv[1:4]
fit = sys.argv[4] if len(sys.argv) > 4 else "fit-eec1437aa10b"
blobs = Path(catalog).parent / "blobs"
db = sqlite3.connect(f"file:{catalog}?mode=ro", uri=True)


def payload(blob):
    d = json.load(gzip.open(blobs / blob[:2] / f"{blob}.json.gz"))
    return d.get("study", d) if "tasks" not in d and isinstance(d.get("study"), dict) else d


def text_of(article):
    """The parse text, or None where the labelling items read none (no sync, no path, no file)."""
    row = db.execute("select summary from artifacts where article_id=? and stage='sync'", (article,)).fetchone()
    try:
        p = Path(json.loads(row[0])["path"]) / "parse"
        paper = json.loads((p / "parsed_paper.json").read_text("utf-8"))
        return (p.parent / paper["text_path"]).read_bytes().decode("utf-8")
    except Exception:  # noqa: BLE001
        return None


tally, misses = collections.Counter(), []
for line in open(rows_path):
    r = json.loads(line)
    article, tid = r["id"].split("#")[0].split(":", 1)
    got = db.execute("""select a.blob, r.fit from artifacts a join runs r on r.run_id=a.run_id
                        where a.article_id=? and a.stage='normalize' and a.status='ok'""", (article,)).fetchone()
    if got is None or got[1] != fit:
        tally["normalize payload not of " + fit] += 1
        continue
    record = payload(got[0])
    task = next((t for t in record.get("tasks") or [] if isinstance(t, dict)
                 and str(design.v(t.get("local_id"))) == tid), None)
    if task is None:
        tally["task missing"] += 1
        continue
    prompt = design.prompt_of(design.item_of(record, task, text_of(article)))
    same = prompt == r["prompt"] and r["template"] == design.template()
    tally["same" if same else "differs"] += 1
    if not same:
        a, b = prompt.splitlines(), r["prompt"].splitlines()
        first = next((i for i, (x, y) in enumerate(zip(a, b)) if x != y), min(len(a), len(b)))
        misses.append({"id": r["id"], "line": first, "ours": a[first][:300] if first < len(a) else None,
                       "training": b[first][:300] if first < len(b) else None})
json.dump({"rows": rows_path, "fit": fit, "tally": dict(tally), "differs": misses}, open(out, "w"), indent=1)
print(json.dumps(dict(tally)))
