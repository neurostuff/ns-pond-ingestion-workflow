"""Score the stage's answers for the gate sample against Claude's labels, with round 4's scorer.

Each labelled item's `prompt_key` is looked up in the answer store the stage run filled, so the answer scored is
the one the stage got for exactly the input the labeller saw. Writes, under OUT_DIR, the scorer's three inputs
(rows_ft.jsonl with each label as the training target, pred.jsonl, split.jsonl), runs ROUND4_DIR's
score_design.py on them, and adds what that does not report: task accuracy when v4 says "high", the old
pipeline's accuracy on the same tasks, and both without the tasks Claude marked as meta-analytic groupings.

    python -I gate_score.py ITEMS.jsonl LABELS_DIR ANSWERS.sqlite ROUND4_DIR OUT_DIR
"""
import glob
import json
import re
import sqlite3
import subprocess
import sys
from pathlib import Path

items_path, labels_dir, store_path, round4, out = sys.argv[1:6]
out = Path(out).resolve()
out.mkdir(parents=True, exist_ok=True)
sys.path.insert(0, round4)
import labels_lib as L  # noqa: E402

META_RE = re.compile(r"meta-?analy|ale |pooled|grouping")
items = [json.loads(line) for line in open(items_path)]
labels = {}
for f in sorted(glob.glob(f"{labels_dir}/*.jsonl")):
    for line in open(f):
        if line.strip():
            x = json.loads(line)
            labels[x["item"]] = x
store = sqlite3.connect(f"file:{store_path}?mode=ro", uri=True)


def target(o):
    """make_ft_v4.target: a normalized label in the template's shape."""
    return json.dumps({
        "factors": [{"from_term": f.get("from_term"), "axis": f["axis"],
                     "levels": [{"level": k, "value": v} for k, v in f["levels"].items()]} for f in o["factors"]],
        "context": o["context"],
        "conditions": [{"id": c["id"], "kind": c.get("kind"), "atoms": c["atoms"],
                        "expansion": c.get("expansion"), "gloss": c.get("gloss")} for c in o["conditions"]],
        "task": o.get("task"), "family": o.get("family"), "task_confidence": o.get("task_confidence"),
    }, ensure_ascii=False)


tally = {"items": len(items), "labelled": 0, "answered": 0}
extra = {"all": [], "not meta": []}
with open(out / "rows_ft.jsonl", "w") as fr, open(out / "pred.jsonl", "w") as fp, open(out / "split.jsonl", "w") as fs:
    for x in items:
        lab = labels.get(x["item"])
        got = store.execute("select finish, content from answers where key=?", (x["prompt_key"],)).fetchone()
        if lab is None or got is None:
            continue
        tally["labelled"] += 1
        tally["answered"] += 1
        rid = f"{x['article']}:{x['task']}"
        gold = L.normalize(lab)
        fr.write(json.dumps({"id": rid, "target_json": target(gold)}, ensure_ascii=False) + "\n")
        fp.write(json.dumps({"id": rid, "finish": got[0], "pred": got[1] or ""}, ensure_ascii=False) + "\n")
        fs.write(json.dumps({"meta": {"article": x["article"], "task": x["task"]}, "input": {"terms": x["terms"]}},
                            ensure_ascii=False) + "\n")
        try:
            pred = json.loads(got[1] or "")
        except ValueError:
            pred = {}
        row = {"gold": gold["task"], "pred": pred.get("task"), "conf": pred.get("task_confidence"),
               "old": x.get("current_label"), "gold_family": gold["family"], "pred_family": pred.get("family")}
        extra["all"].append(row)
        if not META_RE.search((lab.get("note") or "").lower()):
            extra["not meta"].append(row)

subprocess.run([sys.executable, "-s", f"{round4}/score_design.py", str(out / "rows_ft.jsonl"), str(out / "pred.jsonl"),
                str(out / "score.json"), str(out / "split.jsonl")], check=True, cwd=round4,
               stdout=subprocess.DEVNULL)


def pct(a, b):
    return f"{a}/{b} ({100 * a / b:.1f}%)" if b else "n/a"


report = {"counts": tally, "score_design": json.load(open(out / "score.json"))}
for name, rows in extra.items():
    high = [r for r in rows if r["conf"] == "high"]
    report[name] = {
        "tasks": len(rows),
        "v4 task": pct(sum(r["gold"] == r["pred"] for r in rows), len(rows)),
        "v4 says high": pct(len(high), len(rows)),
        "v4 task when high": pct(sum(r["gold"] == r["pred"] for r in high), len(high)),
        "v4 task when not high": pct(sum(r["gold"] == r["pred"] for r in rows if r["conf"] != "high"),
                                     len(rows) - len(high)),
        "old pipeline task": pct(sum(r["gold"] == r["old"] for r in rows), len(rows)),
        "Claude null": sum(r["gold"] is None for r in rows),
        "v4 null": sum(r["pred"] is None for r in rows),
    }
report["disagreements"] = [r for r in extra["all"] if r["gold"] != r["pred"]]
json.dump(report, open(out / "gate.json", "w"), indent=1)
print(json.dumps({k: v for k, v in report.items() if k != "disagreements"}, indent=1))
