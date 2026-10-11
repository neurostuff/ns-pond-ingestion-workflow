"""Sol rewrite pass: make synthetic units less prototypical. One codex call per unit.

    python experiments/role_classifier/synth/rewrite.py IN_DIR OUT_DIR [--workers 3] [--model gpt-6.1-sol]

IN_DIR: units.jsonl + truth.jsonl (role_units.py). OUT_DIR gets units.jsonl (rewritten where the
checks pass, else the original), truth.jsonl (copied) and rewrite.log.jsonl. The prompt never names
the target role. Checks: every coordinate triple and every citation of the original stays verbatim.
"""
import argparse, json, re, sys, shutil, threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from ingestion_workflow.services.set_roles import labeling

SYS = ("You edit text for a corpus of neuroimaging-article excerpts. Rewrite as the authors of a real paper would have written it: "
       "varied, specific, concrete. Keep every number, coordinate, citation, table/figure label and the meaning exactly; do not add "
       "new coordinates or new citations. Do not add commentary.")
T_SCHEMA = {"type": "object", "additionalProperties": False, "required": ["caption", "footer", "citing"],
            "properties": {"caption": {"type": "string"}, "footer": {"type": "string"}, "citing": {"type": "array", "items": {"type": "string"}}}}
P_SCHEMA = {"type": "object", "additionalProperties": False, "required": ["text"], "properties": {"text": {"type": "string"}}}
NUM = re.compile(r"\[\d+\]|\b(?:19|20)\d\d\b|[-−–]?\d+(?:\.\d+)?")


def nums(s):
    return sorted(n.replace("−", "-").replace("–", "-") for n in NUM.findall(s or ""))


def table_prompt(u):
    return ("Rewrite the caption, footer and citing sentences of this table so they read like a real article's: 1-3 sentences for the "
            "caption, add realistic methodological detail (software, thresholds, template, how coordinates were obtained) to the footer "
            "where it fits, and phrase each citing sentence naturally. Keep the table label ('%s') at the start of the caption if "
            "present, and keep every citation and number. Empty footer may stay empty or gain a short true-to-context note.\n\n"
            "CAPTION: %s\nFOOTER: %s\nCITING: %s\nTABLE:\n%s" % (u.get("table_label"), u.get("caption"), u.get("footer"), json.dumps(u.get("citing")), (u.get("table_serialised") or "")[:1500]))


def rewrite(u, truth_sent, call):
    if u["origin"] == "table":
        a, _ = call(SYS, table_prompt(u), T_SCHEMA, u["unit_id"])
        ok = (nums(a["caption"]) == nums(u["caption"]) and nums(" ".join(a["citing"])) == nums(" ".join(u["citing"])) and len(a["citing"]) == len(u["citing"])
              and nums(a["footer"]) == nums(u.get("footer")) and (not u["caption"].startswith(u["table_label"]) or a["caption"].startswith(u["table_label"])))
        return ({**u, "caption": a["caption"], "footer": a["footer"], "citing": a["citing"]} if ok else None), ok
    prompt = ("Rewrite ONLY this sentence from a results/methods passage, in a natural, varied style (you may add a short clause of "
              "context). Keep the coordinates and any citation verbatim.\n\nSENTENCE: %s" % truth_sent)
    a, _ = call(SYS, prompt, P_SCHEMA, u["unit_id"])
    new = a["text"].strip()
    ok = nums(new) == nums(truth_sent) and truth_sent in u["text"] and 0 < len(new) < 3 * len(truth_sent) + 80
    return ({**u, "text": u["text"].replace(truth_sent, new, 1)} if ok else None), ok


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("in_dir", type=Path); ap.add_argument("out_dir", type=Path)
    ap.add_argument("--workers", type=int, default=3); ap.add_argument("--model", default="gpt-6.1-sol")
    a = ap.parse_args()
    a.out_dir.mkdir(parents=True, exist_ok=True)
    U = [json.loads(l) for l in open(a.in_dir / "units.jsonl")]
    sent = {}
    for l in open(a.in_dir / "truth.jsonl"):
        t = json.loads(l)
        if t.get("new_sentence"):
            sent[t["unit_id"]] = t["new_sentence"]
    call = labeling.codex_caller(a.model, "low")
    lock, stats, log = threading.Lock(), {"ok": 0, "kept": 0, "error": 0}, open(a.out_dir / "rewrite.log.jsonl", "a")

    def work(u):
        try:
            new, ok = rewrite(u, sent.get(u["unit_id"]), call)
        except Exception as e:  # noqa
            new, ok = None, False
            with lock: stats["error"] += 1; log.write(json.dumps({"unit_id": u["unit_id"], "error": str(e)[:200]}) + "\n")
        with lock:
            stats["ok" if new else "kept"] += 1
            log.write(json.dumps({"unit_id": u["unit_id"], "rewritten": bool(new)}) + "\n"); log.flush()
        return new or u
    with ThreadPoolExecutor(a.workers) as ex:
        out = list(ex.map(work, U))
    with open(a.out_dir / "units.jsonl", "w") as f:
        for u in out:
            f.write(json.dumps(u, ensure_ascii=False) + "\n")
    shutil.copy(a.in_dir / "truth.jsonl", a.out_dir / "truth.jsonl")
    print(stats, "calls:", len(U))


if __name__ == "__main__":
    main()
