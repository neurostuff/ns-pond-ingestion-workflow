"""Training rows for the decision model: v6 labels joined to their units, serialised by production.

Each row's `input` is the string ns-pond's roles stage builds for the set
(`set_roles.table_context` / `prose_context` at the commit on PYTHONPATH), so
the model is trained on what it will read. The labels are the v6 labeller's
fields; the latest label per set (highest label_version) wins.

    PYTHONPATH=<ns-pond checkout> python build_rows.py UNITS LABEL_DIR... --out rows.jsonl
        [--exclude-articles FILE]
"""

from __future__ import annotations

import argparse
import glob
import json
import random
from pathlib import Path

from ingestion_workflow.services.set_roles import prose_context, table_context

LABEL_FIELDS = ("coordinates", "space", "role", "anchor_kind", "source")


def contexts(unit):
    """Each set's production context, built as the roles stage builds it."""
    sets = unit["sets"]
    if unit["origin"] == "table":
        out = []
        for i, s in enumerate(sets):
            ctx = table_context.build(
                s,
                index=i,
                siblings=sets,
                table_text=unit.get("table_serialised"),
                caption=unit.get("caption") or "",
                footer=unit.get("footer") or "",
                table_label=unit.get("table_label") or "",
            )
            ctx.citing = list(unit.get("citing") or [])
            out.append((table_context, ctx))
        return out
    passage = {k: unit.get(k) for k in ("heading", "text", "before", "after")}
    return [(prose_context, prose_context.build(s, passage=passage)) for s in sets]


def latest_labels(dirs):
    best = {}
    for d in dirs:
        for f in sorted(glob.glob(f"{d}/batch-*.jsonl")):
            for line in open(f, encoding="utf-8"):
                r = json.loads(line)
                if r.get("label_version") != 6 or "space" not in r:
                    continue
                best[r["set_id"]] = r
    return best


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("units", type=Path)
    ap.add_argument("labels", nargs="+")
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--exclude-articles", type=Path)
    ap.add_argument("--val-fraction", type=float, default=0.1)
    args = ap.parse_args()
    exclude = set(args.exclude_articles.read_text().split()) if args.exclude_articles else set()
    labels = latest_labels(args.labels)
    articles = sorted({r["article_id"] for r in labels.values()} - exclude)
    random.Random(6).shuffle(articles)
    val = set(articles[: int(len(articles) * args.val_fraction)])
    n = 0
    with open(args.out, "w", encoding="utf-8") as out:
        for line in open(args.units, encoding="utf-8"):
            unit = json.loads(line)
            if unit.get("article_id") in exclude:
                continue
            for i, (module, ctx) in enumerate(contexts(unit)):
                lab = labels.get(f"{unit['unit_id']}#{i}")
                if lab is None:
                    continue
                coords = bool(lab["role"] is not None)
                out.write(json.dumps({
                    "id": lab["set_id"],
                    "article_id": unit.get("article_id"),
                    "origin": unit["origin"],
                    "split": "val" if unit.get("article_id") in val else "train",
                    "input": module.serialize(ctx),
                    "coordinates": coords,
                    "space": lab.get("space") if coords else None,
                    "role": lab["role"],
                    "anchor_kind": lab.get("anchor_kind"),
                    "source": lab.get("source") if coords else None,
                }, ensure_ascii=False) + "\n")
                n += 1
    print(f"{n} rows; {len(val)} val articles of {len(articles)}")


if __name__ == "__main__":
    main()
