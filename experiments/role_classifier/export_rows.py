"""Write the three kinds of training row from labelling units and their labels.

    python export_rows.py OUT_DIR --units table_units.jsonl prose_units.jsonl \
        --labels labels/astra labels/sol

`--labels` directories are read in order of precedence: a set labelled in the
first (the gold set) keeps that label. Writes:

    OUT_DIR/encoder.jsonl          one row per set, for train_encoder.py
    OUT_DIR/nu_v21_role.jsonl      nu-v21 rows with a role per analysis
    OUT_DIR/nu_v21_template.json   train_nuex3_v2.py's TEMPLATE with the role
    OUT_DIR/prose/{train,val,test}.jsonl   prose v3 rows with each point's role
"""

from __future__ import annotations

import argparse
import collections
import json
from pathlib import Path

from ingestion_workflow.services.set_roles import export, labeling


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("out", type=Path)
    parser.add_argument("--units", type=Path, nargs="+", required=True)
    parser.add_argument("--labels", type=Path, nargs="+", required=True)
    args = parser.parse_args()
    labels = {}
    for directory in reversed(args.labels):
        labels.update(labeling.read_labels(directory))
    units = [u for path in args.units for u in labeling.read_units(path)]
    (args.out / "prose").mkdir(parents=True, exist_ok=True)
    counts = collections.Counter()
    with open(args.out / "encoder.jsonl", "w", encoding="utf-8") as handle:
        for row in export.encoder_rows(units, labels):
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
            counts[f"encoder:{row['origin']}:{row['split']}"] += 1
    with open(args.out / "nu_v21_role.jsonl", "w", encoding="utf-8") as handle:
        for row in export.nu_v21_rows(units, labels):
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
            counts["nu_v21"] += 1
    (args.out / "nu_v21_template.json").write_text(export.NU_V21_TEMPLATE, encoding="utf-8")
    by_split = {u["unit_id"]: u.get("dataset_split", "train") for u in units}
    handles = {
        s: open(args.out / "prose" / f"{s}.jsonl", "w", encoding="utf-8")
        for s in ("train", "val", "test")
    }
    for row in export.prose_rows(units, labels):
        split = by_split.get(f"p:{row['id']}", "train")
        handles[split].write(json.dumps(row, ensure_ascii=False) + "\n")
        counts[f"prose:{split}"] += 1
    for h in handles.values():
        h.close()
    print(json.dumps(dict(sorted(counts.items())), indent=2))


if __name__ == "__main__":
    main()
