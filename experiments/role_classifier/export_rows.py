"""Write the three kinds of training row from labelling units and their labels.

    python export_rows.py OUT_DIR --units table_units.jsonl prose_units.jsonl \
        --labels labels/astra labels/sol --id-map units/slug_dbids.json \
        --prose-dataset /data/alejandro/jk-prose-coords/datasets/v3-20261006

`--labels` directories are read in order of precedence: a set labelled in the
first (the gold set) keeps that label. Writes:

    OUT_DIR/encoder.jsonl          one row per set, for train_encoder.py
    OUT_DIR/nu_v21_role.jsonl      nu-v21 rows with a role per analysis
    OUT_DIR/nu_v21_template.json   train_nuex3_v2.py's TEMPLATE with the role
    OUT_DIR/prose/{train,val,test}.jsonl   prose v3 rows with each point's role
    OUT_DIR/prose/synthetic_hard_{train,test}.jsonl   the same, for those rows
    OUT_DIR/prose/wide-context*.jsonl      copied from the prose dataset

OUT_DIR/prose is a prose dataset directory: `build_ft.py` reads its heading,
before and after from the wide-context files, so the export refuses to write
one without them. `--id-map` maps slug article ids to database ids for the split.
"""

from __future__ import annotations

import argparse
import collections
import json
import shutil
from pathlib import Path

from ingestion_workflow.services.set_roles import export, labeling


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("out", type=Path)
    parser.add_argument("--units", type=Path, nargs="+", required=True)
    parser.add_argument("--labels", type=Path, nargs="+", required=True)
    parser.add_argument("--id-map", type=Path, help="slug article id -> database id (JSON)")
    parser.add_argument(
        "--prose-dataset", type=Path, required=True, help="the prose dataset the units came from"
    )
    args = parser.parse_args()
    wide = sorted(args.prose_dataset.glob("wide-context*.jsonl"))
    if not wide:
        raise SystemExit(f"no wide-context*.jsonl in {args.prose_dataset}: build_ft.py needs them")
    id_map = json.loads(args.id_map.read_text()) if args.id_map else {}
    labels = {}
    for directory in reversed(args.labels):
        labels.update(labeling.read_labels(directory))
    units = [u for path in args.units for u in labeling.read_units(path)]
    (args.out / "prose").mkdir(parents=True, exist_ok=True)
    counts = collections.Counter()
    with open(args.out / "encoder.jsonl", "w", encoding="utf-8") as handle:
        for row in export.encoder_rows(units, labels, id_map):
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
            counts[f"encoder:{row['origin']}:{row['split']}"] += 1
    with open(args.out / "nu_v21_role.jsonl", "w", encoding="utf-8") as handle:
        for row in export.nu_v21_rows(units, labels):
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
            counts["nu_v21"] += 1
    (args.out / "nu_v21_template.json").write_text(export.NU_V21_TEMPLATE, encoding="utf-8")
    by_id = {u["unit_id"]: u for u in units}
    handles = {}
    for row in export.prose_rows(units, labels, keep_unlabelled=True):
        unit = by_id[f"p:{row['id']}"]
        split = unit.get("dataset_split", "train")
        name = f"synthetic_hard_{split}" if unit.get("dataset") == "synthetic-hard" else split
        if name not in handles:
            handles[name] = open(args.out / "prose" / f"{name}.jsonl", "w", encoding="utf-8")
        handles[name].write(json.dumps(row, ensure_ascii=False) + "\n")
        counts[f"prose:{name}"] += 1
    for h in handles.values():
        h.close()
    for path in wide:
        shutil.copy(path, args.out / "prose" / path.name)
        counts[f"prose:{path.name}"] += 1
    print(json.dumps(dict(sorted(counts.items())), indent=2))


if __name__ == "__main__":
    main()
