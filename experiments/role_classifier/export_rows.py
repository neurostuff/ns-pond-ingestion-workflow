"""Write the set-role encoder's training rows from labelling units and their labels.

    python export_rows.py OUT_DIR --units table_units.jsonl prose_units.jsonl \
        --labels labels/astra labels/sol --id-map units/slug_dbids.json \
        --prose-dataset /data/alejandro/jk-prose-coords/datasets/v3-20261006

`--labels` directories are read in order of precedence: a set labelled in the
first (the gold set) keeps that label. Writes OUT_DIR/encoder.jsonl, one row per set,
for train_encoder.py. `--id-map` maps slug article ids to database ids for the split.
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
    parser.add_argument("--id-map", type=Path, help="slug article id -> database id (JSON)")
    args = parser.parse_args()
    id_map = json.loads(args.id_map.read_text()) if args.id_map else {}
    labels = {}
    for directory in reversed(args.labels):
        labels.update(labeling.read_labels(directory))
    units = [u for path in args.units for u in labeling.read_units(path)]
    args.out.mkdir(parents=True, exist_ok=True)
    counts = collections.Counter()
    with open(args.out / "encoder.jsonl", "w", encoding="utf-8") as handle:
        for row in export.encoder_rows(units, labels, id_map):
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")
            counts[f"encoder:{row['origin']}:{row['split']}"] += 1
    print(json.dumps(dict(sorted(counts.items())), indent=2))


if __name__ == "__main__":
    main()
