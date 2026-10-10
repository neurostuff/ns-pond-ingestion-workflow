"""Write the set-role encoders' training rows from labelling units and their labels.

    python export_rows.py OUT_DIR --units table_units.jsonl prose_units.jsonl \
        --labels labels/sol-table labels/sol-prose labels/relabel-v4-no-display \
        --id-map units/slug_dbids.json

Each set takes its label of the highest `label_version` in any `--labels`
directory; between equal versions the first directory given wins (list a gold
set first). A label no version made study_schema's (an old `display`) is left
out. Writes one file per origin, one row per set, for train_encoder.py:
OUT_DIR/encoder-table.jsonl for the table model and OUT_DIR/encoder-text.jsonl
for the prose model. `--id-map` maps slug article ids to database ids for the split.
"""

from __future__ import annotations

import argparse
import collections
import json
from pathlib import Path

from ingestion_workflow.services.set_roles import export, labeling
from ingestion_workflow.services.set_roles.model import ORIGINS


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("out", type=Path)
    parser.add_argument("--units", type=Path, nargs="+", required=True)
    parser.add_argument("--labels", type=Path, nargs="+", required=True)
    parser.add_argument("--id-map", type=Path, help="slug article id -> database id (JSON)")
    args = parser.parse_args()
    id_map = json.loads(args.id_map.read_text()) if args.id_map else {}
    labels = labeling.latest_labels(args.labels)
    units = [u for path in args.units for u in labeling.read_units(path)]
    args.out.mkdir(parents=True, exist_ok=True)
    counts = collections.Counter()
    handles = {
        origin: open(args.out / f"encoder-{origin}.jsonl", "w", encoding="utf-8")
        for origin in ORIGINS
    }
    try:
        for row in export.encoder_rows(units, labels, id_map):
            handles[row["origin"]].write(json.dumps(row, ensure_ascii=False) + "\n")
            counts[f"encoder-{row['origin']}:{row['split']}"] += 1
    finally:
        for handle in handles.values():
            handle.close()
    print(json.dumps(dict(sorted(counts.items())), indent=2))


if __name__ == "__main__":
    main()
