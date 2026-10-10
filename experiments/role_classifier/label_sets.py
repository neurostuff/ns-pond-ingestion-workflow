"""Label coordinate sets through the codex CLI, resumably; compare two labellers.

    python label_sets.py label UNITS.jsonl OUT_DIR --model gpt-6.1-sol [--effort low]
        [--limit N] [--batch-size 25] [--pace 20] [--skip-dataset-labelled]
    python label_sets.py compare DIR_A DIR_B
    python label_sets.py totals OUT_DIR

Every call goes through pondie's `CodexCaller` (`codex exec` on the `codex
login` account, a strict `--output-schema`, API keys stripped from the
environment), which waits out a spent usage limit until its reset time. Needs
a pondie with #10 on PYTHONPATH. Stopping and restarting continues where the
job left off. Bulk labels: gpt-6.1-sol; a small gold set: gpt-6-astra.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ingestion_workflow.services.set_roles import labeling


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    label = sub.add_parser("label")
    label.add_argument("units", type=Path)
    label.add_argument("out", type=Path)
    label.add_argument("--model", required=True)
    label.add_argument("--effort", default="low", choices=["minimal", "low", "medium", "high"])
    label.add_argument("--limit", type=int, default=0, help="units this run (0: all)")
    label.add_argument("--only", type=Path, help="a file of unit ids to label, one per line")
    label.add_argument("--batch-size", type=int, default=25)
    label.add_argument("--pace", type=float, default=20.0, help="seconds between calls")
    label.add_argument("--seed", type=int, default=0)
    label.add_argument(
        "--skip-dataset-labelled",
        action="store_true",
        help="leave out synthetic units, which carry their own labels",
    )
    compare = sub.add_parser("compare")
    compare.add_argument("a", type=Path)
    compare.add_argument("b", type=Path)
    totals = sub.add_parser("totals")
    totals.add_argument("out", type=Path)
    args = parser.parse_args()

    if args.command == "compare":
        print(
            json.dumps(
                labeling.agreement(labeling.read_labels(args.a), labeling.read_labels(args.b)),
                indent=2,
            )
        )
        return
    if args.command == "totals":
        print(json.dumps(labeling.ledger_totals(args.out), indent=2))
        return
    units = labeling.read_units(args.units)
    if args.skip_dataset_labelled:
        units = [u for u in units if u.get("labels_from") != "dataset"]
    if args.only:
        wanted = [line.strip() for line in open(args.only) if line.strip()]
        by_id = {u["unit_id"]: u for u in units}
        units = [by_id[i] for i in wanted if i in by_id]
    else:
        units = labeling.stratified(units, seed=args.seed)
    caller = labeling.codex_caller(args.model, args.effort)
    counts = labeling.run(
        units,
        args.out,
        caller,
        model=args.model,
        effort=args.effort,
        limit=args.limit,
        batch_size=args.batch_size,
        pace=args.pace,
    )
    print(json.dumps({"run": counts, "ledger": labeling.ledger_totals(args.out)}, indent=2))


if __name__ == "__main__":
    main()
