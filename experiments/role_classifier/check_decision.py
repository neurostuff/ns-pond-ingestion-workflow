"""Check an exported model against the predictions its training run wrote.

    python check_decision.py MODEL_DIR EVAL_ROWS PREDICTIONS [--device cuda]

Runs `set_roles.decision.DecisionModel` (what the roles stage runs) on each eval row's
`input` and compares every field with `train_decision.py`'s `eval_predictions.jsonl` for the
same rows. Prints the disagreements and the largest probability difference; exits 1 on a
disagreement.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from ingestion_workflow.services.set_roles import decision

FIELDS = ("role", "anchor_kind", "space", "source")


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("model", type=Path)
    parser.add_argument("rows", type=Path)
    parser.add_argument("predictions", type=Path)
    parser.add_argument("--device", default="auto")
    args = parser.parse_args()
    source, why = decision.state(str(args.model))
    if source is None:
        raise SystemExit(why)
    model = decision.DecisionModel(str(args.model), source=source, device=args.device)
    rows = [json.loads(line) for line in open(args.rows, encoding="utf-8")]
    want = {p["id"]: p for p in map(json.loads, open(args.predictions, encoding="utf-8"))}
    got = model.predict([r["input"] for r in rows])
    differ, worst = [], 0.0
    for row, p in zip(rows, got):
        w = want[row["id"]]
        mine = {"role": p.role, "anchor_kind": p.anchor_kind, "space": p.space, "source": p.source}
        theirs = {k: w[k] for k in FIELDS}
        if mine != theirs:
            differ.append((row["id"], theirs, mine))
        worst = max(worst, abs(p.coordinates_probability - w["p_coordinates"]),
                    *(abs(p.role_probabilities[r] - w["p_role"][r]) for r in w["p_role"]))
    print(json.dumps({"model": source, "rows": len(rows), "disagree": len(differ),
                      "max_probability_difference": round(worst, 5)}))
    for d in differ[:20]:
        print(json.dumps(d))
    sys.exit(1 if differ else 0)


if __name__ == "__main__":
    main()
