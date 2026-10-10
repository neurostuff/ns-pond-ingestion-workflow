"""Fine-tune the set-role encoder on export_rows.py's encoder.jsonl.

    CUDA_VISIBLE_DEVICES=0 ~/venv-train/bin/python train_encoder.py encoder.jsonl MODEL_DIR \
        [--base microsoft/BiomedNLP-BiomedBERT-base-uncased-abstract-fulltext] [--epochs 4]

One model for table and prose sets: every input starts with its origin, and
the role fields are study_schema's. Four heads (`services.set_roles.model`):
whether the numbers are coordinates, the `CoordinateRole` and the `AnchorKind`
(class-weighted cross-entropy so rare ones count; the kind on anchors only), and
the prior-study flag. Rows are split by article (`export.split_of`); evaluation
is on real articles only (a synthetic row's label is its generator's), per role,
per anchor kind, per origin and for the prior flag. The saved directory is what `role_model`
points at; it records the context versions it was trained on.
"""

from __future__ import annotations

import argparse
import collections
import json
import random
from pathlib import Path

from ingestion_workflow.services.set_roles import ANCHOR_KINDS, COORDINATE_ROLES, Prediction
from ingestion_workflow.services.set_roles import model as model_io


def _per_value(pairs):
    """Precision and recall per value of (gold, predicted) pairs."""
    out = {}
    for value in sorted({v for pair in pairs for v in pair}, key=str):
        tp = sum(1 for g, p in pairs if g == value and p == value)
        gold = sum(1 for g, _ in pairs if g == value)
        pred = sum(1 for _, p in pairs if p == value)
        out[str(value)] = {
            "n": gold,
            "precision": round(tp / pred, 3) if pred else None,
            "recall": round(tp / gold, 3) if gold else None,
        }
    return out


def evaluate(model, tokenizer, rows, device):
    if not rows:
        return {}
    out = [
        Prediction(**fields)
        for fields in model_io.predict_batch(
            model, tokenizer, [r["text"] for r in rows], device=device
        )
    ]
    report = {}
    for origin in ("table", "text", "all"):
        pairs = [(r, p) for r, p in zip(rows, out) if origin == "all" or r["origin"] == origin]
        if not pairs:
            continue
        # A set judged not coordinates has role None.
        roles = [
            (r["role"], p.role if p.coordinates_probability >= 0.5 else None) for r, p in pairs
        ]
        kinds = [(r["anchor_kind"], p.anchor_kind) for r, p in pairs if r["role"] == "anchor"]
        prior = [
            (r["from_prior_study"], p.prior_probability >= 0.5)
            for r, p in pairs
            if r["role"] is not None
        ]
        tp = sum(1 for g, p in prior if g and p)
        report[origin] = {
            "n": len(pairs),
            "role_accuracy": round(sum(g == p for g, p in roles) / len(roles), 3),
            "roles": _per_value(roles),
            "anchor_kinds": _per_value(kinds),
            "prior": {
                "n": sum(g for g, _ in prior),
                "precision": round(tp / max(1, sum(p for _, p in prior)), 3),
                "recall": round(tp / max(1, sum(g for g, _ in prior)), 3),
            },
        }
    return report


def _weights(torch, values, counts, n, device):
    """Square-root inverse-frequency class weights."""
    return torch.tensor(
        [(n / (len(values) * counts[v])) ** 0.5 if counts[v] else 0.0 for v in values],
        device=device,
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("rows", type=Path)
    parser.add_argument("out", type=Path)
    parser.add_argument(
        "--base", default="microsoft/BiomedNLP-BiomedBERT-base-uncased-abstract-fulltext"
    )
    parser.add_argument("--epochs", type=int, default=4)
    parser.add_argument("--lr", type=float, default=3e-5)
    parser.add_argument("--batch-size", type=int, default=16)
    parser.add_argument("--max-length", type=int, default=512)
    parser.add_argument("--version", default="1")
    args = parser.parse_args()

    import torch  # noqa: PLC0415
    from transformers import (  # noqa: PLC0415
        AutoModel,
        AutoTokenizer,
        get_linear_schedule_with_warmup,
    )

    rows = [json.loads(line) for line in open(args.rows, encoding="utf-8") if line.strip()]
    train = [r for r in rows if r["split"] == "train"]
    real = [r for r in rows if r.get("label_source") != "dataset"]
    val = [r for r in real if r["split"] == "val"]
    test = [r for r in real if r["split"] == "test"]
    counts = collections.Counter(r["role"] for r in train)
    kind_counts = collections.Counter(r["anchor_kind"] for r in train if r["role"] == "anchor")
    print(
        "train",
        len(train),
        dict(counts),
        dict(kind_counts),
        "val",
        len(val),
        "test",
        len(test),
        flush=True,
    )
    device = "cuda" if torch.cuda.is_available() else "cpu"
    tokenizer = AutoTokenizer.from_pretrained(args.base)
    model = model_io.build_model(AutoModel.from_pretrained(args.base)).to(device)
    role_index = {v: i for i, v in enumerate(COORDINATE_ROLES)}
    kind_index = {v: i for i, v in enumerate(ANCHOR_KINDS)}
    role_loss = torch.nn.CrossEntropyLoss(
        weight=_weights(torch, COORDINATE_ROLES, counts, len(train), device)
    )
    kind_loss = torch.nn.CrossEntropyLoss(
        weight=_weights(torch, ANCHOR_KINDS, kind_counts, sum(kind_counts.values()), device)
    )
    binary_loss = torch.nn.BCEWithLogitsLoss()
    optimiser = torch.optim.AdamW(model.parameters(), lr=args.lr, weight_decay=0.01)
    steps = args.epochs * ((len(train) + args.batch_size - 1) // args.batch_size)
    schedule = get_linear_schedule_with_warmup(optimiser, int(0.06 * steps), steps)
    rng = random.Random(0)
    for epoch in range(args.epochs):
        model.train()
        rng.shuffle(train)
        total = 0.0
        for start in range(0, len(train), args.batch_size):
            batch = train[start : start + args.batch_size]
            enc = tokenizer(
                [r["text"] for r in batch],
                truncation=True,
                max_length=args.max_length,
                padding=True,
                return_tensors="pt",
            ).to(device)
            coords, roles, kinds, prior = model(enc["input_ids"], enc["attention_mask"])
            loss = binary_loss(
                coords, torch.tensor([float(r["role"] is not None) for r in batch], device=device)
            )
            # The role and prior heads learn from coordinates only; the kind head from anchors.
            have = [i for i, r in enumerate(batch) if r["role"] is not None]
            anchors = [i for i in have if batch[i]["role"] == "anchor"]
            if have:
                loss = (
                    loss
                    + role_loss(
                        roles[have],
                        torch.tensor([role_index[batch[i]["role"]] for i in have], device=device),
                    )
                    + binary_loss(
                        prior[have],
                        torch.tensor(
                            [float(batch[i]["from_prior_study"]) for i in have], device=device
                        ),
                    )
                )
            if anchors:
                loss = loss + kind_loss(
                    kinds[anchors],
                    torch.tensor(
                        [kind_index[batch[i]["anchor_kind"]] for i in anchors], device=device
                    ),
                )
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            optimiser.step()
            schedule.step()
            optimiser.zero_grad()
            total += loss.item()
        model.eval()
        print(
            f"epoch {epoch + 1} loss {total / max(1, len(train) // args.batch_size):.4f} "
            f"val {evaluate(model, tokenizer, val, device).get('all', {}).get('role_accuracy')}",
            flush=True,
        )
    report = {
        "val": evaluate(model, tokenizer, val, device),
        "test": evaluate(model, tokenizer, test, device),
    }
    model_io.save(
        model,
        tokenizer,
        args.out,
        name="set-roles",
        version=args.version,
        base_model=args.base,
        extra={"train_rows": len(train), "evaluation": report},
    )
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
