"""Fine-tune the set-role encoder on export_rows.py's encoder.jsonl.

    CUDA_VISIBLE_DEVICES=0 ~/venv-train/bin/python train_encoder.py encoder.jsonl MODEL_DIR \
        [--base microsoft/BiomedNLP-BiomedBERT-base-uncased-abstract-fulltext] [--epochs 4]

One model for table and prose sets: every input starts with its origin, and
the label vocabulary is shared. Two heads (`services.set_roles.model`): the
label, under class-weighted cross-entropy so rare roles count, and the
prior-study flag. Rows are split by article (`export.split_of`); evaluation is
on real articles only (a synthetic row's label is its generator's), per label,
per origin and for the prior flag. The saved directory is what `role_model`
points at; it records the context versions it was trained on.
"""

from __future__ import annotations

import argparse
import collections
import json
import random
from pathlib import Path

from ingestion_workflow.services.set_roles import ROLE_LABELS
from ingestion_workflow.services.set_roles import model as model_io


def evaluate(model, tokenizer, rows, device):
    if not rows:
        return {}
    out = model_io.predict_batch(
        model, tokenizer, [r["text"] for r in rows], ROLE_LABELS, device=device
    )
    report = {}
    for origin in ("table", "text", "all"):
        pairs = [(r, p) for r, p in zip(rows, out) if origin == "all" or r["origin"] == origin]
        if not pairs:
            continue
        per = {}
        for label in ROLE_LABELS:
            tp = sum(
                1 for r, (pr, _) in pairs if r["label"] == label and max(pr, key=pr.get) == label
            )
            gold = sum(1 for r, _ in pairs if r["label"] == label)
            pred = sum(1 for _, (pr, _) in pairs if max(pr, key=pr.get) == label)
            if gold or pred:
                per[label] = {
                    "n": gold,
                    "precision": round(tp / pred, 3) if pred else None,
                    "recall": round(tp / gold, 3) if gold else None,
                }
        prior = [(r["from_prior_study"], pp >= 0.5) for r, (_, pp) in pairs]
        tp = sum(1 for g, p in prior if g and p)
        report[origin] = {
            "n": len(pairs),
            "accuracy": round(
                sum(r["label"] == max(pr, key=pr.get) for r, (pr, _) in pairs) / len(pairs), 3
            ),
            "labels": per,
            "prior": {
                "n": sum(g for g, _ in prior),
                "precision": round(tp / max(1, sum(p for _, p in prior)), 3),
                "recall": round(tp / max(1, sum(g for g, _ in prior)), 3),
            },
        }
    return report


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
    counts = collections.Counter(r["label"] for r in train)
    print("train", len(train), dict(counts), "val", len(val), "test", len(test), flush=True)
    device = "cuda" if torch.cuda.is_available() else "cpu"
    tokenizer = AutoTokenizer.from_pretrained(args.base)
    model = model_io.build_model(AutoModel.from_pretrained(args.base)).to(device)
    index = {label: i for i, label in enumerate(ROLE_LABELS)}
    weights = torch.tensor(
        [
            (len(train) / (len(ROLE_LABELS) * counts[label])) ** 0.5 if counts[label] else 0.0
            for label in ROLE_LABELS
        ],
        device=device,
    )
    role_loss = torch.nn.CrossEntropyLoss(weight=weights)
    prior_loss = torch.nn.BCEWithLogitsLoss()
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
            role_logits, prior_logits = model(enc["input_ids"], enc["attention_mask"])
            loss = role_loss(
                role_logits, torch.tensor([index[r["label"]] for r in batch], device=device)
            ) + prior_loss(
                prior_logits,
                torch.tensor([float(r["from_prior_study"]) for r in batch], device=device),
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
            f"val {evaluate(model, tokenizer, val, device).get('all', {}).get('accuracy')}",
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
