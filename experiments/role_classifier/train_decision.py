"""Fine-tune the set-role decision model: a MiniCheck encoder with five heads.

The heads read the encoder's first-token embedding of the set's serialised
context (the roles stage's input):

    coordinates  the numbers locate something in a brain or on a head        (binary)
    space        human MNI/Talairach mm, or any other frame                  (binary; coordinates only)
    role         result, anchor, localization, reference, other             (5-way; coordinates only)
    kind         roi, seed, stimulation_target, node                         (4-way; anchors only)
    source       this_study, prior_study, atlas_or_database                  (3-way; coordinates only)

With --lora the backbone is a causal LM (Bespoke-MiniCheck-7B, InternLM2) with LoRA adapters,
pooled at the last token (left padding); only the adapters and heads train and are saved.

A head's loss counts only the rows its question applies to and that are
labelled. The upload decision is coordinates and space=template and role in
{result, anchor}.

    python train_decision.py --rows rows.jsonl --eval eval_rows.jsonl --base lytang/MiniCheck-DeBERTa-v3-Large \
        --out runs/deberta [--epochs 3 --lr 2e-5 --batch 8 --accum 2 --max-length 512]
"""

from __future__ import annotations

import argparse
import json
import math
import random
import time
from pathlib import Path

import torch
from torch import nn
from transformers import AutoModel, AutoModelForCausalLM, AutoTokenizer, get_linear_schedule_with_warmup

ROLES = ("result", "anchor", "localization", "reference", "other")
KINDS = ("roi", "seed", "stimulation_target", "node")
SOURCES = ("this_study", "prior_study", "atlas_or_database")
IGNORE = -100
#: minicheck-ft's recipe for Bespoke-MiniCheck-7B (InternLM2 module names)
LORA = dict(r=16, lora_alpha=32, lora_dropout=0.05, target_modules=["wqkv", "wo", "w1", "w2", "w3"])


def targets(row):
    """(coordinates, space, role, kind, source), IGNORE where the question does not apply."""
    coords = 1 if row["coordinates"] else 0
    on = bool(row["coordinates"])
    space = {"template": 1, "other": 0}.get(row.get("space"), IGNORE) if on else IGNORE
    role = ROLES.index(row["role"]) if on and row.get("role") in ROLES else IGNORE
    kind = KINDS.index(row["anchor_kind"]) if row.get("role") == "anchor" and row.get("anchor_kind") in KINDS else IGNORE
    source = SOURCES.index(row["source"]) if on and row.get("source") in SOURCES else IGNORE
    return coords, space, role, kind, source


class Decision(nn.Module):
    def __init__(self, base: str, dropout: float = 0.1, lora: bool = False):
        super().__init__()
        self.lora = lora
        if lora:
            from peft import LoraConfig, get_peft_model
            from transformers.dynamic_module_utils import get_class_from_dynamic_module
            cfg = get_class_from_dynamic_module("configuration_internlm2.InternLM2Config", base)
            cfg._rope_scaling_validation = lambda self: None
            lm = AutoModelForCausalLM.from_pretrained(base, trust_remote_code=True, torch_dtype=torch.bfloat16)
            lm.config.use_cache = False
            self.peft = get_peft_model(lm, LoraConfig(task_type="CAUSAL_LM", **LORA))
            self.encoder = self.peft.get_base_model().model      # the transformer, LoRA layers injected
        else:
            self.encoder = AutoModel.from_pretrained(base)
        h = self.encoder.config.hidden_size
        self.drop = nn.Dropout(dropout)
        self.heads = nn.ModuleDict({
            "coordinates": nn.Linear(h, 2), "space": nn.Linear(h, 2),
            "role": nn.Linear(h, len(ROLES)), "kind": nn.Linear(h, len(KINDS)),
            "source": nn.Linear(h, len(SOURCES)),
        })

    def forward(self, input_ids, attention_mask):
        if self.lora:  # left padding: the last position reads the whole input
            pos = (attention_mask.cumsum(-1) - 1).clamp(min=0)
            h = self.encoder(input_ids=input_ids, attention_mask=attention_mask, position_ids=pos, use_cache=False)[0]
            pooled = self.drop(h[:, -1].float())
        else:
            pooled = self.drop(self.encoder(input_ids=input_ids, attention_mask=attention_mask).last_hidden_state[:, 0])
        return {k: head(pooled) for k, head in self.heads.items()}


HEAD_ORDER = ("coordinates", "space", "role", "kind", "source")


def batches(rows, tok, size, max_length, shuffle, seed=0):
    order = list(range(len(rows)))
    if shuffle:
        random.Random(seed).shuffle(order)
    for s in range(0, len(order), size):
        chunk = [rows[i] for i in order[s : s + size]]
        enc = tok([r["input"] for r in chunk], truncation=True, max_length=max_length, padding=True, return_tensors="pt")
        y = torch.tensor([targets(r) for r in chunk], dtype=torch.long)
        yield chunk, enc, y


def predict(model, rows, tok, device, max_length, size=32):
    model.eval()
    out = []
    with torch.no_grad(), torch.autocast(device.type, dtype=torch.bfloat16, enabled=device.type == "cuda"):
        for chunk, enc, _ in batches(rows, tok, size, max_length, shuffle=False):
            logits = model(enc["input_ids"].to(device), enc["attention_mask"].to(device))
            probs = {k: torch.softmax(v.float(), -1).cpu() for k, v in logits.items()}
            for i, r in enumerate(chunk):
                p = {k: probs[k][i].tolist() for k in probs}
                coords = p["coordinates"][1] >= 0.5
                role = ROLES[max(range(len(ROLES)), key=lambda j: p["role"][j])]
                out.append({
                    "id": r["id"],
                    "coordinates": coords, "p_coordinates": p["coordinates"][1],
                    "space": ("template" if p["space"][1] >= 0.5 else "other") if coords else None,
                    "p_template": p["space"][1],
                    "role": role if coords else None, "p_role": dict(zip(ROLES, p["role"])),
                    "anchor_kind": KINDS[max(range(len(KINDS)), key=lambda j: p["kind"][j])] if coords and role == "anchor" else None,
                    "source": SOURCES[max(range(len(SOURCES)), key=lambda j: p["source"][j])] if coords else None,
                })
    return out


def score(rows, preds):
    """Accuracy of each head over the rows it applies to, plus the upload decision."""
    by = {p["id"]: p for p in preds}
    acc = {k: [0, 0] for k in ("coordinates", "space", "role", "kind", "source", "upload")}
    for r in rows:
        p = by[r["id"]]
        acc["coordinates"][0] += p["coordinates"] == bool(r["coordinates"]); acc["coordinates"][1] += 1
        if r["coordinates"] and r.get("space"):
            acc["space"][0] += p["space"] == r["space"]; acc["space"][1] += 1
        if r["coordinates"] and r.get("role"):
            acc["role"][0] += p["role"] == r["role"]; acc["role"][1] += 1
        if r.get("role") == "anchor" and r.get("anchor_kind"):
            acc["kind"][0] += p["anchor_kind"] == r["anchor_kind"]; acc["kind"][1] += 1
        if r["coordinates"] and r.get("source"):
            acc["source"][0] += p["source"] == r["source"]; acc["source"][1] += 1
        if r["coordinates"] and r.get("space") == "template" and not r.get("role"):
            continue
        gold_up = bool(r["coordinates"]) and r.get("space") == "template" and r.get("role") in ("result", "anchor")
        pred_up = p["coordinates"] and p["space"] == "template" and p["role"] in ("result", "anchor")
        acc["upload"][0] += gold_up == pred_up; acc["upload"][1] += 1
    return {k: (round(a / n, 4) if n else None, n) for k, (a, n) in acc.items()}


def score_by_origin(rows, preds):
    """`score` over all rows, then over each origin's rows (table, text): one model reads both."""
    out = {"all": score(rows, preds)}
    for origin in sorted({r.get("origin") for r in rows} - {None}):
        out[origin] = score([r for r in rows if r.get("origin") == origin], preds)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=Path, required=True)
    ap.add_argument("--eval", type=Path)
    ap.add_argument("--base", default="lytang/MiniCheck-DeBERTa-v3-Large")
    ap.add_argument("--tokenizer", default=None, help="defaults to --base")
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--epochs", type=int, default=3)
    ap.add_argument("--lr", type=float, default=2e-5)
    ap.add_argument("--batch", type=int, default=8)
    ap.add_argument("--accum", type=int, default=2)
    ap.add_argument("--max-length", type=int, default=512)
    ap.add_argument("--limit", type=int, default=0, help="rows per split, for a smoke test")
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--optim", choices=("adamw", "adamw8bit"), default="adamw",
                    help="adamw8bit (bitsandbytes) fits a large encoder on an 8 GB card")
    ap.add_argument("--freeze-embeddings", action="store_true", help="train every layer but the token embeddings")
    ap.add_argument("--grad-ckpt", action="store_true", help="gradient checkpointing: less memory, ~30%% slower")
    ap.add_argument("--predict-batch", type=int, default=32, help="rows per scoring batch (8 fits an 8 GB card)")
    ap.add_argument("--lora", action="store_true", help="causal-LM backbone (Bespoke-MiniCheck-7B) with LoRA adapters")
    args = ap.parse_args()
    torch.manual_seed(args.seed)
    args.out.mkdir(parents=True, exist_ok=True)
    rows = [json.loads(l) for l in open(args.rows)]
    train = [r for r in rows if r["split"] == "train"]
    val = [r for r in rows if r["split"] == "val"]
    test = [json.loads(l) for l in open(args.eval)] if args.eval else []
    if args.limit:
        train, val, test = train[: args.limit], val[: args.limit], test[: args.limit]
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    tok = AutoTokenizer.from_pretrained(args.tokenizer or args.base, trust_remote_code=args.lora)
    if args.lora:
        tok.padding_side = "left"
        tok.pad_token = tok.pad_token or tok.eos_token
    model = Decision(args.base, lora=args.lora).to(device)
    if args.freeze_embeddings:
        model.encoder.get_input_embeddings().weight.requires_grad_(False)
    if args.grad_ckpt and args.lora:  # minicheck-ft's setup for InternLM2's remote code
        model.peft.base_model.model.gradient_checkpointing_enable()
        model.peft.base_model.model.enable_input_require_grads()
    elif args.grad_ckpt:
        model.encoder.gradient_checkpointing_enable(gradient_checkpointing_kwargs={"use_reentrant": False})
    params = [p for p in model.parameters() if p.requires_grad]
    if args.optim == "adamw8bit":
        import bitsandbytes as bnb
        opt = bnb.optim.AdamW8bit(params, lr=args.lr, weight_decay=0.01)
    else:
        opt = torch.optim.AdamW(params, lr=args.lr, weight_decay=0.01)
    steps = math.ceil(len(train) / args.batch / args.accum) * args.epochs
    sched = get_linear_schedule_with_warmup(opt, int(0.1 * steps), steps)
    loss_fn = nn.CrossEntropyLoss(ignore_index=IGNORE)
    log = open(args.out / "log.jsonl", "a")
    started, best = time.time(), -1.0
    for epoch in range(args.epochs):
        model.train()
        for step, (_, enc, y) in enumerate(batches(train, tok, args.batch, args.max_length, True, args.seed + epoch)):
            with torch.autocast(device.type, dtype=torch.bfloat16, enabled=device.type == "cuda"):
                logits = model(enc["input_ids"].to(device), enc["attention_mask"].to(device))
            y = y.to(device)
            loss = sum(
                loss_fn(logits[h].float(), y[:, i]) if (y[:, i] != IGNORE).any() else logits[h].sum() * 0
                for i, h in enumerate(HEAD_ORDER)
            )
            (loss / args.accum).backward()
            if (step + 1) % args.accum == 0:
                nn.utils.clip_grad_norm_(model.parameters(), 1.0)
                opt.step(); sched.step(); opt.zero_grad()
            if step % 50 == 0:
                mem = round(torch.cuda.max_memory_allocated() / 2**30, 2) if device.type == "cuda" else None
                print(json.dumps({"epoch": epoch, "step": step, "loss": round(loss.item(), 4),
                                  "elapsed_s": round(time.time() - started), "max_mem_gb": mem}), flush=True)
        vs = score_by_origin(val, predict(model, val, tok, device, args.max_length, args.predict_batch)) if val else {"all": {}}
        mean = sum(v[0] for v in vs["all"].values() if v[0] is not None) / max(1, sum(v[0] is not None for v in vs["all"].values()))
        rec = {"epoch": epoch, "val": vs, "val_mean": round(mean, 4), "elapsed_s": round(time.time() - started)}
        print(json.dumps(rec), flush=True); log.write(json.dumps(rec) + "\n"); log.flush()
        if mean > best:
            best = mean
            state = model.state_dict()
            if args.lora:  # the adapters and heads; the 7B base stays as downloaded
                keep = {n for n, p in model.named_parameters() if p.requires_grad}
                state = {k: v for k, v in state.items() if k in keep}
            torch.save(state, args.out / "best.pt")
    model.load_state_dict(torch.load(args.out / "best.pt", map_location=device), strict=not args.lora)
    tok.save_pretrained(args.out / "tokenizer")
    (args.out / "config.json").write_text(json.dumps({**vars(args), "rows": str(args.rows), "eval": str(args.eval),
                                                      "out": str(args.out), "roles": ROLES, "kinds": KINDS,
                                                      "sources": SOURCES, "train_s": round(time.time() - started)}, indent=2, default=str))
    if test:
        preds = predict(model, test, tok, device, args.max_length, args.predict_batch)
        with open(args.out / "eval_predictions.jsonl", "w") as f:
            for p in preds:
                f.write(json.dumps(p) + "\n")
        ts = score_by_origin(test, preds)
        print(json.dumps({"eval": ts}), flush=True)
        log.write(json.dumps({"eval": ts}) + "\n")


if __name__ == "__main__":
    main()
