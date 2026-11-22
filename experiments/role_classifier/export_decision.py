"""Write a trained decision model as the directory the roles stage loads (`set_roles.decision`).

    python export_decision.py RUN_DIR OUT_DIR --name role-decision --version v1 \
        --table-context 2 --text-context 4

RUN_DIR is `train_decision.py`'s `--out`: `best.pt` (the `Decision` module's state dict),
`config.json` and `tokenizer/`. The context versions are the ones `build_rows.py` built the
training rows with (`table_context.TABLE_CONTEXT_VERSION`, `prose_context.PROSE_CONTEXT_VERSION`
at that commit); the stage refuses the model once this code builds others. Needs the base
model's config in the HF cache (`--base` in the run's config).
"""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

import torch
from safetensors.torch import save_file
from transformers import AutoConfig, AutoModel

from ingestion_workflow.services.set_roles.decision import CARD, HEADS, HEADS_FILE


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("run", type=Path)
    parser.add_argument("out", type=Path)
    parser.add_argument("--name", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--table-context", type=int, required=True)
    parser.add_argument("--text-context", type=int, required=True)
    args = parser.parse_args()
    config = json.loads((args.run / "config.json").read_text())
    if config.get("lora"):
        raise SystemExit("a LoRA run is not an encoder: the roles stage loads encoders only")
    state = torch.load(args.run / "best.pt", map_location="cpu")
    encoder = {k[len("encoder."):]: v for k, v in state.items() if k.startswith("encoder.")}
    heads = {k[len("heads."):]: v.contiguous() for k, v in state.items() if k.startswith("heads.")}
    if {k.split(".")[0] for k in heads} != set(HEADS):
        raise SystemExit(f"the run's heads {sorted(heads)} are not {HEADS}")
    model = AutoModel.from_config(AutoConfig.from_pretrained(config["base"]))
    model.load_state_dict(encoder, strict=True)
    args.out.mkdir(parents=True, exist_ok=False)
    model.save_pretrained(args.out / "encoder", safe_serialization=True)
    save_file(heads, str(args.out / HEADS_FILE))
    shutil.copytree(args.run / "tokenizer", args.out / "tokenizer")
    card = {
        "name": args.name,
        "version": args.version,
        "base": config["base"],
        "max_length": config["max_length"],
        "roles": config["roles"],
        "kinds": config["kinds"],
        "sources": config["sources"],
        "context_versions": {"table": args.table_context, "text": args.text_context},
        "training": {k: config.get(k) for k in ("rows", "eval", "epochs", "lr", "batch", "accum",
                                                 "optim", "freeze_embeddings", "seed", "train_s")},
    }
    (args.out / CARD).write_text(json.dumps(card, indent=2) + "\n")
    print(json.dumps({"out": str(args.out), **{k: card[k] for k in ("name", "version", "base")}}))


if __name__ == "__main__":
    main()
