"""Merge a peft adapter into its bf16 base and write a directory vLLM loads, on a cluster node.

    python merge.py BASE_DIR ADAPTER_DIR OUT_DIR [cpu|cuda]

Run with the machine's train python (torch, peft, transformers). The base is always loaded in
bf16, also for an adapter trained as QLoRA nf4. Non-weight files of the base are symlinked and
config.json is copied. Ported from pondie tacc/merge_adapter.py, which looked the base up by
model family; here the job names it.
"""

import json
import shutil
import sys
import time
from pathlib import Path

import torch
from peft import PeftModel
from transformers import AutoModelForImageTextToText

base, adapter, out = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3])
dev = sys.argv[4] if len(sys.argv) > 4 else "cpu"
t = time.time()
m = AutoModelForImageTextToText.from_pretrained(
    base, dtype=torch.bfloat16, device_map={"": dev if dev == "cpu" else 0})
for holder in (m, getattr(m, "model", None)):
    for attr in ("visual", "vision_tower", "vision_model"):
        if holder is not None and getattr(holder, attr, None) is not None:
            setattr(holder, attr, None)   # as in training, so the adapter's module names match
m = PeftModel.from_pretrained(m, str(adapter))
print(f"loaded {time.time() - t:.0f}s", flush=True)
m = m.merge_and_unload()
print(f"merged {time.time() - t:.0f}s", flush=True)
out.mkdir(parents=True, exist_ok=True)
m.save_pretrained(out, safe_serialization=True, max_shard_size="10GB")
print(f"saved {time.time() - t:.0f}s", flush=True)
for f in base.iterdir():
    d = out / f.name
    if f.name == "config.json":
        shutil.copy(f.resolve(), d)
    elif (d.exists() or f.name.startswith(("model.safetensors", "model-0"))
          or f.suffix in (".png", ".svg")):
        continue
    else:
        d.symlink_to(f.resolve())
# The multi-token-prediction head (mtp.*, vLLM's speculative method qwen3_5_mtp) has no module
# in the HF class, so save_pretrained drops it; copy it from the base unchanged (the adapter
# never touched it).
ixf = out / "model.safetensors.index.json"
bix = base / "model.safetensors.index.json"
if ixf.exists() and bix.exists():
    from safetensors import safe_open
    from safetensors.torch import save_file

    bmap = json.load(open(bix))["weight_map"]
    mtp = {}
    for k in sorted(k for k in bmap if k.startswith("mtp.")):
        with safe_open(str(base / bmap[k]), "pt") as f:
            mtp[k] = f.get_tensor(k)
    if mtp:
        save_file(mtp, str(out / "model-mtp.safetensors"), metadata={"format": "pt"})
        oix = json.load(open(ixf))
        oix["weight_map"].update({k: "model-mtp.safetensors" for k in mtp})
        ixf.write_text(json.dumps(oix, indent=2))
        print(f"mtp: copied {len(mtp)} tensors", flush=True)


def names(d):
    """The weight names of a model directory: the merged ones must be the base's, since vLLM
    loads by name."""
    ix = d / "model.safetensors.index.json"
    if ix.exists():
        return set(json.load(open(ix))["weight_map"])
    from safetensors import safe_open

    with safe_open(str(d / "model.safetensors"), "pt") as f:
        return set(f.keys())


a, b = names(base), names(out)
print(f"weights: base {len(a)} merged {len(b)} only-in-base {len(a - b)} "
      f"only-in-merged {len(b - a)}", flush=True)
print("  e.g. only-in-base", sorted(a - b)[:3], "only-in-merged", sorted(b - a)[:3], flush=True)
bad = {x for x in (b - a) if "lora" in x}
assert not bad, bad
(out / ".ok").write_text("ok")
print("MERGE OK", flush=True)
