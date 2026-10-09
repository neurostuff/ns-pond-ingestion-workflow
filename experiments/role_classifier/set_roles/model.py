"""The set-role encoder: a pretrained text encoder with two heads.

One head picks a label from `ROLE_LABELS`; the other says whether the
coordinates come from another publication. Both read the encoder's first-token
embedding of the serialised context. torch and transformers are imported only
here, and only when a model is loaded or trained, so the pipeline runs without
them when no role model is configured.

A saved model is a directory:

    encoder/        the fine-tuned encoder (save_pretrained)
    tokenizer/      its tokenizer
    heads.pt        the two heads' weights
    set_roles.json  labels, context version, name and version, base model
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional, Sequence

from .context import CONTEXT_VERSION
from .labels import ROLE_LABELS

META_FILE = "set_roles.json"


def _torch():
    import torch  # noqa: PLC0415 - optional dependency, see module docstring

    return torch


def build_model(base: Any, n_labels: int = len(ROLE_LABELS), dropout: float = 0.1):
    """Wrap an encoder (a transformers `PreTrainedModel`) with the two heads."""
    torch = _torch()
    nn = torch.nn

    class SetRoleEncoder(nn.Module):
        def __init__(self) -> None:
            super().__init__()
            self.encoder = base
            hidden = base.config.hidden_size
            self.dropout = nn.Dropout(dropout)
            self.role_head = nn.Linear(hidden, n_labels)
            self.prior_head = nn.Linear(hidden, 1)

        def forward(self, input_ids, attention_mask):
            out = self.encoder(input_ids=input_ids, attention_mask=attention_mask)
            pooled = self.dropout(out.last_hidden_state[:, 0])
            return self.role_head(pooled), self.prior_head(pooled).squeeze(-1)

    return SetRoleEncoder()


def save(model, tokenizer, path: Path, *, name: str, version: str, base_model: str,
         extra: Optional[Dict[str, Any]] = None) -> Path:
    torch = _torch()
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    model.encoder.save_pretrained(path / "encoder")
    tokenizer.save_pretrained(path / "tokenizer")
    torch.save({"role_head": model.role_head.state_dict(), "prior_head": model.prior_head.state_dict()},
               path / "heads.pt")
    meta = {"name": name, "version": version, "base_model": base_model, "labels": list(ROLE_LABELS),
            "context_version": CONTEXT_VERSION, **(extra or {})}
    (path / META_FILE).write_text(json.dumps(meta, indent=2), encoding="utf-8")
    return path


def read_meta(path: Path) -> Dict[str, Any]:
    return json.loads((Path(path) / META_FILE).read_text(encoding="utf-8"))


def load(path: Path, device: str = "cpu"):
    """(model, tokenizer, meta) from a directory `save` wrote."""
    torch = _torch()
    from transformers import AutoModel, AutoTokenizer  # noqa: PLC0415

    path = Path(path)
    meta = read_meta(path)
    if meta.get("context_version") != CONTEXT_VERSION:
        raise ValueError(
            f"{path} was trained on context version {meta.get('context_version')}, "
            f"this code builds version {CONTEXT_VERSION}; retrain it")
    if list(meta.get("labels", [])) != list(ROLE_LABELS)[: len(meta.get("labels", []))]:
        raise ValueError(f"{path} was trained on labels {meta.get('labels')}, not a prefix of ROLE_LABELS")
    model = build_model(AutoModel.from_pretrained(path / "encoder"), n_labels=len(meta["labels"]))
    heads = torch.load(path / "heads.pt", map_location=device)
    model.role_head.load_state_dict(heads["role_head"])
    model.prior_head.load_state_dict(heads["prior_head"])
    model.to(device).eval()
    return model, AutoTokenizer.from_pretrained(path / "tokenizer"), meta


def predict_batch(model, tokenizer, texts: Sequence[str], labels: Sequence[str], *, device: str = "cpu",
                  max_length: int = 512, batch_size: int = 32):
    """[(label probabilities, prior-study probability)] for each text."""
    torch = _torch()
    out = []
    with torch.no_grad():
        for start in range(0, len(texts), batch_size):
            batch = tokenizer(list(texts[start:start + batch_size]), truncation=True, max_length=max_length,
                              padding=True, return_tensors="pt").to(device)
            role_logits, prior_logits = model(batch["input_ids"], batch["attention_mask"])
            role_probs = torch.softmax(role_logits, dim=-1).cpu().tolist()
            prior_probs = torch.sigmoid(prior_logits).cpu().tolist()
            for probs, prior in zip(role_probs, prior_probs):
                out.append(({label: p for label, p in zip(labels, probs)}, float(prior)))
    return out
