"""The set-role encoder: a pretrained text encoder with four heads.

One says whether the numbers are coordinates at all, one picks the
`CoordinateRole`, one the `AnchorKind` (trained on anchors only), and one says
whether the coordinates come from another publication. All read the encoder's first-token
embedding of the serialised context. torch and transformers are imported only
here, and only when a model is loaded or trained, so the pipeline runs without
them when no role model is configured.

A saved model is a directory:

    encoder/        the fine-tuned encoder (save_pretrained)
    tokenizer/      its tokenizer
    heads.pt        the heads' weights
    set_roles.json  roles and anchor kinds, each origin's context version, name and
                    version, base model

One model reads table and prose sets alike: each input starts with its origin
(`[ORIGIN] table` or `[ORIGIN] text`), and the role vocabulary is shared.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional, Sequence

from .labels import ANCHOR_KINDS, COORDINATE_ROLES
from .prose_context import PROSE_CONTEXT_VERSION
from .table_context import TABLE_CONTEXT_VERSION

META_FILE = "set_roles.json"

#: The context version of each origin this code builds.
CONTEXT_VERSIONS = {"table": TABLE_CONTEXT_VERSION, "text": PROSE_CONTEXT_VERSION}


def check_meta(meta: Dict[str, Any], path: Any = "the model") -> None:
    """Refuse a model trained on contexts shaped differently from the ones this code builds."""
    trained = meta.get("context_versions") or {}
    if trained != CONTEXT_VERSIONS:
        raise ValueError(
            f"{path} was trained on context versions {trained or None}; this code builds "
            f"{CONTEXT_VERSIONS}; retrain it"
        )
    for key, values in (("roles", COORDINATE_ROLES), ("anchor_kinds", ANCHOR_KINDS)):
        if list(meta.get(key) or []) != list(values):
            raise ValueError(
                f"{path} was trained on {key} {meta.get(key)}, not study_schema's {list(values)}"
            )


def _torch():
    import torch  # noqa: PLC0415 - optional dependency, see module docstring

    return torch


HEADS = ("coordinates_head", "role_head", "kind_head", "prior_head")


def build_model(base: Any, dropout: float = 0.1):
    """Wrap an encoder (a transformers `PreTrainedModel`) with the heads."""
    torch = _torch()
    nn = torch.nn

    class SetRoleEncoder(nn.Module):
        def __init__(self) -> None:
            super().__init__()
            self.encoder = base
            hidden = base.config.hidden_size
            self.dropout = nn.Dropout(dropout)
            self.coordinates_head = nn.Linear(hidden, 1)
            self.role_head = nn.Linear(hidden, len(COORDINATE_ROLES))
            self.kind_head = nn.Linear(hidden, len(ANCHOR_KINDS))
            self.prior_head = nn.Linear(hidden, 1)

        def forward(self, input_ids, attention_mask):
            """(coordinates logit, role logits, anchor-kind logits, prior-study logit)."""
            out = self.encoder(input_ids=input_ids, attention_mask=attention_mask)
            pooled = self.dropout(out.last_hidden_state[:, 0])
            return (
                self.coordinates_head(pooled).squeeze(-1),
                self.role_head(pooled),
                self.kind_head(pooled),
                self.prior_head(pooled).squeeze(-1),
            )

    return SetRoleEncoder()


def save(
    model,
    tokenizer,
    path: Path,
    *,
    name: str,
    version: str,
    base_model: str,
    extra: Optional[Dict[str, Any]] = None,
) -> Path:
    torch = _torch()
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    model.encoder.save_pretrained(path / "encoder")
    tokenizer.save_pretrained(path / "tokenizer")
    torch.save(
        {head: getattr(model, head).state_dict() for head in HEADS},
        path / "heads.pt",
    )
    meta = {
        "name": name,
        "version": version,
        "base_model": base_model,
        "roles": list(COORDINATE_ROLES),
        "anchor_kinds": list(ANCHOR_KINDS),
        "context_versions": CONTEXT_VERSIONS,
        **(extra or {}),
    }
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
    check_meta(meta, path)
    model = build_model(AutoModel.from_pretrained(path / "encoder"))
    heads = torch.load(path / "heads.pt", map_location=device)
    for head in HEADS:
        getattr(model, head).load_state_dict(heads[head])
    model.to(device).eval()
    return model, AutoTokenizer.from_pretrained(path / "tokenizer"), meta


def predict_batch(
    model,
    tokenizer,
    texts: Sequence[str],
    *,
    device: str = "cpu",
    max_length: int = 512,
    batch_size: int = 32,
):
    """A `Prediction`'s fields (as a dict) for each text."""
    torch = _torch()
    out = []
    with torch.no_grad():
        for start in range(0, len(texts), batch_size):
            batch = tokenizer(
                list(texts[start : start + batch_size]),
                truncation=True,
                max_length=max_length,
                padding=True,
                return_tensors="pt",
            ).to(device)
            coords, roles, kinds, prior = model(batch["input_ids"], batch["attention_mask"])
            for c, r, k, p in zip(
                torch.sigmoid(coords).cpu().tolist(),
                torch.softmax(roles, dim=-1).cpu().tolist(),
                torch.softmax(kinds, dim=-1).cpu().tolist(),
                torch.sigmoid(prior).cpu().tolist(),
            ):
                out.append(
                    {
                        "coordinates_probability": float(c),
                        "role_probabilities": dict(zip(COORDINATE_ROLES, r)),
                        "kind_probabilities": dict(zip(ANCHOR_KINDS, k)),
                        "prior_probability": float(p),
                    }
                )
    return out
