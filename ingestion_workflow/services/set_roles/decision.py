"""The set-role decision model: one encoder with five heads, for table and prose sets alike.

It reads a set's serialised context (`table_context.serialize` or
`prose_context.serialize`, whose `[ORIGIN]` line says which) and answers five
questions from the encoder's first-token embedding:

    coordinates  the numbers locate something in a brain or on a head    (binary)
    space        human MNI/Talairach millimetres, or any other frame      (binary)
    role         result, anchor, localization, reference, other   (a CoordinateRole)
    kind         roi, seed, stimulation_target, node              (an AnchorKind; anchors only)
    source       this_study, prior_study, atlas_or_database

The model runs in the roles stage's own process (CPU or one small GPU), so
there is no server to reach. A model is a directory written by
`experiments/role_classifier/export_decision.py`:

    decision.json      name, version, label orders, max_length, the context versions trained on
    encoder/           the fine-tuned encoder (`save_pretrained`)
    heads.safetensors  each head's linear layer, `<head>.weight` / `<head>.bias`
    tokenizer/

Each answer is the most probable value of each head, as the model was scored:
coordinates and template space at p >= 0.5. torch and transformers are the
`roles` extra; they are imported when a model is loaded, not before.
"""

from __future__ import annotations

import hashlib
import importlib.util
import json
import queue
import threading
from concurrent.futures import Future
from dataclasses import dataclass
from pathlib import Path
from typing import List, Mapping, Optional, Sequence, Tuple

from ingestion_workflow.clients.model_server import ModelUnavailable

from .classifier import Prediction
from .labels import ANCHOR_KINDS, COORDINATE_ROLES, COORDINATE_SOURCES
from .prose_context import PROSE_CONTEXT_VERSION
from .table_context import TABLE_CONTEXT_VERSION

#: The context version of each origin this code builds: `table` sets and `text` (prose) sets.
CONTEXT_VERSIONS = {"table": TABLE_CONTEXT_VERSION, "text": PROSE_CONTEXT_VERSION}

CARD = "decision.json"
HEADS_FILE = "heads.safetensors"
HEADS = ("coordinates", "space", "role", "kind", "source")
#: The decision threshold of the two binary heads, as the model was scored.
THRESHOLD = 0.5
#: Rows per forward pass; the stage's articles share them.
BATCH = 32


@dataclass(frozen=True)
class Card:
    """What `decision.json` says about a model directory."""

    name: str
    version: str
    max_length: int
    roles: Tuple[str, ...]
    kinds: Tuple[str, ...]
    sources: Tuple[str, ...]
    #: The context version of each origin the model was trained on.
    context_versions: Mapping[str, int]

    @classmethod
    def read(cls, path: Path) -> "Card":
        raw = json.loads((Path(path) / CARD).read_text(encoding="utf-8"))
        return cls(
            name=raw["name"],
            version=raw["version"],
            max_length=int(raw["max_length"]),
            roles=tuple(raw["roles"]),
            kinds=tuple(raw["kinds"]),
            sources=tuple(raw["sources"]),
            context_versions={k: int(v) for k, v in raw["context_versions"].items()},
        )

    def check(self) -> None:
        """Refuse a model whose answers are not study_schema's, or whose inputs this code
        no longer builds."""
        for what, have, want in (("roles", self.roles, COORDINATE_ROLES),
                                 ("anchor kinds", self.kinds, ANCHOR_KINDS),
                                 ("sources", self.sources, COORDINATE_SOURCES)):
            if set(have) != set(want):
                raise ValueError(f"its {what} {list(have)} are not {list(want)}")
        stale = {o: v for o, v in CONTEXT_VERSIONS.items() if self.context_versions.get(o) != v}
        if stale:
            raise ValueError(
                "it was trained on context versions "
                + ", ".join(f"{o} {self.context_versions.get(o)}" for o in stale)
                + ", and this code builds "
                + ", ".join(f"{o} {v}" for o, v in stale.items())
            )


def identity(path: Path) -> str:
    """A digest of a model directory: card and heads by content, weights by name, size, time."""
    root = Path(path)
    digest = hashlib.sha256()
    for name in (CARD, HEADS_FILE, "encoder/config.json"):
        file = root / name
        if file.is_file():
            digest.update(name.encode() + hashlib.sha256(file.read_bytes()).digest())
    for weights in sorted((root / "encoder").glob("*.safetensors")):
        stat = weights.stat()
        digest.update(f"{weights.name}:{stat.st_size}:{stat.st_mtime_ns}".encode())
    return digest.hexdigest()


def state(path: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """(`<name>@<version>+<digest>`, None) for a usable model directory, or (None, why not)."""
    if not path:
        return None, "no role model is configured (role_model)"
    root = Path(path)
    where = f"the role model at {root}"
    if not (root / CARD).is_file():
        return None, f"{where} has no {CARD}"
    try:
        card = Card.read(root)
        card.check()
    except (ValueError, KeyError, TypeError) as exc:
        return None, f"{where} cannot be used: {exc}"
    missing = [m for m in ("torch", "transformers", "safetensors")
               if importlib.util.find_spec(m) is None]
    if missing:
        return None, f"{where} needs {', '.join(missing)}: install the `roles` extra"
    return f"{card.name}@{card.version}+{identity(root)[:12]}", None


def answer(card: Card, probabilities: Mapping[str, Sequence[float]]) -> Prediction:
    """One set's prediction from its five heads' probabilities, in the card's label orders."""
    p = {k: list(v) for k, v in probabilities.items()}
    p_coordinates, p_template = p["coordinates"][1], p["space"][1]
    role_p = dict(zip(card.roles, p["role"]))
    if p_coordinates < THRESHOLD:
        return Prediction(role=None, anchor_kind=None, role_probabilities=role_p,
                          space=None, source=None, coordinates_probability=p_coordinates)
    role = max(role_p, key=role_p.get)
    kinds = dict(zip(card.kinds, p["kind"]))
    sources = dict(zip(card.sources, p["source"]))
    return Prediction(
        role=role,
        anchor_kind=max(kinds, key=kinds.get) if role == "anchor" else None,
        role_probabilities=role_p,
        space="template" if p_template >= THRESHOLD else "other",
        source=max(sources, key=sources.get),
        coordinates_probability=p_coordinates,
    )


class DecisionModel:
    """A model directory loaded for prediction; the roles stage's classifier for both origins.

    `predict` may be called from many threads at once: their texts are queued and
    one worker runs them through the model in batches of `batch_size`, so the
    sets of every article under way share each forward pass. After `stop`, or
    an error in the model itself, nothing more is run: every caller waiting and
    every later one gets `ModelUnavailable`.
    """

    def __init__(self, path: str, *, source: str, device: str = "auto",
                 batch_size: int = BATCH) -> None:
        import torch  # noqa: PLC0415 - the `roles` extra, needed only once a model is loaded
        from safetensors.torch import load_file  # noqa: PLC0415
        from transformers import AutoModel, AutoTokenizer  # noqa: PLC0415

        self.root = Path(path)
        self.source = source
        self.card = Card.read(self.root)
        self.card.check()
        if device == "auto":
            device = "cuda" if torch.cuda.is_available() else "cpu"
        self.device = torch.device(device)
        self.batch_size = batch_size
        self._torch = torch
        self.tokenizer = AutoTokenizer.from_pretrained(self.root / "tokenizer")
        self.encoder = AutoModel.from_pretrained(self.root / "encoder").to(self.device).eval()
        weights = load_file(str(self.root / HEADS_FILE))
        sizes = {"coordinates": 2, "space": 2, "role": len(self.card.roles),
                 "kind": len(self.card.kinds), "source": len(self.card.sources)}
        self.heads = torch.nn.ModuleDict(
            {h: torch.nn.Linear(weights[f"{h}.weight"].shape[1], n) for h, n in sizes.items()}
        )
        self.heads.load_state_dict(weights, strict=True)
        self.heads.to(self.device).eval()
        self._stopped: Optional[str] = None
        self._queue: "queue.Queue[Optional[Tuple[str, Future]]]" = queue.Queue()
        self._worker = threading.Thread(target=self._run, name=f"roles-{self.card.name}",
                                        daemon=True)
        self._worker.start()

    def stop(self, reason: str) -> ModelUnavailable:
        """The error that stops the stage; the first reason stays, and nothing queued is run."""
        self._stopped = self._stopped or reason
        self._queue.put(None)
        return ModelUnavailable(self._stopped)

    def predict(self, texts: Sequence[str]) -> List[Prediction]:
        """Each text's prediction, in order."""
        if self._stopped:
            raise ModelUnavailable(self._stopped)
        futures = []
        for text in texts:
            future: Future = Future()
            self._queue.put((text, future))
            futures.append(future)
        return [f.result() for f in futures]

    def _run(self) -> None:
        while True:
            first = self._queue.get()
            batch = [first] if first is not None else []
            while len(batch) < self.batch_size:
                try:
                    item = self._queue.get_nowait()
                except queue.Empty:
                    break
                if item is not None:
                    batch.append(item)
            if self._stopped:
                self._fail(batch)
                continue
            if not batch:
                continue
            try:
                for (_, future), prediction in zip(batch, self._forward([t for t, _ in batch])):
                    future.set_result(prediction)
            except Exception as exc:  # noqa: BLE001 - the model's own failure stops the stage
                self.stop(f"the role model at {self.root} failed: {type(exc).__name__}: {exc}")
                self._fail(batch)

    def _fail(self, batch) -> None:
        for _, future in batch:
            if not future.done():
                future.set_exception(ModelUnavailable(self._stopped))
        while True:  # and everything still queued
            try:
                item = self._queue.get_nowait()
            except queue.Empty:
                return
            if item is not None and not item[1].done():
                item[1].set_exception(ModelUnavailable(self._stopped))

    def _forward(self, texts: List[str]) -> List[Prediction]:
        torch = self._torch
        enc = self.tokenizer(texts, truncation=True, max_length=self.card.max_length,
                             padding=True, return_tensors="pt").to(self.device)
        cuda = self.device.type == "cuda"
        with torch.no_grad(), torch.autocast(self.device.type, dtype=torch.bfloat16, enabled=cuda):
            pooled = self.encoder(input_ids=enc["input_ids"],
                                  attention_mask=enc["attention_mask"]).last_hidden_state[:, 0]
            probs = {h: torch.softmax(self.heads[h](pooled).float(), -1).cpu() for h in HEADS}
        return [answer(self.card, {h: probs[h][i].tolist() for h in HEADS})
                for i in range(len(texts))]
