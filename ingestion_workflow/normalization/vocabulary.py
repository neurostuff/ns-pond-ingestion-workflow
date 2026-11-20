"""The condition vocabulary: the axes and values a condition's atoms take, and the normalization of a labeller's atom.

The vocabulary is `data/condition_vocabulary_v3.json`. An atom is `(axis, value)`. Normalizing one:
lower snake_case, numeric values unit last (`deg_17` -> `17_deg`), aliases mapped onto vocabulary atoms, and
structural axes (task, group, score, drug, session) dropped, since those come from the record. An axis missing
from the vocabulary is a local axis: kept, and reported by `status`.
"""
from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path

PATH = Path(__file__).resolve().parent / "data" / "condition_vocabulary_v3.json"

_UNIT_FIRST = re.compile(r"^(deg|hz|pct|morph)_(\d+(?:_\d+)?)$")
_NUMERIC = re.compile(r"^(minus_)?\d+(_\d+)*(_[a-z]+)?$")
_UNIT_NAME = {"morph": "pct"}


@lru_cache(maxsize=1)
def load(path: Path = PATH) -> dict:
    return json.loads(path.read_text())


def snake(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", text.strip().lower()).strip("_")


def _format(value: str) -> str:
    v = snake(value)
    m = _UNIT_FIRST.match(v)
    if m:
        unit, num = m.groups()
        v = f"{num}_{_UNIT_NAME.get(unit, unit)}"
    return v


def normalize_atom(axis: str, value: str | list[str], vocab: dict | None = None) -> list[tuple[str, str]]:
    """A labeller's atom as vocabulary atoms. A list value becomes one atom per element."""
    vocab = vocab or load()
    axis_alias, value_alias = vocab["aliases"]["axes"], vocab["aliases"]["values"]
    structural = vocab["structural"]
    ax = snake(axis)
    out: list[tuple[str, str]] = []
    for raw in value if isinstance(value, list) else [value]:
        v = _format(raw)
        a = axis_alias.get(ax, ax)
        targets = value_alias.get(f"{ax}:{v}") or value_alias.get(f"{a}:{v}")
        if targets:
            out += [tuple(t.split(":", 1)) for t in targets]
            continue
        if a == "stage" and v.isdigit():
            v = f"block_{v}"
        out.append((a, v))
    return [(a, v) for a, v in dict.fromkeys(out) if a not in structural]


def status(axis: str, value: str, vocab: dict | None = None) -> str:
    """`in` (a listed value, or a well-formed value of an open axis), `open_value` (an unlisted value of an open
    axis), `new_value` (an unlisted value of a closed axis), or `local` (an axis missing from the vocabulary)."""
    vocab = vocab or load()
    spec = vocab["axes"].get(axis)
    if spec is None:
        return "local"
    if value in spec["values"]:
        return "in"
    if spec.get("numeric") and _NUMERIC.match(value):
        return "in"
    if (axis == "serial_position" and value.isdigit()) or (axis == "stage" and re.match(r"^block_\d+$", value)):
        return "in"
    return "open_value" if spec["open"] else "new_value"
