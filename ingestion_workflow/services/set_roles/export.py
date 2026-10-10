"""One set of labels, the set-role encoder's training rows.

- `encoder_rows`: the set-role encoder's (one input string per set, its role
  fields), split by article (`splits`).
Roles come only from the roles stage; no extractor's training rows carry them. Numbers
labelled not coordinates have no role, and every role a row carries is checked (`checked`).
"""

from __future__ import annotations

import hashlib
from typing import Any, Dict, Iterable, Iterator, Mapping, Optional, Sequence

from .labeling import context_version_field, contexts, duplicates, held_out, serialize
from .labels import SetRole, role_error

#: The role fields every exported role carries.
ROLE_FIELDS = ("role", "anchor_kind", "from_prior_study")

#: Labellers whose labels are the gold set: an article holding one is test data.
GOLD_MODELS = ("gpt-6-astra", "hand")


def split_of(article_id: Optional[str]) -> str:
    """train, val or test, by article: one article's sets never straddle two splits."""
    bucket = int(hashlib.sha1((article_id or "").encode()).hexdigest(), 16) % 10
    return "test" if bucket == 0 else "val" if bucket == 1 else "train"


def _is_hand(unit: Mapping[str, Any]) -> bool:
    return (unit.get("base_row") or {}).get("label_source") == "hand"


def splits(
    units: Sequence[Mapping[str, Any]],
    labels: Mapping[str, Mapping[str, Any]],
    id_map: Optional[Mapping[str, str]] = None,
) -> Dict[str, str]:
    """Each unit's split, by article under its database id (`id_map` maps slug ids).

    Articles holding the same table text are one article. An article with a gold
    label or a held-out prose row is test (or val, the prose dataset's split); a
    synthetic unit, which has no article, is train.
    """
    id_map = id_map or {}
    parent: Dict[str, str] = {}

    def find(key: str) -> str:
        while parent.setdefault(key, key) != key:
            key = parent[key]
        return key

    def article(unit: Mapping[str, Any]) -> str:
        aid = unit.get("article_id") or unit["unit_id"]
        return find(id_map.get(aid, aid).lower())

    by_id = {u["unit_id"]: u for u in units}
    for dup, first in duplicates(units).items():
        a, b = sorted((article(by_id[dup]), article(by_id[first])))
        parent[b] = a
    forced: Dict[str, str] = {}
    for unit in units:
        n = len(unit["sets"])
        gold = _is_hand(unit) or any(
            (labels.get(f"{unit['unit_id']}#{i}") or {}).get("model") in GOLD_MODELS
            for i in range(n)
        )
        if gold or (held_out(unit) and unit.get("dataset_split") == "test"):
            forced[article(unit)] = "test"
        elif held_out(unit):
            forced.setdefault(article(unit), unit["dataset_split"])
    out = {}
    for unit in units:
        if unit.get("labels_from") == "dataset" and not _is_hand(unit):
            out[unit["unit_id"]] = "train"
            continue
        key = article(unit)
        out[unit["unit_id"]] = forced.get(key) or split_of(key)
    return out


def _dataset_label(unit: Mapping[str, Any], index: int) -> Optional[Dict[str, Any]]:
    """A synthetic or hand-labelled unit's own label: the role fields on the set or its points."""
    s = unit["sets"][index]
    source = (
        s
        if "role" in s
        else next((p for p in s.get("points") or [] if isinstance(p, dict) and "role" in p), None)
    )
    if source is None:
        return None
    return {**SetRole.of(source).fields(), "model": "hand" if _is_hand(unit) else "dataset"}


def checked(fields: Mapping[str, Any], where: str) -> Dict[str, Any]:
    """`fields`' role fields, refused unless they are study_schema's."""
    error = role_error(fields)
    if error:
        raise ValueError(f"{where}: {error}")
    return {k: fields[k] for k in ROLE_FIELDS}


def encoder_rows(
    units: Iterable[Mapping[str, Any]],
    labels: Mapping[str, Mapping[str, Any]],
    id_map: Optional[Mapping[str, str]] = None,
) -> Iterator[Dict[str, Any]]:
    """One row per labelled set: its input string, label, prior flag and split.

    A hand-labelled prose row gives its own labels (the prose gold set); a
    repeated table is left out.
    """
    units = list(units)
    split = splits(units, labels, id_map)
    repeated = duplicates(units)
    for unit in units:
        if unit["unit_id"] in repeated:
            continue
        field, version = context_version_field(unit["origin"])
        own = unit.get("labels_from") == "dataset" or _is_hand(unit)
        for i, ctx in enumerate(contexts(unit)):
            set_id = f"{unit['unit_id']}#{i}"
            label = _dataset_label(unit, i) if own else labels.get(set_id)
            if label is None:
                continue
            yield {
                "set_id": set_id,
                "article_id": unit.get("article_id"),
                "origin": unit["origin"],
                field: version,
                "text": serialize(unit, ctx),
                **checked(label, set_id),
                "coordinates": label["role"] is not None,
                "label_source": label.get("model"),
                "split": split[unit["unit_id"]],
            }
