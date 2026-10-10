"""One set of labels, three kinds of training row.

- `encoder_rows`: the set-role encoder's (one input string per set, its role
  fields), split by article (`splits`).
- `nu_v21_rows`: nu-v21's own rows (`/home/james/train-data/train_v21.jsonl`
  on beast: title, abstract, caption, footer, `table_serialised`,
  `target_json`, ...), with each target analysis given its role fields.
- `prose_rows`: the prose dataset's own rows (the v3 `train.jsonl` that
  `jk-prose-coords/ft/build_ft.py` reads: text, heading, points with `xyz`,
  `stat`, `analysis`), with each point's role fields set from its set's label.

The extractors' rows keep their format; only the role fields are added or
replaced: study_schema's `role`, `anchor_kind` and `from_prior_study`, the same
fields the encoder and the paper parse use, so retrained extractors answer in
them directly. Numbers labelled not coordinates are taken out of the
extractors' targets, and every role a row carries is checked (`checked`). A unit is exported
to an extractor only when every one of its sets is labelled, so no row teaches
an unlabelled analysis's role by omission. A prose row that is evaluation data
or a human's labels (`labeling.held_out`) is passed through unchanged.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence

from .labeling import context_version_field, contexts, duplicates, held_out, serialize
from .labels import ANCHOR_KINDS, COORDINATE_ROLES, SetRole, role_error

#: The role fields every exported role carries.
ROLE_FIELDS = ("role", "anchor_kind", "from_prior_study")

#: nu-v21's template (`train_nuex3_v2.py`'s TEMPLATE) with the analysis's role fields.
NU_V21_TEMPLATE = json.dumps(
    {
        "space": ["MNI", "TAL"],
        "analyses": [
            {
                "name": "verbatim-string",
                "role": list(COORDINATE_ROLES),
                "anchor_kind": list(ANCHOR_KINDS),
                "from_prior_study": "boolean",
                "measure": ["voxels", "mm^3"],
                "points": [
                    [
                        "number",
                        "number",
                        "number",
                        ["T", "Z", "F", "P", "R", "B"],
                        "number",
                        "integer",
                    ]
                ],
            }
        ],
    },
    ensure_ascii=False,
)


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


def _all_labelled(
    unit: Mapping[str, Any], labels: Mapping[str, Any]
) -> Optional[List[Mapping[str, Any]]]:
    found = [labels.get(f"{unit['unit_id']}#{i}") for i in range(len(unit["sets"]))]
    return found if found and all(found) else None


def _with_copies(
    units: Sequence[Mapping[str, Any]], labels: Mapping[str, Mapping[str, Any]]
) -> Dict[str, Mapping[str, Any]]:
    """`labels` plus, for each repeated table, its first copy's labels (labelled once)."""
    out = dict(labels)
    for dup, first in duplicates(units).items():
        i = 0
        while f"{first}#{i}" in labels:
            out.setdefault(f"{dup}#{i}", labels[f"{first}#{i}"])
            i += 1
    return out


def nu_v21_rows(
    units: Iterable[Mapping[str, Any]], labels: Mapping[str, Mapping[str, Any]]
) -> Iterator[Dict[str, Any]]:
    """nu-v21 rows with the role fields after each target analysis's name.

    A table unit's sets are its base row's target analyses, in order; an
    analysis labelled not coordinates is dropped from the target.
    """
    units = list(units)
    labels = _with_copies(units, labels)
    for unit in units:
        base = unit.get("base_row")
        found = _all_labelled(unit, labels) if unit["origin"] == "table" and base else None
        if found is None:
            continue
        target = json.loads(base["target_json"])
        if len(target.get("analyses") or []) != len(found):
            continue
        target["analyses"] = [
            {
                "name": a.get("name"),
                **checked(label, label.get("set_id", unit["unit_id"])),
                **{k: v for k, v in a.items() if k != "name" and k not in ROLE_FIELDS},
            }
            for a, label in zip(target["analyses"], found)
            if label["role"] is not None
        ]
        yield {
            **base,
            "target_json": json.dumps(target, ensure_ascii=False, separators=(",", ":")),
            "role_labels": [label["model"] for label in found],
            context_version_field("table")[0]: context_version_field("table")[1],
        }


def prose_rows(
    units: Iterable[Mapping[str, Any]],
    labels: Mapping[str, Mapping[str, Any]],
    *,
    keep_unlabelled: bool = False,
) -> Iterator[Dict[str, Any]]:
    """Prose dataset rows with each point's role fields set from its set's label.

    A prose unit's sets are its base row's analyses in order of first
    appearance; a point listed under several analyses takes each one's role.
    `keep_unlabelled` passes the other rows through unchanged (a synthetic row
    keeps its generator's roles), so the output can replace the dataset. A
    held-out row (val, test, or hand-labelled) is never relabelled. Points that
    are not coordinates are dropped from every row. The labellers
    go in `role_label_source`; the row's own `label_source` stays.
    """
    for unit in units:
        base = unit.get("base_row")
        if unit["origin"] != "text" or not base:
            continue
        found = None if held_out(unit) else _all_labelled(unit, labels)
        if found is None:
            if keep_unlabelled:
                yield {**base, "points": _points(base, {}, unit["unit_id"])}
            continue
        roles = {s.get("name"): label for s, label in zip(unit["sets"], found)}
        yield {
            **base,
            "points": _points(base, roles, unit["unit_id"]),
            "role_label_source": "+".join(sorted({label["model"] for label in found})),
            context_version_field("text")[0]: context_version_field("text")[1],
        }


def _points(
    base: Mapping[str, Any], roles: Mapping[Any, Mapping[str, Any]], unit_id: str
) -> List[Dict[str, Any]]:
    """A prose row's points with their analysis's role fields (`roles`) or their own.

    Points that are not coordinates are dropped.
    """
    out = []
    for p in base.get("points") or []:
        fields = checked(roles.get(p.get("analysis"), p), f"{unit_id} point {p.get('xyz')}")
        if fields["role"] is not None:
            out.append({**p, **fields})
    return out
