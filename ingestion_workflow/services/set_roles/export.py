"""One set of labels, three kinds of training row.

- `encoder_rows`: the set-role encoder's (one input string per set, its label
  and prior-study flag), split by article.
- `nu_v21_rows`: nu-v21's own rows (`/home/james/train-data/train_v21.jsonl`
  on beast: title, abstract, caption, footer, `table_serialised`,
  `target_json`, ...), with each target analysis given its `role`.
- `prose_rows`: the prose dataset's own rows (the v3 `train.jsonl` that
  `jk-prose-coords/ft/build_ft.py` reads: text, heading, points with `xyz`,
  `stat`, `analysis`, `role`), with each point's `role` set from its set's label.

The extractors' rows keep their format; only the role is added or replaced, in
the extractors' role vocabulary (`labels.extractor_role`). A unit is exported
to an extractor only when every one of its sets is labelled, so no row teaches
an unlabelled analysis's role by omission.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional

from .labeling import context_version_field, contexts, serialize
from .labels import EXTRACTOR_ROLES, extractor_role, label_from_prose

#: nu-v21's template (`train_nuex3_v2.py`'s TEMPLATE) with the analysis's role.
NU_V21_TEMPLATE = json.dumps(
    {
        "space": ["MNI", "TAL"],
        "analyses": [
            {
                "name": "verbatim-string",
                "role": list(EXTRACTOR_ROLES),
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


def split_of(article_id: Optional[str]) -> str:
    """train, val or test, by article: one article's sets never straddle two splits."""
    bucket = int(hashlib.sha1((article_id or "").encode()).hexdigest(), 16) % 10
    return "test" if bucket == 0 else "val" if bucket == 1 else "train"


def _dataset_label(unit: Mapping[str, Any], index: int) -> Optional[Dict[str, Any]]:
    """A synthetic unit's own label: its generator wrote each point's role."""
    s = unit["sets"][index]
    role = s.get("role") or next(
        (p.get("role") for p in s.get("points") or [] if isinstance(p, dict)), None
    )
    if role is None:
        return None
    return {
        "label": label_from_prose(role),
        "from_prior_study": role == "prior_study",
        "model": "dataset",
    }


def encoder_rows(
    units: Iterable[Mapping[str, Any]], labels: Mapping[str, Mapping[str, Any]]
) -> Iterator[Dict[str, Any]]:
    """One row per labelled set: its input string, label and prior flag."""
    for unit in units:
        field, version = context_version_field(unit["origin"])
        for i, ctx in enumerate(contexts(unit)):
            set_id = f"{unit['unit_id']}#{i}"
            label = labels.get(set_id) or (
                _dataset_label(unit, i) if unit.get("labels_from") == "dataset" else None
            )
            if label is None:
                continue
            yield {
                "set_id": set_id,
                "article_id": unit.get("article_id"),
                "origin": unit["origin"],
                field: version,
                "text": serialize(unit, ctx),
                "label": label["label"],
                "from_prior_study": bool(label["from_prior_study"]),
                "label_source": label.get("model"),
                "split": split_of(unit.get("article_id")),
            }


def _all_labelled(
    unit: Mapping[str, Any], labels: Mapping[str, Any]
) -> Optional[List[Mapping[str, Any]]]:
    found = [labels.get(f"{unit['unit_id']}#{i}") for i in range(len(unit["sets"]))]
    return found if found and all(found) else None


def nu_v21_rows(
    units: Iterable[Mapping[str, Any]], labels: Mapping[str, Mapping[str, Any]]
) -> Iterator[Dict[str, Any]]:
    """nu-v21 rows with `role` after each target analysis's name.

    A table unit's sets are its base row's target analyses, in order.
    """
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
                "role": extractor_role(label["label"]),
                **{k: v for k, v in a.items() if k != "name"},
            }
            for a, label in zip(target["analyses"], found)
        ]
        yield {
            **base,
            "target_json": json.dumps(target, ensure_ascii=False, separators=(",", ":")),
            "role_labels": [label["model"] for label in found],
            context_version_field("table")[0]: context_version_field("table")[1],
        }


def prose_rows(
    units: Iterable[Mapping[str, Any]], labels: Mapping[str, Mapping[str, Any]]
) -> Iterator[Dict[str, Any]]:
    """Prose dataset rows with each point's `role` set from its set's label.

    A prose unit's sets are its base row's analyses in order of first
    appearance; a point listed under several analyses takes each one's role.
    """
    for unit in units:
        base = unit.get("base_row")
        found = _all_labelled(unit, labels) if unit["origin"] == "text" and base else None
        if found is None:
            continue
        roles = {
            s.get("name"): extractor_role(label["label"]) for s, label in zip(unit["sets"], found)
        }
        points = [
            {**p, "role": roles.get(p.get("analysis"), p.get("role"))}
            for p in base.get("points") or []
        ]
        yield {
            **base,
            "points": points,
            "label_source": "+".join(sorted({label["model"] for label in found})),
            context_version_field("text")[0]: context_version_field("text")[1],
        }
