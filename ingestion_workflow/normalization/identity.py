"""Each analysis's contrast identity, assembled from its structure (`structure.assemble`) and its task's design
(`design.read`). Nothing here is written by a model: the model gives atoms and the task, and the identity, its
key and its canonical text follow from them.

    identity(structure, task, design) -> {kind, measure, task, family, baseline_type, manipulated, shared,
                                          target, baseline, complete, key, canonical}

`manipulated` holds the axes whose values differ between the two sides; `shared` the task's context and the axes
both sides hold at the same values. A side's atoms are the union over its conditions. `complete` is False when the
design has no atoms for some condition on a side.

Contrast key v2 is (kind, measure, task or family, baseline type) (`key_of`). The manipulated axes are part of the
identity and of the sidecar, but not of the key: v4's agree with Claude's on 35% of condition pairs.
"""
from __future__ import annotations

from typing import Dict, List, Optional

KEY_VERSION = "v2"
#: Kinds whose two sides are conditions, so that the sides' atoms can differ.
TWO_SIDED = ("condition_vs_condition", "condition_vs_baseline", "group_by_condition", "condition_interaction")


def atoms_by_condition(design: Optional[dict]) -> Dict[str, Dict[str, set]]:
    out: Dict[str, Dict[str, set]] = {}
    for c in (design or {}).get("conditions") or []:
        held = out.setdefault(c["id"], {})
        for a in c["atoms"]:
            held.setdefault(a["axis"], set()).add(a["value"])
    return out


def _side(conditions: List[str], atoms: Dict[str, Dict[str, set]]):
    merged: Dict[str, set] = {}
    for cid in conditions:
        for axis, values in atoms.get(cid, {}).items():
            merged.setdefault(axis, set()).update(values)
    return merged, all(cid in atoms for cid in conditions)


def measure_of(structure: dict) -> str:
    if structure["measure"] != "connectivity":
        return structure["measure"]
    return "connectivity:" + ((structure.get("connectivity") or {}).get("method") or "unspecified")


def label_of(task: Optional[str], family: Optional[str]) -> str:
    return task or (f"family:{family}" if family else "none")


def key_of(identity: dict) -> str:
    return "|".join([KEY_VERSION, identity["kind"], identity["measure"],
                     label_of(identity["task"], identity["family"]), identity["baseline_type"]])


def _text(side: Dict[str, List[str]], axes) -> str:
    return "; ".join(f"{a}={'+'.join(side[a])}" for a in axes if a in side)


def canonical_of(identity: dict, structure: dict) -> str:
    """The identity in words: `<task or family>: <target> > <baseline>`, the sides on the manipulated axes."""
    label, kind = label_of(identity["task"], identity["family"]), identity["kind"]
    target, baseline = identity["target"], identity["baseline"]
    axes = identity["manipulated"] or sorted(target)
    t = _text(target, axes) or "task"
    b = _text(baseline, axes) or identity["baseline_type"]
    gt, gb = (" + ".join(structure["groups"][k]) or "?" for k in ("target", "baseline"))
    if kind in ("condition_vs_condition", "condition_vs_baseline", "condition_interaction"):
        said = f"{t} > {b}"
    elif kind == "group_by_condition":
        said = f"({gt} > {gb}) x ({t} > {b})"
    elif kind == "group_difference":
        said = f"{gt} > {gb}"
    elif kind == "correlation":
        said = f"correlation with {', '.join(structure['covariates']) or 'a covariate'}" + (
            f" ({structure['sign']})" if structure.get("sign") else "")
    else:
        said = kind.replace("_", " ")
    measure = "" if identity["measure"] == "activation" else f" [{identity['measure']}]"
    return f"{label}: {said}{measure}"


def identity(structure: dict, task: Optional[str], design: Optional[dict]) -> dict:
    """The contrast identity of one analysis whose task is `task` (None when its cells span several tasks or
    none), under that task's design (None when the model gave none)."""
    atoms = atoms_by_condition(design)
    target, t_complete = _side(structure["target"]["conditions"], atoms)
    baseline, b_complete = _side(structure["baseline"]["conditions"], atoms)
    kind = structure["kind"]
    two_sided = kind in TWO_SIDED and bool(structure["target"]["conditions"]) \
        and bool(structure["baseline"]["conditions"])
    manipulated = sorted(a for a in set(target) | set(baseline) if target.get(a) != baseline.get(a)) \
        if two_sided else []
    context = {(x["axis"], x["value"]) for x in (design or {}).get("context") or []}
    same = {(a, v) for a in set(target) & set(baseline) if target[a] == baseline[a] for v in target[a]} \
        if two_sided else set()
    out = {"kind": kind, "measure": measure_of(structure),
           "task": (design or {}).get("task"), "family": (design or {}).get("family"),
           "task_confidence": (design or {}).get("task_confidence"),
           "baseline_type": structure["baseline_type"], "manipulated": manipulated,
           "shared": [{"axis": a, "value": v} for a, v in sorted(context | same)],
           "target": {a: sorted(vs) for a, vs in sorted(target.items())},
           "baseline": {a: sorted(vs) for a, vs in sorted(baseline.items())},
           "complete": design is not None and t_complete and b_complete,
           "task_id": task}
    out["key"] = key_of(out)
    out["canonical"] = canonical_of(out, structure)
    return out


def task_of(analysis: dict, structure: dict, condition_task: Dict[str, str]) -> Optional[str]:
    """The one task an analysis is of: the tasks it names, else those of the conditions its cells carry."""
    named = {t for t in analysis.get("tasks") or [] if isinstance(t, str)}
    held = structure["target"]["conditions"] + structure["baseline"]["conditions"] + structure["held"]
    tasks = named or {condition_task[c] for c in held if c in condition_task}
    return next(iter(tasks)) if len(tasks) == 1 else None


def identities(record: dict, structures: List[dict], designs: Dict[str, Optional[dict]]) -> Dict[str, dict]:
    """analysis id -> its identity, for every structure of `record`."""
    condition_task = {c.get("local_id"): t.get("local_id") for t in record.get("tasks") or [] if isinstance(t, dict)
                      for c in t.get("conditions") or [] if isinstance(c, dict)}
    analyses = {a.get("local_id"): a for a in record.get("analyses") or [] if isinstance(a, dict)}
    out = {}
    for s in structures:
        tid = task_of(analyses.get(s["analysis"]) or {}, s, condition_task)
        out[s["analysis"]] = identity(s, tid, designs.get(tid) if tid else None)
    return out


__all__ = ["KEY_VERSION", "TWO_SIDED", "atoms_by_condition", "canonical_of", "identities", "identity", "key_of",
           "label_of", "measure_of", "task_of"]
