"""Each task's design, from the task-design model: its prompt, the request, and the answer read back.

The prompt is the one the model was trained on, byte for byte (round 4's make_ft_v4.py over the
items make_task_items_r4.py built): the fixed prefix (`data/task_design_v4_prefix.txt`: the
instruction, the inventory with definitions, condition vocabulary 3.1) and the task's document.

    item_of(record, task, text) -> the task as the labeller saw it
    prompt_of(item) -> the user turn
    request(prompt, model) -> the chat request body
    read(answer) -> the design: factors, context, conditions, task, family, task_confidence

`read` puts every atom through the vocabulary (`vocabulary.normalize_atom`), as the scorer did.
"""
from __future__ import annotations

import hashlib
import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional

from . import vocabulary

DATA = Path(__file__).resolve().parent / "data"
INVENTORY = DATA / "task_inventory_v3.json"

#: Served name -> the prompt it was trained on (prefix file, its sha256).
MODELS = {
    "task-design-v4": ("task_design_v4_prefix.txt",
                       "c64f1e567547f8b36a2364471a4feeef6212a2b5a7130fbafee1fb6cfec0b46f"),
}
KINDS = ["task_state", "rest", "fixation", "control_state", "implicit_baseline"]
CONFIDENCE = ("high", "medium", "low")
MAX_COMPLETION_TOKENS = 4096


def v(x):
    return x.get("value") if isinstance(x, dict) else x


@lru_cache(maxsize=1)
def inventory() -> Dict[str, dict]:
    return {t["id"]: t for t in json.loads(INVENTORY.read_text())["tasks"]}


@lru_cache(maxsize=1)
def template() -> str:
    families = sorted({t["family"] for t in inventory().values()})
    return json.dumps({
        "factors": [{"from_term": "verbatim-string", "axis": "string",
                     "levels": [{"level": "verbatim-string", "value": "string"}]}],
        "context": [{"axis": "string", "value": "string"}],
        "conditions": [{"id": "verbatim-string", "kind": KINDS,
                        "atoms": [{"axis": "string", "value": "string"}],
                        "expansion": "string", "gloss": "string"}],
        "task": "string",
        "family": families,
        "task_confidence": list(CONFIDENCE),
    }, ensure_ascii=False)


@lru_cache(maxsize=None)
def prefix(model: str) -> str:
    if model not in MODELS:
        raise ValueError(f"no prompt for served model {model!r}: knows {sorted(MODELS)}")
    name, sha = MODELS[model]
    raw = (DATA / name).read_bytes()
    if hashlib.sha256(raw).hexdigest() != sha:
        raise ValueError(f"{name} has changed: its sha256 is not {sha}")
    return raw.decode("utf-8")


def _excerpt(task, text):
    """The paper text around the task description's first two evidence spans."""
    try:
        spans = [(s.get("start_char"), s.get("end_char"))
                 for es in ((task.get("description") or {}).get("evidence") or {}).get("sets") or []
                 for s in es.get("spans") or []]
        spans = [(a, b) for a, b in spans if isinstance(a, int)][:2]
        return " […] ".join(text[max(0, a - 200):b + 400] for a, b in spans)[:1800]
    except Exception:  # noqa: BLE001 - the builder read no excerpt for a malformed span or no text
        return ""


def item_of(record: dict, task: dict, text: Optional[str]) -> dict:
    """One task as the labeller saw it: its conditions, the model terms whose levels carry them,
    the analyses naming it and the text around its description. `text` is the parse's text."""
    tid = str(v(task.get("local_id")))
    cids = {str(v(c.get("local_id"))) for c in task.get("conditions") or [] if isinstance(c, dict)}
    terms = []
    for me in record.get("model_estimations") or []:
        for term in (me.get("terms") or []) if isinstance(me, dict) else []:
            if not isinstance(term, dict):
                continue
            levels = [{"level": v(L.get("level")), "conditions": [str(v(c)) for c in L.get("conditions") or []],
                       "groups": [str(v(g)) for g in L.get("groups") or []],
                       "timepoints": [str(v(y)) for y in L.get("timepoints") or []]}
                      for L in term.get("levels") or [] if isinstance(L, dict)]
            if any(set(L["conditions"]) & cids for L in levels):
                terms.append({"name": v(term.get("name")), "type": v(term.get("type")),
                              "variation_level": v(term.get("variation_level")), "levels": levels})
    analyses = [str(v(a.get("name")))[:120] for a in record.get("analyses") or []
                if isinstance(a, dict) and tid in [str(v(y)) for y in a.get("tasks") or []]]
    return {"name": v(task.get("name")), "paradigm": v(task.get("paradigm")),
            "description": (v(task.get("description")) or "")[:900],
            "conditions": [{"id": str(v(c.get("local_id"))), "name": v(c.get("name")),
                            "kind": v(c.get("condition_kind")),
                            "description": (v(c.get("description")) or "")[:300],
                            "stimulus": v(c.get("stimulus_content"))}
                           for c in task.get("conditions") or [] if isinstance(c, dict)][:24],
            "terms": terms[:8], "analyses": analyses[:15],
            "excerpt": _excerpt(task, text) if text is not None else ""}


def document(t: dict) -> str:
    """The task's part of the prompt (make_ft_doc.document without its first two lines)."""
    lines = [f"Task: {t.get('name') or ''}"]
    if t.get("paradigm"):
        lines.append(f"Paradigm: {t['paradigm']}")
    if t.get("description"):
        lines.append(f"Description: {t['description']}")
    lines.append("Conditions:")
    for c in t.get("conditions") or []:
        stim = c.get("stimulus")
        stim = ", ".join(map(str, stim)) if isinstance(stim, list) else (stim or "")
        lines.append(f"- {c['id']} | {c.get('name') or ''} | kind: {c.get('kind') or ''} | {c.get('description') or ''}"
                     + (f" | stimulus: {stim}" if stim else ""))
    if t.get("terms"):
        lines.append("Model terms:")
        for m in t["terms"]:
            levels = "; ".join(f"{L.get('level')} -> {', '.join(L.get('conditions') or []) or '-'}"
                               for L in m.get("levels") or [])
            lines.append(f"- {m.get('name')} ({m.get('type')}, {m.get('variation_level')}): {levels}")
    if t.get("analyses"):
        lines.append("Analyses: " + " | ".join(t["analyses"]))
    if t.get("excerpt"):
        lines.append(f"Excerpt: {t['excerpt']}")
    return "\n".join(lines)


def prompt_of(item: dict, model: str = "task-design-v4") -> str:
    return prefix(model) + "\n" + document(item)


def tasks_of(record: dict) -> List[dict]:
    """The record's tasks the model labels: those with conditions."""
    return [t for t in record.get("tasks") or [] if isinstance(t, dict) and t.get("conditions")]


def request(prompt: str, model: str) -> dict:
    """The chat request: the prompt as the user turn, the template, greedy."""
    return {"model": model, "messages": [{"role": "user", "content": prompt}], "temperature": 0.0,
            "max_completion_tokens": MAX_COMPLETION_TOKENS,
            "chat_template_kwargs": {"template": template(), "enable_thinking": False}}


def key_of(prompt: str, model: str) -> str:
    """What an answer is stored under: the served model, the template and the prompt."""
    return hashlib.sha256(json.dumps([model, template(), prompt]).encode("utf-8")).hexdigest()


def _atoms(axis, value):
    return [{"axis": a, "value": x} for a, x in vocabulary.normalize_atom(axis or "", value)]


def read(answer: str) -> Dict[str, Any]:
    """The model's answer as a design, every atom in vocabulary form (round 4's labels_lib.normalize).
    A task off the inventory, a family, kind or confidence off its list, reads as None. Raises
    ValueError when the answer is not a JSON object."""
    t = json.loads(answer)
    if not isinstance(t, dict):
        raise ValueError("the answer is not a JSON object")
    factors = []
    for f in t.get("factors") or []:
        if not isinstance(f, dict):
            continue
        per_axis: Dict[str, Dict[str, str]] = {}
        for lev in f.get("levels") or []:
            if isinstance(lev, dict) and isinstance(lev.get("value"), (str, list)):
                for x in _atoms(f.get("axis"), lev["value"]):
                    per_axis.setdefault(x["axis"], {})[str(lev.get("level"))] = x["value"]
        factors += [{"from_term": f.get("from_term"), "axis": a, "levels": levels}
                    for a, levels in per_axis.items()]
    context = [x for c in t.get("context") or [] if isinstance(c, dict) and c.get("axis")
               and isinstance(c.get("value"), (str, list)) for x in _atoms(c["axis"], c["value"])]
    conditions = []
    for c in t.get("conditions") or []:
        if not isinstance(c, dict) or c.get("id") is None:
            continue
        atoms = [x for a in c.get("atoms") or [] if isinstance(a, dict) and a.get("axis")
                 and isinstance(a.get("value"), (str, list)) for x in _atoms(a["axis"], a["value"])]
        conditions.append({"id": str(c["id"]), "kind": c.get("kind") if c.get("kind") in KINDS else None,
                           "atoms": list({(x["axis"], x["value"]): x for x in atoms}.values()),
                           "expansion": c.get("expansion") or None, "gloss": c.get("gloss") or None})
    families = {x["family"] for x in inventory().values()}
    task = t.get("task") if t.get("task") in inventory() else None
    return {"factors": factors, "context": list({(x["axis"], x["value"]): x for x in context}.values()),
            "conditions": conditions, "task": task,
            "task_off_inventory": t.get("task") if t.get("task") and task is None else None,
            "family": t.get("family") if t.get("family") in families else None,
            "task_confidence": t.get("task_confidence") if t.get("task_confidence") in CONFIDENCE else None}


__all__ = ["CONFIDENCE", "KINDS", "MAX_COMPLETION_TOKENS", "MODELS", "document", "inventory", "item_of",
           "key_of", "prefix", "prompt_of", "read", "request", "tasks_of", "template"]
