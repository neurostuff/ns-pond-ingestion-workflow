"""Tasks, conditions (in task context), analyses and condition contrasts read off one extraction record.

A port of the Astra build's astra_normalize.py `one()`, the same rules over a record dict instead of a file:

Tasks:      taskcat gate -> identity (A lexical / B TF-IDF procedure) -> family/stem/canonical/variant; also the
            raw, unscoped condition atoms of each task (the contrast classifier's evidence).
Conditions: every declared Condition, normalize_condition_ctx under its task's label; `in_schema` against the
            task's condition schema (task_inventory_v3); `referenced` = reached by some analysis cell.
Analyses:   every analysis, read off the record graph: each cell resolved cell -> ModelTerm -> FactorLevel -> the
            entities it carries; tested_across = the non-condition entity types on directed levels.
Contrasts:  the analyses with >= 1 directed cell on a Condition. Sides from Cell.direction; shape from the extracted
            condition_kind; key = family | target tokens | baseline kind (or the other side's tokens).
"""
import json
import re
from collections import Counter
from pathlib import Path

from .taskcat.conditions import normalize_condition, normalize_condition_ctx
from .taskcat.decompose import STEM_OF
from .taskcat.gate import gate

INVENTORY = Path(__file__).resolve().parent / "data" / "task_inventory_v3.json"
BASELINE = {"control_state", "rest", "fixation"}
GATED = {"not_a_task", "questionnaire_or_scale", "intervention_or_training", "neuropsychological_test"}
REST = re.compile(r"\brest(ing)?[- ]?(state)?\b", re.I)


def v(x):
    return x.get("value") if isinstance(x, dict) else x


def text(x):
    x = v(x)
    return "; ".join(map(str, x)) if isinstance(x, list) else (x or "")


def fold(s):
    return re.sub(r"[^a-z0-9]+", " ", str(s).lower()).strip() if s is not None else ""


STIM_AXES = ("stimulus", "emotion", "valence", "outcome", "modality", "magnitude", "social")


#: where a description stops being about its own condition: "SS: successful stop trials, compared with go trials"
DESC_HEAD_END = re.compile(r"[:;(]|\. |\b(relative to|compared (to|with)|versus|vs\.?|than|in contrast|following|preceded by|after|before)\b", re.I)


def description_head(desc):
    """The parts of a Condition.description that say what the condition itself is, before any comparison: the label
    before a colon ("Successful stop trials: ...") and the definition after it, each cut at its first comparison."""
    d = (desc or "").strip()
    parts = d.split(":", 1) if ":" in d[:60] else [d]
    heads = []
    for p in parts:
        m = DESC_HEAD_END.search(p.strip())
        heads.append((p.strip()[:m.start()] if m else p.strip())[:160])
    return heads


def description_atoms(desc, lab, fam):
    """Atoms of the first description head that yields any: the label wins over the definition after it."""
    for h in description_head(desc):
        d = normalize_condition_ctx(h, lab, fam)
        if d["atoms"]:
            return d
    return {"atoms": [], "phases": []}


def condition_atoms(c, lab, fam):
    """A Condition's atoms: from its name (normalize_condition_ctx); when the name gives none (an abbreviation like
    "SS", a code like "pSEi"), from the head of its extracted `description`; plus those its `stimulus_content` states
    on the stimulus-like axes. The name wins on an axis both give. Returns the name normalization with `atoms`
    merged, and `atoms_from_description` / `atoms_from_stimulus` listing what each added."""
    n = normalize_condition_ctx(text(c.get("name")), lab, fam)
    from_desc = []
    if not n["atoms"]:
        d = description_atoms(text(c.get("description")), lab, fam)
        from_desc = d["atoms"]
        n = {**n, "atoms": from_desc, "phases": n["phases"] or d["phases"]}
    stim = " ; ".join(str(x) for x in (v(c.get("stimulus_content")) or []))
    have = {a.split(":")[0] for a in n["atoms"]}
    add = [a for a in normalize_condition_ctx(stim, lab, fam)["atoms"] if a.split(":")[0] in STIM_AXES and a.split(":")[0] not in have] if stim else []
    return {**n, "atoms": n["atoms"] + sorted(set(add)), "atoms_from_description": from_desc, "atoms_from_stimulus": sorted(set(add))}


def expander(abbreviations, study):
    """str -> the str with the paper's own short forms spelled out, from pondie's per-paper store
    (`Abbreviations.for_paper`); without a store, the str as it is."""
    if abbreviations is None:
        return lambda s: s
    from pondie.normalization.medical_condition import _expanded

    return lambda s: _expanded(s, abbreviations, study)[0] if s else s


def evidence(t, contrast_names, lexicon, expand):
    """The contrast route's document for one task: its condition names, their atoms (with the description fallback),
    the names of its contrasts, the conditions' descriptions and stimulus content, and the task's own name and
    description with every lexicon task name masked, since the name route has already read those. Each with the
    paper's abbreviations spelled out. Sections are joined by " || "; empty when the task says nothing."""
    conds = [c for c in t.get("conditions") or [] if isinstance(c, dict)]
    names = [expand(text(c.get("name"))) for c in conds]
    descs = [expand(text(c.get("description"))) for c in conds]
    atoms = sorted({a for c, n, d in zip(conds, names, descs)
                    for a in condition_atoms({**c, "name": n, "description": d}, None, None)["atoms"]})
    stim = [" ; ".join(str(x) for x in (v(c.get("stimulus_content")) or [])) for c in conds]
    sections = [
        " ".join(x for x in (" ; ".join(names), " ".join(a.replace(":", "_") for a in atoms),
                             " ; ".join(expand(k) for k in contrast_names)) if x),
        " ".join(x for x in (" ; ".join(d[:200] for d in descs if d), " ; ".join(x for x in stim if x)) if x),
        " ".join(x for x in (lexicon.mask(expand(text(t.get("name")))),
                             lexicon.mask(expand(text(t.get("description"))[:600]))) if x)]
    return " || ".join(x for x in sections if x.strip())


def token(atoms, phases):
    return ",".join(sorted(set(atoms) | {f"phase:{p}" for p in phases}))


def schemas():
    inv = json.load(open(INVENTORY))
    tasks = inv["tasks"] if "tasks" in inv else inv
    tasks = tasks.values() if isinstance(tasks, dict) else tasks
    return {t["id"]: {a for a in (t.get("condition_schema") or {}) if not a.startswith("_")}
            for t in tasks if t.get("id")}


def read(r, study, CB, SCH, final=None, abbreviations=None):
    """One record -> its rows of the four tables (tasks, conditions, contrasts, analyses), and its counts.

    `CB` the task identity (taskcat.identity.CardBuilder), `SCH` `schemas()`, `final` the final task labels by
    (study, task local_id): conditions and contrasts are read under those, not taskcat's own. `abbreviations`, the
    paper's own short forms (pondie `Abbreviations.for_paper`), are spelled out in each task's `evidence`."""
    final = final or {}
    T, Cn, K, A = ([] for _ in range(4))
    st = Counter()
    terms = {t.get("local_id"): t for m in r.get("model_estimations") or [] for t in m.get("terms") or []}
    tasks = r.get("tasks") or []
    cond_task, cond, label = {}, {}, {}
    for t in tasks:
        for c in t.get("conditions") or []:
            cond_task[c.get("local_id")] = t.get("local_id"); cond[c.get("local_id")] = c
    # ---- tasks
    for t in tasks:
        name = text(t.get("name")); conds = [text(c.get("name")) for c in t.get("conditions") or []]
        kinds = [v(c.get("condition_kind")) for c in t.get("conditions") or []]
        lex = CB.L.match(name); lex = lex[0] if lex else None
        rest_flag = bool(REST.search(name)) or (bool(kinds) and all(k == "rest" for k in kinds))
        kind = gate(name, rest_flag, lex)
        m = {"name": name, "paradigm_term": None, "description": text(t.get("description")),
             "instructions": text(t.get("instructions")),
             "stimuli": [{"content": s} for s in (v(t.get("stimuli")) or []) if isinstance(s, str)],
             "conditions": [{"name": c} for c in conds if c], "entity_kind": kind}
        if kind in GATED:
            lab, route, agree, B = None, "gate", [], None
        else:
            lab, route, agree, B = CB.identity(m)
            # Astra descriptions are procedural: nearly all mention a fixation cross or rest blocks, so a
            # pseudo-task hit in the description says nothing about the paradigm (34 of 354 smoke tasks)
            if route == "A:description" and CB.fam(lab) == "pseudo_tasks":
                lab, route, agree, B = CB.identity({**m, "description": ""})
        dec = CB.D(name)
        fam = CB.fam(lab)
        if (study, t.get("local_id")) in final:
            x = final[(study, t.get("local_id"))]; lab, fam, route = x["canonical"], x["family"], x["decided_by"]
        label[t.get("local_id")] = (lab, fam)
        st["tasks"] += 1; st[f"kind:{kind}"] += 1; st[f"route:{route}"] += 1
        raw_atoms = sorted({a for c in conds for a in normalize_condition(c)["atoms"]})
        T.append({
            "study": study, "task": t.get("local_id"), "name": name, "entity_kind": kind,
            "family": fam, "stem": STEM_OF.get(lab, lab) if lab else None, "canonical": lab,
            "canonical_name": CB.L.names.get(lab) if lab else None,
            "variant": dec.get("variant") if dec.get("canonical_task") == lab else None,
            "qualifiers": dec.get("qualifiers"), "route": route, "agree": agree,
            "route_b": list(B) if B else None, "condition_names": conds, "raw_condition_atoms": raw_atoms,
            "description": m["description"][:600]})
    # ---- analyses and contrasts, read off the graph:
    # cell -> ModelTerm -> FactorLevel (by label) -> conditions | groups | arms | timepoints | regions
    referenced = set()
    measures = {m.get("local_id"): m for m in r.get("measures") or []}
    groups = {g.get("local_id"): g for g in r.get("groups") or []}
    for a in (x for x in r.get("analyses") or [] if isinstance(x, dict)):        # a 27B record carries a stray string
        eff = a.get("effect") or {}
        cells = []
        for cell in eff.get("cells") or []:
            term = terms.get(cell.get("term")) or {}
            lvl = v(cell.get("level")); d = v(cell.get("direction"))
            fl = next((L for L in term.get("levels") or [] if fold(v(L.get("level"))) == fold(lvl)), None) if lvl else None
            carries = {k: list(fl.get(k) or []) for k in ("conditions", "groups", "arms", "timepoints", "regions")} if fl else {}
            carries = {k: x for k, x in carries.items() if x}
            cells.append({"term": cell.get("term"), "term_name": text(term.get("name")), "term_type": v(term.get("type")),
                          "variation_level": v(term.get("variation_level")), "level": lvl, "direction": d,
                          "label": v(cell.get("label")), "carries": carries,
                          "assessment": term.get("assessment"), "region": term.get("region"),
                          "interaction_with": term.get("interaction_with") or []})
        directed = [c for c in cells if c["direction"] in ("positive", "negative")]
        # what the effect is tested ACROSS: every entity type a cell's level carries, besides conditions, whatever
        # its direction -- a two-sided or null group comparison names its groups with no direction to state
        # a factor is compared only when >= 2 of its levels enter; a single level restricts the effect to that
        # entity (a within-patients effect), which is `restricted_to`, not a comparison
        levels_of = {}
        for c in cells:
            for k in c["carries"]:
                if k != "conditions":
                    levels_of.setdefault(k, set()).add(fold(c["level"]))
        across = sorted({k for k, ls in levels_of.items() if len(ls) >= 2} |
                        ({"covariate"} if any(c["term_type"] == "continuous" for c in cells) else set()))
        restricted_to = sorted({i for c in cells for k, ids in c["carries"].items()
                                if k != "conditions" and len(levels_of.get(k, ())) == 1 for i in ids})
        across_directed = sorted({k for c in directed for k in c["carries"] if k != "conditions"})
        unlinked = sorted({f"{c['term_name']}={c['level']}" for c in cells if c["level"] is not None and not c["carries"]})
        mid = a.get("measure"); mid = mid[0] if isinstance(mid, list) and mid else mid
        m = measures.get(mid) or {}
        atasks = [t for t in a.get("tasks") or [] if isinstance(t, str)]
        cond_cells = [c for c in directed if c["carries"].get("conditions")]
        for c in cond_cells:
            referenced.update(i for i in c["carries"]["conditions"] if i in cond)
        tids = set(atasks) or {cond_task[i] for c in cond_cells for i in c["carries"]["conditions"] if i in cond_task}
        tid = next(iter(tids)) if len(tids) == 1 else None
        lab, fam = label.get(tid, (None, None))
        row = {"study": study, "analysis": a.get("local_id"), "name": text(a.get("name")),
               "definition": text(a.get("definition"))[:600], "effect_kind": v(eff.get("kind")),
               "tested_across": across, "across_directed": across_directed, "restricted_to": restricted_to,
               "covariates": sorted({c["assessment"] or c["term_name"] for c in cells if c["term_type"] == "continuous"}),
               "measure_family": v(m.get("family")), "measure_type": v(m.get("type")),
               "outcome": v(a.get("outcome")), "spatial_scope": v(a.get("spatial_scope")),
               "inference_settings": a.get("inference_settings"),
               "population": [{"group": g.get("group"), "n": v(g.get("n")), "is_healthy": v((groups.get(g.get("group")) or {}).get("is_healthy"))}
                              for g in a.get("groups") or [] if isinstance(g, dict)],
               "tasks": atasks or sorted(tids), "task": tid, "family": fam, "canonical": lab,
               "unlinked_levels": unlinked, "cells": cells}
        A.append(row); st["analyses"] += 1
        if not cond_cells:
            st["analyses:no_condition_cell"] += 1
            continue

        def side(items):
            out = []
            for c in items:
                for i in c["carries"]["conditions"]:
                    if i not in cond:
                        continue
                    nm = text(cond[i].get("name")); n = condition_atoms(cond[i], lab, fam)
                    stim = [str(x).lower() for x in (v(cond[i].get("stimulus_content")) or [])]
                    out.append({"condition": i, "name": nm, "level": c["level"], "kind": v(cond[i].get("condition_kind")),
                                "atoms": n["atoms"], "phases": n["phases"], "stimulus": stim,
                                "atoms_from_description": n["atoms_from_description"], "atoms_from_stimulus": n["atoms_from_stimulus"],
                                "token": token(n["atoms"], n["phases"]) if n["atoms"] else "~" + (n["residue"] or fold(nm))})
            return out
        pos = side([c for c in cond_cells if c["direction"] == "positive"])
        neg = side([c for c in cond_cells if c["direction"] == "negative"])
        pb = [x["kind"] in BASELINE for x in pos]; nb = [x["kind"] in BASELINE for x in neg]
        if pos and neg:
            shape = ("target>baseline" if not any(pb) and all(nb) else
                     "reversed" if all(pb) and not any(nb) else "neither")
        else:
            shape = "one_sided"
        tgt, base = (neg, pos) if shape == "reversed" else (pos, neg)
        sign = -1 if shape == "reversed" else 1
        # atoms both sides carry are the task's material (words in a Stroop, faces in a face task), not what the
        # contrast manipulates: they leave the key and are kept as `shared_atoms`
        shared = (set.intersection(*[set(x["atoms"]) for x in tgt]) & set.intersection(*[set(x["atoms"]) for x in base])) if tgt and base else set()
        if shared:
            for x in tgt + base:
                left = [a for a in x["atoms"] if a not in shared]
                if left:
                    x["token"] = token(left, x["phases"])
        if tgt and base and {s["condition"] for s in tgt} == {s["condition"] for s in base}:
            # the same conditions on both sides: the levels are finer than the conditions they carry
            # (positions within a switch run, early vs late CS+), so the level is the identity
            for s in tgt + base:
                n = normalize_condition_ctx(s["level"] or s["name"], lab, fam)
                s["name"] = f"{s['name']} [{s['level']}]"; s["atoms"] = n["atoms"] or s["atoms"]
                s["token"] = "~" + fold(s["level"] or s["name"])
        elif tgt and base and {s["token"] for s in tgt} == {s["token"] for s in base}:
            for s in tgt + base:   # atoms do not separate the sides: the condition names do
                s["token"] = "~" + fold(s["name"])
        tkey = ";".join(sorted({s["token"] for s in tgt})) or "-"
        if shape in ("target>baseline", "reversed") and not any(s["atoms"] for s in base):
            bkey = "+".join(sorted({s["kind"] or "?" for s in base}))
        elif shape in ("target>baseline", "reversed"):
            # a baseline that carries atoms is keyed by them, whatever condition_kind the extraction gave it:
            # 0-back is `control_state` in one paper and `task_state` in another, and is the same baseline
            bkey = "vs:" + ";".join(sorted({s["token"] for s in base}))
        else:
            bkey = "vs:" + ";".join(sorted({s["token"] for s in base})) if base else "-"
        st[f"shape:{shape}"] += 1; st["contrasts"] += 1
        st["contrasts:tested_across:" + ("+".join(across) or "none")] += 1
        K.append({
            **{k: row[k] for k in ("study", "analysis", "name", "effect_kind", "tested_across", "across_directed", "restricted_to", "covariates",
                                  "measure_family", "measure_type", "outcome", "spatial_scope", "population",
                                  "task", "family", "canonical", "unlinked_levels")},
            "shape": shape, "sign": sign, "target": tgt, "baseline": base, "shared_atoms": sorted(shared),
            "key": f"{fam or f'task:{study}/{tid}'}|{tkey}|{bkey}"})
    # ---- conditions in task context
    for cid, c in cond.items():
        lab, fam = label.get(cond_task[cid], (None, None))
        nm = text(c.get("name")); n = condition_atoms(c, lab, fam)
        sch = SCH.get(lab)
        st["conditions"] += 1; st["conditions:referenced"] += cid in referenced
        Cn.append({
            "study": study, "task": cond_task[cid], "condition": cid, "name": nm,
            "extracted_kind": v(c.get("condition_kind")), "kind": n["kind"], "phases": n["phases"],
            "atoms": n["atoms"], "residue": n["residue"], "family": fam, "canonical": lab,
            "in_schema": None if sch is None else [a.split(":")[0] in sch for a in n["atoms"]],
            "referenced": cid in referenced,
            # not in the build's table: which atoms the name did not give, for the sidecar's `also_read`
            "atoms_from_description": n["atoms_from_description"], "atoms_from_stimulus": n["atoms_from_stimulus"],
            "identity": f"{lab or fam or f'task:{study}/{cond_task[cid]}'}|{','.join(sorted(n['atoms'])) or n['residue'] or fold(nm)}"})
    # ---- the contrast route's evidence, once the task's contrasts are read (not in the build's table)
    expand = expander(abbreviations, study)
    by_task = {t.get("local_id"): t for t in tasks}
    for row in T:
        names = [k["name"] for k in K if k["task"] == row["task"]]
        row["evidence"] = evidence(by_task[row["task"]], names, CB.L, expand)
    return {"tasks": T, "conditions": Cn, "contrasts": K, "analyses": A}, st
