"""The structure of each analysis in an extraction record, read from the record alone.

Per analysis: its `kind`, its `measure` (activation, or connectivity with its method and seeds), its two sides as
the record's cells state them, the groups on each side, the baseline type, the conditions held fixed, the
covariates and a correlation's sign. Nothing is inferred from wording, and the
sides are never turned round: the extraction already orders them by the sign of the reported statistics.
`statistics` reports that sign (the parse's t/z values) for consumers that check it.

    assemble(record, parse=None) -> [structure, ...], one per analysis

Cells are resolved cell -> ModelTerm -> FactorLevel (by level label) -> the entities the level carries.
"""
import re

KINDS = ("conjunction", "correlation", "parametric", "main_effect", "group_by_condition", "condition_interaction",
         "group_difference", "condition_vs_baseline", "condition_vs_condition", "no_contrast")
MEASURES = ("activation", "connectivity")
CARRIED = ("conditions", "groups", "arms", "timepoints", "regions")
SIDE_ENTITIES = ("conditions", "arms", "timepoints")
BASELINE_TYPE = {"rest": "rest", "fixation": "fixation", "control_state": "low_level_control",
                 "implicit_baseline": "implicit"}


def v(x):
    return x.get("value") if isinstance(x, dict) else x


def text(x):
    x = v(x)
    return "; ".join(map(str, x)) if isinstance(x, list) else (x or "")


def fold(s):
    return re.sub(r"[^a-z0-9]+", " ", str(s).lower()).strip() if s is not None else ""


def snake(s):
    return re.sub(r"[^a-z0-9]+", "_", str(s or "").lower()).strip("_")


def terms_of(record):
    return {t.get("local_id"): t for m in record.get("model_estimations") or [] for t in m.get("terms") or []}


def read_cells(analysis, terms):
    """The analysis's cells, each resolved to its term and to the entities its level carries."""
    cells = []
    for cell in (analysis.get("effect") or {}).get("cells") or []:
        term = terms.get(cell.get("term")) or {}
        lvl = v(cell.get("level"))
        fl = next((L for L in term.get("levels") or [] if fold(v(L.get("level"))) == fold(lvl)), None) if lvl else None
        carries = {k: list(fl.get(k) or []) for k in CARRIED} if fl else {}
        cells.append({"term": cell.get("term"), "term_name": text(term.get("name")), "term_type": v(term.get("type")),
                      "variation_level": v(term.get("variation_level")), "level": lvl,
                      "direction": v(cell.get("direction")), "label": v(cell.get("label")),
                      "carries": {k: x for k, x in carries.items() if x},
                      "assessment": term.get("assessment"), "region": term.get("region"),
                      "interaction_with": term.get("interaction_with") or []})
    return cells


def statistics_sign(parse, key):
    """`negative`, `positive` or `mixed` over the t and z values of the parse analysis `key`, else `none`."""
    pa = next((x for x in (parse or {}).get("analyses") or [] if x.get("key") == key), None) if key else None
    vals = [x.get("value") for pt in (pa or {}).get("points") or [] for x in pt.get("values") or []
            if isinstance(x.get("value"), (int, float)) and x.get("kind") in ("t", "z")]
    if not vals:
        return "none"
    return "negative" if all(x < 0 for x in vals) else "positive" if all(x > 0 for x in vals) else "mixed"


def group_facts(group):
    mc = [e.get("label") or e.get("value") for e in ((group.get("medical_condition") or {}).get("normalized") or [])
          if e.get("status") == "mapped" and str(e.get("vocabulary", "")).lower() in ("mondo", "onvoc")]
    return {"id": group.get("local_id"), "healthy": v(group.get("is_healthy")) is True,
            "condition": mc[0] if mc else None, "name": snake(v(group.get("name")) or group.get("local_id"))}


def group_names(pos, neg):
    """`healthy_controls` / `patients_<condition>` when exactly one side is all healthy, else the groups' names
    (a patient group by its condition)."""
    one_healthy = bool(pos and neg) and all(x["healthy"] for x in pos) != all(x["healthy"] for x in neg)

    def name(x):
        if one_healthy:
            if x["healthy"]:
                return "healthy_controls"
            if x["condition"]:
                return "patients_" + snake(x["condition"])
            return x["name"] if "patient" in x["name"] else "patients_" + x["name"]
        return snake(x["condition"]) if x["condition"] and not x["healthy"] else x["name"]
    return sorted({name(x) for x in pos}), sorted({name(x) for x in neg})


def group_cells(cells):
    """(compared, restricting): group cells of a group factor with >= 2 levels in the analysis, and those whose
    factor enters with one level only (a within-patients effect)."""
    gc = [c for c in cells if c["carries"].get("groups")]
    levels = {}
    for c in gc:
        levels.setdefault(c["term"], set()).add(fold(c["level"]))
    return [c for c in gc if len(levels[c["term"]]) >= 2], [c for c in gc if len(levels[c["term"]]) == 1]


def _kind(effect_kind, details_type, cells, conditions):
    """The comparison, whatever is measured: a connectivity contrast takes the kind its cells give."""
    directed = [c for c in cells if c["direction"] in ("positive", "negative")]
    group = [c for c in group_cells(cells)[0] if c["direction"] in ("positive", "negative")]
    within = [c for c in directed if any(c["carries"].get(k) for k in SIDE_ENTITIES)]
    continuous = [c for c in cells if c["term_type"] == "continuous"]
    if "conjunction" in details_type:
        return "conjunction"
    if effect_kind == "parametric_modulation":
        return "parametric"
    if effect_kind == "cross_subject_regression" or continuous:
        return "correlation"
    if effect_kind == "omnibus":
        return "main_effect"
    if group and within and {c["direction"] for c in within} == {"positive", "negative"}:
        return "group_by_condition"     # (g1 > g2) x (a > b)
    if group:
        return "group_difference"       # g1 > g2 | a: the condition, if any, is held
    if effect_kind == "interaction" and len({c["term"] for c in within}) >= 2:
        return "condition_interaction"
    if not within:
        return "no_contrast"
    base = [i for c in within if c["direction"] == "negative" for i in c["carries"].get("conditions", [])]
    kinds = [v((conditions.get(i) or {}).get("condition_kind")) for i in base]
    if not base or all(k in ("rest", "fixation", "implicit_baseline") for k in kinds):
        return "condition_vs_baseline"
    return "condition_vs_condition"


def _side(cells, direction):
    side = {k: [] for k in SIDE_ENTITIES}
    side["levels"] = []
    for c in cells:
        if c["direction"] != direction or not any(c["carries"].get(k) for k in SIDE_ENTITIES):
            continue
        side["levels"].append(c["level"])
        for k in SIDE_ENTITIES:
            side[k] += [i for i in c["carries"].get(k, []) if i not in side[k]]
    return side


def _baseline_type(kind, baseline, conditions):
    if kind in ("group_difference", "correlation", "parametric", "main_effect", "no_contrast"):
        return "none" if not baseline["levels"] else _baseline_type("condition_vs_condition", baseline, conditions)
    if not baseline["levels"]:
        return "implicit"
    kinds = {v((conditions.get(i) or {}).get("condition_kind")) for i in baseline["conditions"]}
    if len(kinds) == 1:
        return BASELINE_TYPE.get(next(iter(kinds)), "active_condition")
    return "active_condition"


def connectivity_of(details, regions):
    """Method, seeds and modulatory input of a ConnectivityDetails, else None."""
    if "connectivity" not in str(v(details.get("details_type")) or "").lower():
        return None
    seeds = [s for s in (details.get("seed_regions") or []) if isinstance(s, str)]
    return {"method": v(details.get("connectivity_method")),
            "seeds": [{"id": s, "name": text((regions.get(s) or {}).get("name")) or None} for s in seeds],
            "modulatory_input": v(details.get("modulatory_input"))}


def analysis_structure(analysis, terms, conditions, groups, parse=None, regions=None):
    cells = read_cells(analysis, terms)
    eff = analysis.get("effect") or {}
    effect_kind = v(eff.get("kind"))
    details = analysis.get("details") if isinstance(analysis.get("details"), dict) else {}
    details_type = str(v(details.get("details_type")) or "").lower()
    kind = _kind(effect_kind, details_type, cells, conditions)
    target, baseline = _side(cells, "positive"), _side(cells, "negative")
    one_sided = []
    if kind == "group_difference":   # a condition on one side only is the condition the groups are compared on
        one_sided = target["conditions"] + baseline["conditions"]
        target, baseline = _side([], "positive"), _side([], "negative")
    compared, restricting = group_cells(cells)

    def facts(direction):
        return [group_facts(groups[g]) for c in compared if c["direction"] == direction
                for g in c["carries"]["groups"] if g in groups]
    gt, gb = group_names(facts("positive"), facts("negative"))
    held = sorted({i for c in cells if c["direction"] not in ("positive", "negative")
                   for i in c["carries"].get("conditions", [])} | set(one_sided))
    restricted = sorted({g for c in restricting for g in c["carries"]["groups"]})
    continuous = [c for c in cells if c["term_type"] == "continuous"]
    sign = None
    if kind == "correlation" and continuous and continuous[0]["direction"] in ("positive", "negative"):
        sign = continuous[0]["direction"]
    connectivity = connectivity_of(details, regions or {})
    return {"analysis": analysis.get("local_id"), "kind": kind, "effect_kind": effect_kind,
            "measure": "connectivity" if connectivity else "activation", "connectivity": connectivity,
            "target": target, "baseline": baseline, "groups": {"target": gt, "baseline": gb},
            "held": held, "restricted_to": restricted,
            "baseline_type": _baseline_type(kind, baseline, conditions),
            "covariates": sorted({c["assessment"] or c["term_name"] for c in continuous}), "sign": sign,
            "statistics": statistics_sign(parse, v(analysis.get("source_table_analysis")))}


def assemble(record, parse=None):
    """One structure per analysis of `record` (a study_schema extraction record), in record order."""
    terms = terms_of(record)
    conditions = {c.get("local_id"): c for t in record.get("tasks") or [] for c in t.get("conditions") or []
                  if isinstance(c, dict)}
    groups = {g.get("local_id"): g for g in record.get("groups") or [] if isinstance(g, dict)}
    regions = {r.get("local_id"): r for r in record.get("regions") or [] if isinstance(r, dict)}
    return [analysis_structure(a, terms, conditions, groups, parse, regions)
            for a in record.get("analyses") or [] if isinstance(a, dict)]
