"""Writing the normalization into the record's `normalized` sidecars (study_schema#56), with pondie's writer.

Each entry names this package's module and the fit it ran under in `mapping_tool`
(`ingestion_workflow.normalization.<module> <fit_id>`), so a rerun under any fit replaces this module's entries and
leaves every other normalizer's alone. Where the entries go:

    tasks[].name                       astra canonical task, astra task family
    tasks[].conditions[].name          astra condition atom: <axis> (one vocabulary per axis), astra condition phase
    groups[].medical_condition         MONDO, ONVOC (pondie's entry shape), astra medical role
    analyses[].name                    astra contrast key, ns-pond contrast set <fit_id>,
                                       ns-pond contrast set orientation <fit_id>

An analysis's contrast is its `effect`, which is not an extracted value and so has no sidecar: the contrast
entries sit on the analysis's `name`, with `also_read` naming `effect` and `tasks`, as an entity-level task label
sits on the task's name.
"""
from pondie.normalization import _sidecar
from pondie.normalization import medical_condition as mc
from pondie.normalization._records import value_of

MAPPED, UNMAPPED = _sidecar.MAPPED, _sidecar.UNMAPPED

#: The Task slots each `decided_by` route read besides `name` (pondie's migrate_normalized_sidecar.py). The
#: contrast route also read contrast names, which live on another entity and have no slot to name here.
TASK_ALSO_READ = {
    "name": [],
    "citation": ["references"],
    "contrast": ["conditions"],
    "taskcat": ["description"],
    "rest": ["description"],
    "gate": ["description"],
}
#: Which Condition slot each atom source is.
ATOM_SLOT = {"name": None, "description": "description", "stimulus": "stimulus_content"}


def tool(module, fit_id):
    return f"ingestion_workflow.normalization.{module} {fit_id}"


def _by_id(items):
    return {str(value_of(x.get("local_id")) or ""): x for x in items or [] if isinstance(x, dict)}


def write_tasks(record, labels, fit_id):
    mapping_tool = tool("tasks", fit_id)
    n = 0
    for tid, task in _by_id(record.get("tasks")).items():
        row = labels.get(tid)
        if row is None or not isinstance(task.get("name"), dict):
            continue
        by = row["decided_by"]
        common = {"method": by, "also_read": TASK_ALSO_READ.get(by),
                  "confidence": row["routes"]["citation_conf"] if by == "citation" else
                  row["routes"]["contrast_p"] if by == "contrast" else None}
        if row["canonical"]:
            canon = _sidecar.entry("astra canonical task", MAPPED, mapping_tool, value=row["canonical"],
                                   label=row["canonical_name"],
                                   qualifiers=[q["text"] if isinstance(q, dict) else str(q)
                                               for q in row.get("qualifiers") or []], **common)
        else:
            canon = _sidecar.entry("astra canonical task", UNMAPPED, mapping_tool,
                                   **{**common, "method": f"{by}: {row['entity_kind']}" if by == "gate" else by})
        fam = (_sidecar.entry("astra task family", MAPPED, mapping_tool, value=row["family"], **common)
               if row["family"] else _sidecar.entry("astra task family", UNMAPPED, mapping_tool, method=by,
                                                    also_read=common["also_read"]))
        _sidecar.replace(task["name"], mapping_tool, [canon, fam])
        n += 1
    return {"set": n}


def write_conditions(record, conditions, fit_id):
    mapping_tool = tool("conditions", fit_id)
    rows = {r["condition"]: r for r in conditions}
    n = 0
    for task in record.get("tasks") or []:
        for cid, cond in _by_id(task.get("conditions") if isinstance(task, dict) else None).items():
            row = rows.get(cid)
            if row is None or not isinstance(cond.get("name"), dict):
                continue
            entries = []
            for a in row["atoms"]:
                axis, val = a.split(":", 1)
                src = "description" if a in row["atoms_from_description"] else \
                    "stimulus" if a in row["atoms_from_stimulus"] else "name"
                entries.append(_sidecar.entry(f"astra condition atom: {axis}", MAPPED, mapping_tool, value=val,
                                              method=f"from {src}",
                                              also_read=[ATOM_SLOT[src]] if ATOM_SLOT[src] else None))
            for p in row["phases"]:
                entries.append(_sidecar.entry("astra condition phase", MAPPED, mapping_tool, value=p))
            if not entries:
                entries.append(_sidecar.entry("astra condition atom", UNMAPPED, mapping_tool,
                                              method=f"residue: {row['residue']}" if row["residue"] else "no atom"))
            _sidecar.replace(cond["name"], mapping_tool, entries)
            n += len(entries)
    return {"set": n}


def write_medical(record, links, fit_id):
    mapping_tool = tool("medical", fit_id)
    from .medical import role

    by_group = {}
    for link in links:
        by_group.setdefault(link.group, []).append(link)
    tally = {"set": 0, "unmapped": 0}
    for gid, group in _by_id(record.get("groups")).items():
        if not isinstance(group.get("medical_condition"), dict):
            continue
        rows = sorted(by_group.get(gid, []), key=lambda r: r.item)
        written, seen = [], set()
        for row in rows:
            written += mc.entries(row, mapping_tool)
            if row.item not in seen:                 # the role is the item's, whichever head it names
                seen.add(row.item)
                written.append(_sidecar.entry("astra medical role", MAPPED, mapping_tool, value=role(row),
                                              item=row.item))
        _sidecar.replace(group["medical_condition"], mapping_tool, written)
        tally["set"] += len(written)
        tally["unmapped"] += sum(e["status"] == UNMAPPED for e in written)
    return tally


def write_contrasts(record, contrasts, assigned, fit_id, threshold):
    mapping_tool = tool("contrast_sets", fit_id)
    rows = {k["analysis"]: k for k in contrasts}
    tally = {"keys": 0, "assigned": 0, "unassigned": 0}
    for aid, analysis in _by_id(record.get("analyses")).items():
        if not isinstance(analysis.get("name"), dict):
            continue
        k = rows.get(aid)
        if k is None:
            _sidecar.replace(analysis["name"], mapping_tool, [])
            continue
        read = ["effect", "tasks"]
        entries = [_sidecar.entry("astra contrast key", MAPPED, mapping_tool, value=k["key"],
                                  method=f"{k['shape']}; sign {k['sign']:+d}", also_read=read)]
        tally["keys"] += 1
        doc_id = f"{k['study']}/{aid}"
        if doc_id in assigned:
            a = assigned[doc_id]
            vocab = f"ns-pond contrast set {fit_id}"
            if a is None:
                entries.append(_sidecar.entry(vocab, UNMAPPED, mapping_tool, also_read=read,
                                              method=f"no same-family set at average similarity >= {threshold}"))
                tally["unassigned"] += 1
            else:
                how = f"average similarity to its {a['n_members']} members"
                entries.append(_sidecar.entry(vocab, MAPPED, mapping_tool, value=a["set"], label=a["label"],
                                              confidence=min(1.0, max(0.0, a["similarity"])), method=how,
                                              also_read=read))
                entries.append(_sidecar.entry(f"ns-pond contrast set orientation {fit_id}", MAPPED, mapping_tool,
                                              value="same" if a["orient"] == 1 else "reversed",
                                              method="against the set's medoid, the set stated in its majority "
                                                     "direction", also_read=read))
                tally["assigned"] += 1
        _sidecar.replace(analysis["name"], mapping_tool, entries)
    return tally
