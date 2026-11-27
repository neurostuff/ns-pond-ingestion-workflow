"""Writing the normalization into the record's `normalized` sidecars (study_schema#56), with pondie's writer.

Each entry names this package's module and the run version in `mapping_tool`
(`ingestion_workflow.normalization.<module> <version>`), so a rerun replaces this module's entries and leaves
every other normalizer's alone. Where the entries go:

    tasks[].name                       astra canonical task, astra task family, ns-pond task confidence,
                                       ns-pond task factor, condition context: <axis>
    tasks[].conditions[].name          ns-pond condition kind, condition atom: <axis>, ns-pond condition expansion
    groups[].medical_condition         MONDO, ONVOC (pondie's entry shape), astra medical role
    analyses[].name                    ns-pond contrast key v2, ns-pond contrast kind, ns-pond contrast measure,
                                       ns-pond contrast baseline type, ns-pond contrast manipulated

`acquisitions[].modality` carries pondie's `Modality` entry, which pondie's `apply_normalized` writes.

The task and family keep the inventory's id space and the names Neurostore reads. Atoms carry the condition
vocabulary's version in `method`. An analysis's contrast is its `effect`, which is not an extracted value and so
has no sidecar: the contrast entries sit on the analysis's `name`, with `also_read` naming `effect` and `tasks`, as
an entity-level task label sits on the task's name.
"""
from pondie.normalization import _sidecar
from pondie.normalization import medical_condition as mc
from pondie.normalization._records import value_of

MAPPED, UNMAPPED = _sidecar.MAPPED, _sidecar.UNMAPPED
#: What the model read of a task besides its name.
TASK_ALSO_READ = ["description", "conditions"]
CONTRAST_ALSO_READ = ["effect", "tasks"]


def tool(module, version):
    return f"ingestion_workflow.normalization.{module} {version}"


def _by_id(items):
    return {str(value_of(x.get("local_id")) or ""): x for x in items or [] if isinstance(x, dict)}


def write_tasks(record, designs, model, vocabulary_version, version):
    """Each task's design onto its name and its conditions' names. `designs`: task id -> the design
    (`design.read`), or a str saying why the task has none."""
    from .design import inventory

    mapping_tool = tool("design", version)
    names = inventory()
    tally = {"tasks": 0, "no_design": 0, "conditions": 0}
    for tid, task in _by_id(record.get("tasks")).items():
        if not isinstance(task.get("name"), dict):
            continue
        d = designs.get(tid, "no conditions")
        tally["tasks"] += 1
        conditions = _by_id(task.get("conditions"))
        if isinstance(d, str):
            tally["no_design"] += 1
            _sidecar.replace(task["name"], mapping_tool, [
                _sidecar.entry("astra canonical task", UNMAPPED, mapping_tool, method=d)])
            for cond in conditions.values():
                if isinstance(cond.get("name"), dict):
                    _sidecar.replace(cond["name"], mapping_tool, [])
            continue
        common = {"method": model, "also_read": TASK_ALSO_READ}
        entries = [
            _sidecar.entry("astra canonical task", MAPPED, mapping_tool, value=d["task"],
                           label=names[d["task"]]["name"], **common) if d["task"] else
            _sidecar.entry("astra canonical task", UNMAPPED, mapping_tool, method=(
                f"{model}: {d['task_off_inventory']!r} is not an inventory task" if d.get("task_off_inventory")
                else f"{model}: no inventory task fits"), also_read=TASK_ALSO_READ),
            _sidecar.entry("astra task family", MAPPED, mapping_tool, value=d["family"], **common) if d["family"]
            else _sidecar.entry("astra task family", UNMAPPED, mapping_tool, **common)]
        if d["task_confidence"]:
            entries.append(_sidecar.entry("ns-pond task confidence", MAPPED, mapping_tool,
                                          value=d["task_confidence"], **common))
        method = f"{model}, condition vocabulary {vocabulary_version}"
        entries += [_sidecar.entry("ns-pond task factor", MAPPED, mapping_tool, value=f["axis"], method=method + (
            f"; from model term {f['from_term']!r}" if f.get("from_term") else "")) for f in d["factors"]]
        entries += [_sidecar.entry(f"condition context: {x['axis']}", MAPPED, mapping_tool, value=x["value"],
                                   method=method) for x in d["context"]]
        _sidecar.replace(task["name"], mapping_tool, entries)
        answered = {c["id"]: c for c in d["conditions"]}
        for cid, cond in conditions.items():
            if not isinstance(cond.get("name"), dict):
                continue
            c = answered.get(cid)
            if c is None:
                _sidecar.replace(cond["name"], mapping_tool, [_sidecar.entry(
                    "ns-pond condition kind", UNMAPPED, mapping_tool, method=f"{model}: not in the answer")])
                continue
            got = [_sidecar.entry("ns-pond condition kind", MAPPED if c["kind"] else UNMAPPED, mapping_tool,
                                  value=c["kind"], label=c.get("gloss"), method=model)]
            got += [_sidecar.entry(f"condition atom: {a['axis']}", MAPPED, mapping_tool, value=a["value"],
                                   method=method) for a in c["atoms"]]
            if c.get("expansion"):
                got.append(_sidecar.entry("ns-pond condition expansion", MAPPED, mapping_tool,
                                          value=c["expansion"], method=model))
            _sidecar.replace(cond["name"], mapping_tool, got)
            tally["conditions"] += 1
    return tally


def write_medical(record, links, version):
    mapping_tool = tool("medical", version)
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


def write_contrasts(record, identities, version):
    """Each analysis's contrast identity (`identity.identities`) onto its name."""
    mapping_tool = tool("contrasts", version)
    tally = {"keys": 0, "incomplete": 0}
    for aid, analysis in _by_id(record.get("analyses")).items():
        if not isinstance(analysis.get("name"), dict):
            continue
        k = identities.get(aid)
        if k is None:
            _sidecar.replace(analysis["name"], mapping_tool, [])
            continue
        how = "assembled from the record and its task's design" + ("" if k["complete"] else
                                                                     "; some conditions have no atoms")
        common = {"also_read": CONTRAST_ALSO_READ}
        entries = [_sidecar.entry("ns-pond contrast key v2", MAPPED, mapping_tool, value=k["key"],
                                  label=k["canonical"], method=how, **common),
                   _sidecar.entry("ns-pond contrast kind", MAPPED, mapping_tool, value=k["kind"], **common),
                   _sidecar.entry("ns-pond contrast measure", MAPPED, mapping_tool, value=k["measure"], **common),
                   _sidecar.entry("ns-pond contrast baseline type", MAPPED, mapping_tool, value=k["baseline_type"],
                                  **common)]
        entries += [_sidecar.entry("ns-pond contrast manipulated", MAPPED, mapping_tool, value=a, **common)
                    for a in k["manipulated"]]
        _sidecar.replace(analysis["name"], mapping_tool, entries)
        tally["keys"] += 1
        tally["incomplete"] += not k["complete"]
    return tally
