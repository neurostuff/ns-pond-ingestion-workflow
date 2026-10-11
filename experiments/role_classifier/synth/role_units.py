"""Synthetic labelling units for the sparse set roles, seeded from real units.

    python experiments/role_classifier/synth/role_units.py OUT_DIR --plan plan.json \
        --units table_units.jsonl prose_units.jsonl --labels LABEL_DIR [LABEL_DIR ...] [--seed 41]

Extends `synth_prose.py` (the nu-prose generator, which overweights hard cases) to
study_schema's v4 roles and to tables. Each case starts from a REAL unit whose sets
the labeller called `result`, from a train article (never a held-out or hand
article), and is rewritten into one target role with that role's cues (cue_analysis.py):

  table  reference      prior-study coordinate tables: a Study/Reference column,
                        "taken from previous studies", included-studies lists
         localization   electrode contacts / recording sites / optodes (never MRS voxels: those are anchor:roi)
         other          slice positions of a figure, simulated sources, lesion centroids
         anchor:stimulation_target  TMS / tDCS targets
  text   reference      a peak quoted from another study, with its citation
         localization   where electrodes, contacts, dipoles or recording sites sat
         other          slice/crosshair positions, lesion-overlap peaks, simulated sources,
                        worked-example atlas voxels

and hard negatives that carry a sparse class's cue but are another role: results that
cite prior work, results inside an a-priori ROI, results in a figure legend with its
slice position, prior-study ROIs (anchor + from_prior_study), electrode-level results.

Output (no field of a unit carries its label):
  OUT_DIR/units.jsonl   labelling units in build_sets.py's shape; sets hold only
                        `name` and `points`; dataset "synthetic-v4", so no [PROPOSED]
                        role is rendered and the bulk job skips them
  OUT_DIR/truth.jsonl   the label by construction, per (unit_id, set index)
Real result sets kept inside an appended prose passage carry their real label in truth.
"""
from __future__ import annotations

import argparse
import collections
import json
import random
import re
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))  # synth_prose, prose_coords: sibling scripts

import synth_prose as sp  # noqa: E402
from ingestion_workflow.services.set_roles import labeling  # noqa: E402
from ingestion_workflow.services.set_roles.labels import role_error  # noqa: E402


def set_index(set_id):
    return int(set_id.rsplit("#", 1)[1])


def held_out(u):
    return labeling.held_out(u)


def synthetic(u):
    return str(u.get("dataset") or "").startswith("synthetic") or "synth" in u["unit_id"]


def load_units(paths):
    units = {}
    for path in paths:
        for u in labeling.read_units(Path(path)):
            units[u["unit_id"]] = u
    return units

AUTHORS = ["Smith", "Garcia", "Chen", "Müller", "Rossi", "Kim", "Novak", "Okafor", "Tanaka", "Haddad", "Johansson",
           "Petrov", "Silva", "Nguyen", "O'Brien", "Kowalski", "Dubois", "Sato", "Fernández", "Ahmed", "Van der Berg",
           "Lindqvist", "Morales", "Zhang", "Wang", "Liu", "Brown", "Wilson", "Schmidt", "Ivanova", "Kaplan", "Moreau",
           "Bianchi", "Park", "Yilmaz", "Andersen", "Costa", "Hoffmann", "Ramirez", "Suzuki"]
SPACE_HDR = [("MNI", "MNI coordinates"), ("MNI", "MNI (x, y, z)"), ("TAL", "Talairach coordinates"),
             ("MNI", "Peak MNI coordinates"), ("TAL", "Talairach (x, y, z)"), ("MNI", "Coordinates (mm)")]
SPACE_WORD = {"MNI": "MNI", "TAL": "Talairach"}
MODALITY = {
    "localization": re.compile(r"\b(s?EEG|MEG|ECoG|iEEG|intracranial|electrode|fNIRS|NIRS|epilep\w*)\b", re.I),
    "anchor_roi_mrs": re.compile(r"\b(MRS|spectroscopy|GABA|glutamate|NAA|metabolite\w*)\b", re.I),
    "stimulation_target": re.compile(r"\b(r?TMS|tDCS|tACS|theta.burst|stimulation|neuromodulation)\b", re.I),
    "other": re.compile(r"\b(lesion|stroke|tumou?r|simulat\w*|atlas|patients?)\b", re.I),
}


def cite(rng, numbered=False):
    if numbered or rng.random() < 0.15:
        return f"[{rng.randint(3, 68)}]"
    a, y = rng.choice(AUTHORS), rng.randint(1996, 2024)
    form = rng.choice(["et_al_paren", "et_al_text", "two", "one"])
    if form == "et_al_paren":
        return f"({a} et al., {y})"
    if form == "et_al_text":
        return f"{a} et al. ({y})"
    if form == "two":
        return f"({a} and {rng.choice(AUTHORS)}, {y})"
    return f"{a} ({y})"


def narr(c):
    """'(Smith et al., 2010)' -> 'Smith et al., 2010'; narrative forms unchanged."""
    return c[1:-1] if c.startswith("(") and c.endswith(")") else c


LIM = (70, 100, 70)  # |x|, |y|, |z| bound of an MNI brain, loosely


def jitter(xyz, rng, mm=8):
    return [max(-l, min(l, int(round(v + rng.randint(-mm, mm))))) for v, l in zip(xyz, LIM)]


def xyz_of(p):
    if isinstance(p, dict):
        return [int(round(float(v))) for v in (p.get("xyz") or [p.get("x"), p.get("y"), p.get("z")])]
    return [int(round(float(v))) for v in p[:3]]


# -- seeds -------------------------------------------------------------------------

def seeds(units, labels):
    """Real units every one of whose labelled sets is `result`, by origin."""
    roles = collections.defaultdict(dict)
    for sid, lab in labels.items():
        roles[lab["unit_id"]][set_index(sid)] = lab["role"]
    out = {"table": [], "text": []}
    for uid, rs in roles.items():
        u = units.get(uid)
        if not u or held_out(u) or synthetic(u) or u.get("dataset") == "wild":
            continue
        if len(rs) == len(u["sets"]) and set(rs.values()) == {"result"}:
            if u["origin"] == "table" and not u.get("table_serialised"):
                continue
            out[u["origin"]].append(u)
    return out


ROWS = [8, 18]  # rows per synthetic set; real tables run ~16 rows, the pilot made 6
_RICH = {}


def pick_seed(pool, rng, want=None):
    if pool and pool[0]["origin"] == "table":
        key = id(pool)
        if key not in _RICH:
            _RICH[key] = [u for u in pool if len(real_rows(u)) >= 3] or pool
        pool = _RICH[key]
    if want is not None:
        fit = [u for u in pool if want.search((u.get("title") or "") + " " + (u.get("abstract") or ""))]
        if len(fit) >= 5:
            return rng.choice(fit)
    return rng.choice(pool)


_REGION_CELL = re.compile(r"(gyrus|gyri|cortex|lobule|insula|amygdala|hippocamp\w*|thalamus|putamen|caudate|precuneus|cerebell\w*|"
                          r"nucleus|operculum|pole|sulcus|cingulate|striatum|pallidum|cuneus|fusiform|lingual|frontal|temporal|"
                          r"parietal|occipital|_[LR]\b)", re.I)
FALLBACK_REGIONS = ["Left inferior frontal gyrus", "Right anterior insula", "Left precuneus", "Right amygdala",
                    "Left superior temporal gyrus", "Right middle frontal gyrus", "Anterior cingulate cortex"]


def real_regions(u):
    """Region names from a real table's body cells."""
    out = []
    for line in (u.get("table_serialised") or "").splitlines():
        for c in line.split(" | "):
            c = re.sub(r"^[#^<0-9]*:?", "", c.strip())
            if _REGION_CELL.search(c) and 4 < len(c) < 50 and not re.search(r"\d{2}", c) and c not in out:
                out.append(c)
    return out or FALLBACK_REGIONS


def real_rows(u):
    """(region, xyz) of a real table's body rows: the region cell and the row's first triple."""
    out, last = [], None
    for line in (u.get("table_serialised") or "").splitlines():
        cells = [re.sub(r"^[#^<0-9]*:?", "", c.strip()) for c in line.split(" | ")]
        reg = next((c for c in cells if _REGION_CELL.search(c) and 4 < len(c) < 50 and not re.search(r"\d{2}", c)), None)
        reg = re.sub(r"\s*\((?!L\)|R\))[^)]*\)", "", reg).strip() if reg else last
        nums = [float(n) for n in re.findall(r"(?<![\w.])-?\d+(?:\.\d+)?", line.replace("−", "-"))]
        for i in range(len(nums) - 2):
            t = nums[i:i + 3]
            if all(abs(v) <= 100 for v in t) and reg:
                out.append((reg, [int(round(v)) for v in t]))
                break
        last = reg or last
    return out


def study_label(rng):
    """A table's study cell: 'Smith et al., 2010', 'Chen and Kim (2015)', 'Rossi, 2008 [12]'."""
    a, y = rng.choice(AUTHORS), rng.randint(1996, 2024)
    return rng.choice([f"{a} et al., {y}", f"{a} et al. ({y})", f"{a} and {rng.choice(AUTHORS)}, {y}", f"{a} ({y})",
                       f"{a} et al. [{rng.randint(3, 60)}]"])


# -- tables ------------------------------------------------------------------------

def _table(header, rows):
    # A body cell that starts with `#` reads as a header cell (table_context.split_table); a row-spanning
    # label is `^n:label` and a continuation `~`, as in the serialisation nspond_tables writes.
    return "\n".join([" | ".join(header)] + [" | ".join(str(c) for c in r) for r in rows])


def table_case(seed, target, rng, hard=False):
    """(unit fields, sets, truth) for one table rewritten into `target`."""
    space, space_hdr = rng.choice(SPACE_HDR)
    paired = real_rows(seed) or [(r, sp.realistic_xyz(rng)) for r in FALLBACK_REGIONS]
    regions = [r for r, _ in paired]
    pts = [p for _, p in paired]
    where = {tuple(p): r for r, p in paired}
    n = rng.randint(3, min(8, max(3, len(pts))))
    tl = seed.get("table_label") or f"Table {rng.randint(1, 5)}"
    tag = tl.split()[-1]
    sw = SPACE_WORD[space]
    if target == "reference":
        studies = [study_label(rng) for _ in range(rng.randint(2, 4))]
        layout = rng.choice(["study_col", "ref_col", "grouped"])
        rows, sets = [], []
        for si, st in enumerate(studies):
            src = [rng.choice(paired) for _ in range(rng.randint(ROWS[0], ROWS[1]))]
            spts = [jitter(p, rng) for _, p in src]
            name = st if rng.random() < 0.7 else f"{st}: {rng.choice(regions)}"
            sets.append({"name": name, "points": [[*p, None, None, None] for p in spts]})
            for (reg, _), p in zip(src, spts):
                if layout == "study_col":
                    rows.append([st, reg, *p])
                elif layout == "ref_col":
                    rows.append([reg, *p, st])
                else:
                    rows.append([f"^{len(spts)}:{st}" if p is spts[0] else "~", reg, *p])
        header = {"study_col": ["#Study", "#Region", f"#<3:{space_hdr}"], "ref_col": ["#Region", f"#<3:{space_hdr}", "#Reference"],
                  "grouped": ["#Study", "#Region", f"#<3:{space_hdr}"]}[layout]
        caption = rng.choice([
            f"Peak coordinates reported in previous studies of {rng.choice(['the same task', 'this contrast', 'the disorder', 'healthy samples'])}.",
            f"{sw} coordinates of the regions reported in earlier studies, listed for comparison with the present findings.",
            f"Studies included in the comparison and the peak coordinates they reported.",
            f"Literature coordinates for {rng.choice(regions).lower()} and neighbouring regions.",
        ])
        footer = rng.choice([
            f"Coordinates are given as reported in the original publications{'; Talairach coordinates were converted to MNI with icbm2tal' if sw == 'MNI' else ''}.",
            f"Coordinates taken from the cited studies ({sw} space).", "", "Only peaks within 10 mm of a present-study cluster are listed."])
        citing = [rng.choice([
            f"{tl} lists the coordinates reported by previous studies ({studies[0].replace(" (", ", ").rstrip(")")}) for comparison.",
            f"Our peaks were within 8 mm of those reported previously ({tl}).",
            f"The location of this effect agrees with earlier reports ({studies[-1].replace(" (", ", ").rstrip(")")}; see {tl})."])]
        truth = [("reference", None, True)] * len(sets)
    elif target == "localization":
        kind = rng.choice(["contacts", "contacts", "optodes"])
        rows, sets = [], []
        for pi in range(rng.randint(1, 3)):
            label = f"P{pi + 1}" if rng.random() < 0.6 else f"Patient {pi + 1}"
            src = [rng.choice(paired) for _ in range(rng.randint(ROWS[0], ROWS[1] + 1))]
            spts = [jitter(p, rng, 6) for _, p in src]
            sets.append({"name": label, "points": [[*p, None, None, None] for p in spts]})
            for ci, p in enumerate(spts):
                el = {"contacts": f"{rng.choice('LR')}{rng.choice('ABCHT')}{rng.randint(1, 12)}", "optodes": f"S{rng.randint(1, 16)}-D{rng.randint(1, 16)}",
                      "mrs": rng.choice(["Session 1", "Session 2", "Mean"])}[kind]
                lab_col = rng.random()
                rows.append([f"^{len(spts)}:{label}" if ci == 0 else "~", el, *p, src[ci][0]])
        header = [f"#{rng.choice(['Patient', 'Subject', 'Case'])}", "#" + {"contacts": "Contact", "optodes": "Channel", "mrs": "Voxel"}[kind],
                  f"#<3:{space_hdr}", f"#{rng.choice(['Anatomical label', 'AAL label', 'Region (Harvard-Oxford)', 'Location'])}"]
        caption = {"contacts": f"{sw} coordinates of the depth-electrode contacts {rng.choice(['in each patient', 'used for the analysis', 'with task-related responses'])}.",
                   "optodes": f"fNIRS channel positions projected onto {sw} space.",
                   "mrs": f"Centre of the {rng.choice(['MRS', '1H-MRS', 'spectroscopy'])} voxel in each participant ({sw})."}[kind]
        footer = {"contacts": rng.choice(["Contacts were localized on the post-implantation CT co-registered to the pre-operative MRI.",
                                          "Anatomical labels from the AAL atlas.", ""]),
                  "optodes": rng.choice(["Optode positions were digitized with a Polhemus system and projected with NIRS-SPM.", ""]),
                  "mrs": rng.choice(["Voxel size 20 x 20 x 20 mm.", "Mean voxel overlap across participants was 78%.", ""])}[kind]
        citing = [f"The location of each {'contact' if kind == 'contacts' else 'channel' if kind == 'optodes' else 'voxel'} is given in {tl}."]
        truth = [("localization", None, False)] * len(sets)
    elif target == "anchor_roi_mrs":
        rows, sets = [], []
        for pi in range(rng.randint(1, 3)):
            label = rng.choice([f"{rng.choice(['M1', 'ACC', 'PCC', 'DLPFC', 'Thalamus', 'Occipital'])} voxel", f"S{pi + 1}", f"Session {pi + 1}"])
            src = [rng.choice(paired) for _ in range(rng.randint(2, 6))]
            spts = [jitter(p, rng, 6) for _, p in src]
            sets.append({"name": label, "points": [[*p, None, None, None] for p in spts]})
            for ci, p in enumerate(spts):
                rows.append([f"^{len(spts)}:{label}" if ci == 0 else "~", f"Subject {ci + 1}", *p, rng.choice(["20 x 20 x 20", "30 x 30 x 30", "25 x 25 x 25"])])
        header = [f"#{rng.choice(['Voxel', 'Region'])}", "#Subject", f"#<3:{space_hdr}", "#Voxel size (mm)"]
        caption = rng.choice([f"Centre of the {rng.choice(['MRS', '1H-MRS', 'spectroscopy'])} voxel in each participant ({sw}).",
                              f"Placement of the MRS voxels used to measure {rng.choice(['GABA', 'glutamate', 'NAA and creatine'])} ({sw} coordinates)."])
        footer = rng.choice(["Voxel size 20 x 20 x 20 mm.", "Mean voxel overlap across participants was 78%.", "Voxels were positioned on the anatomical scan.", ""])
        citing = [f"The location of each voxel is given in {tl}."]
        truth = [("anchor", "roi", False)] * len(sets)
    elif target in ("seed", "node"):
        rows, sets = [], []
        for gi in range(rng.randint(1, 3)):
            reg = rng.choice(regions)
            label = (f"{reg} seed" if target == "seed" else f"{reg} node") if rng.random() < 0.7 else (f"Seed {gi + 1}" if target == "seed" else f"Node {gi + 1}")
            src = [rng.choice(paired) for _ in range(rng.randint(ROWS[0] // 2, ROWS[1] // 2))]
            spts = [jitter(p, rng, 6) for _, p in src]
            sets.append({"name": label, "points": [[*p, None, None, None] for p in spts]})
            for ci, p in enumerate(spts):
                rows.append([f"^{len(spts)}:{label}" if ci == 0 else "~", src[ci][0], *p, rng.choice(["8", "6", "10"]) if target == "seed" else rng.choice(["DMN", "FPN", "Salience", "Dorsal attention"])])
        header = ["#Seed" if target == "seed" else "#Network", "#Region", f"#<3:{space_hdr}", "#Radius (mm)" if target == "seed" else "#Module"]
        caption = rng.choice([f"{sw} coordinates of the seed regions used in the connectivity analysis.", "Seed regions for the functional connectivity analysis."]) if target == "seed" else \
            rng.choice([f"Nodes of the {rng.choice(['default mode', 'frontoparietal', 'salience'])} network and their {sw} coordinates.", f"{sw} coordinates of the network nodes (parcel centres)."])
        footer = rng.choice(["Seeds were spheres centred on these coordinates.", "", "Coordinates are the centres of the seed regions."]) if target == "seed" else \
            rng.choice(["Nodes were defined from the parcellation.", "", "Coordinates are parcel centres of mass."])
        citing = [f"{tl} lists the {'seeds' if target == 'seed' else 'nodes'} of the {'connectivity' if target == 'seed' else 'network'} analysis."]
        truth = [("anchor", target, False)] * len(sets)
    elif target == "other":
        kind = rng.choice(["slices", "simulated", "lesion", "example"])
        rows, sets = [], []
        for gi in range(rng.randint(1, 3)):
            label = {"slices": f"Figure {rng.randint(1, 6)}{'ABC'[gi]}", "simulated": f"Source {gi + 1}",
                     "lesion": f"Patient {gi + 1}", "example": f"Example {gi + 1}"}[kind]
            src = [rng.choice(paired) for _ in range(rng.randint(ROWS[0], ROWS[1]))]
            spts = [jitter(p, rng, 6) for _, p in src]
            sets.append({"name": label, "points": [[*p, None, None, None] for p in spts]})
            for ci, p in enumerate(spts):
                rows.append([f"^{len(spts)}:{label}" if ci == 0 else "~", src[ci][0], *p])
        header = ["#" + {"slices": "Panel", "simulated": "Simulated source", "lesion": "Patient", "example": "Voxel"}[kind], "#Region", f"#<3:{space_hdr}"]
        caption = {"slices": f"Positions of the slices displayed in Figure {rng.randint(1, 6)}.",
                   "simulated": f"Locations of the simulated sources used to test {rng.choice(['localization accuracy', 'the beamformer', 'the inverse solution'])}.",
                   "lesion": "Lesion centre of mass for each patient.",
                   "example": f"Worked examples: voxels and their probabilistic assignment in the {rng.choice(['Jülich', 'Harvard-Oxford', 'Talairach Daemon'])} atlas."}[kind]
        footer = rng.choice(["", f"Coordinates in {sw} space.", "Ground-truth positions; no statistics apply."])
        citing = [{"slices": f"{tl} gives the slice positions used for display.", "simulated": f"The simulated source positions are listed in {tl}.",
                   "lesion": f"See {tl} for each patient's lesion centre.", "example": f"{tl} gives worked examples of the atlas lookup."}[kind]]
        truth = [("other", None, False)] * len(sets)
    elif target == "stimulation_target":
        rows, sets = [], []
        for gi in range(rng.randint(1, 2)):
            label = rng.choice(["Left DLPFC target", "Right IFG target", "Active site", "Control site (vertex)", "Group mean target"])
            src = [rng.choice(paired) for _ in range(rng.randint(ROWS[0], ROWS[1]))]
            spts = [jitter(p, rng, 6) for _, p in src]
            sets.append({"name": label, "points": [[*p, None, None, None] for p in spts]})
            for ci, p in enumerate(spts):
                rows.append([f"^{len(spts)}:{label}" if ci == 0 else "~", *p, rng.choice(["Neuronavigation", "fMRI-guided", "Beam F3", "Individual peak"])])
        header = ["#Site", f"#<3:{space_hdr}", "#Targeting method"]
        caption = rng.choice([f"{rng.choice(['TMS', 'rTMS', 'iTBS', 'tDCS'])} stimulation targets ({sw} coordinates).",
                              "Coordinates of the stimulation sites, by condition."])
        footer = rng.choice(["Targets were located individually with frameless neuronavigation.", "", "The coil was positioned tangentially at 45°."])
        citing = [f"The stimulation targets are listed in {tl}."]
        truth = [("anchor", "stimulation_target", False)] * len(sets)
    else:  # hard negative: a result table with a sparse class's cue
        cue = rng.choice(["cites", "roi", "electrode_result", "slice_note"])
        rows, sets = [], []
        for s in seed["sets"][:3]:
            spts = [xyz_of(p) for p in s["points"]][:5]
            stat = [p[4] if not isinstance(p, dict) and len(p) > 4 else round(rng.uniform(3.2, 7.5), 2) for p in s["points"][:5]]
            sets.append({"name": s.get("name") or "A > B", "points": [[*p, "T", st, None] for p, st in zip(spts, stat)]})
            for ci, (p, st) in enumerate(zip(spts, stat)):
                rows.append([f"^{len(spts)}:{s.get('name') or 'A > B'}" if ci == 0 else "~", rng.choice(regions), *p, st])
        header = ["#Contrast", "#Region", f"#<3:{space_hdr}", "#t"]
        caption = {"cites": f"Regions showing {rng.choice(['greater', 'reduced'])} activation, in line with {cite(rng)}.",
                   "roi": "Peaks within the a priori regions of interest (small-volume corrected).",
                   "electrode_result": "Clusters of significant source-level power change (beamformer, cluster-corrected).",
                   "slice_note": f"Significant clusters (p < 0.05 FWE), shown in Figure {rng.randint(1, 5)} on axial slices."}[cue]
        footer = rng.choice([f"Peaks overlap those reported previously {cite(rng)}.", "ROIs were 8-mm spheres from a prior meta-analysis.", ""])
        citing = [f"{tl} lists the peaks of this effect, consistent with {cite(rng)}."]
        truth = [("result", None, False)] * len(sets)
    hdr = header + ["~"] * 0
    sub = ["~"] * (header.index(next(h for h in header if h.startswith("#<3:")))) + ["#x", "#y", "#z"]
    serial = _table(hdr, [sub + ["~"] * (len(rows[0]) - len(sub))] + rows) if rows else ""
    unit = {"origin": "table", "article_id": f"synth:{seed['article_id']}", "source": seed.get("source"),
            "table_id": f"synth-{rng.randint(0, 10**9)}", "table_label": tl, "title": seed.get("title"),
            "abstract": seed.get("abstract"), "caption": f"{caption}" if rng.random() < 0.5 else f"{tl}. {caption}",
            "footer": footer, "table_serialised": serial, "citing": citing, "sets": sets}
    return unit, truth


# -- prose -------------------------------------------------------------------------

def prose_sentence(target, rng, region, hard=False):
    """(sentence, name, xyz, truth) of one prose set."""
    xyz = sp.realistic_xyz(rng)
    c = sp.coord_text(xyz, rng)
    w = sp.wrap(c)
    ct = cite(rng)
    if target == "reference":
        t = rng.choice([
            f"This peak lies close to the {region} coordinate reported by {narr(ct)} {w}.",
            f"{narr(ct)} reported a similar effect in the {region} at {c}.",
            f"In the meta-analysis by {narr(ct)}, the {region} cluster peaked at {c}.",
            f"Previous work located the {region} at {c} {ct}, about {rng.randint(4, 12)} mm from our peak.",
            f"The {region} coordinate of an earlier study {cite(rng, numbered=True)} {w} falls within our cluster.",
            f"For comparison, the canonical {region} location {w} was taken from {narr(ct)}.",
        ])
        return t, rng.choice([ct.replace("(", "").replace(")", "").strip("[]") if ct.startswith("(") is False else narr(ct), f"{region} (previous study)", f"{region}"]), xyz, ("reference", None, True)
    if target == "localization":
        t = rng.choice([
            f"Electrode {rng.choice('LR')}{rng.choice('ABHT')}{rng.randint(1, 10)} was located in the {region} at {c}.",
            f"In a typical recording site in the {region} {w}, responses emerged at about {rng.randint(120, 300)} ms.",
            f"Dipoles were placed at {c} in the {region}, following the source model.",
            f"Depth-electrode contacts in the {region} had a mean position of {c}.",
            f"The fNIRS optode over the {region} projected to {c}.",
            f"Subdural contacts over the {region} were localized on the post-implant CT at {c}.",
        ])
        name = ("electrode location" if "Electrode" in t else "recording site" if "recording" in t else "dipole location" if "Dipoles" in t
                else "optode location" if "optode" in t else f"{region} contact")
        return t, name, xyz, ("localization", None, False)
    if target == "anchor_roi_mrs":
        t = rng.choice([
            f"The MRS voxel was centred on the {region} (mean position {c}).",
            f"A {rng.choice([20, 25, 30])} mm isotropic spectroscopy voxel was placed in the {region} {w}.",
            f"1H-MRS spectra were acquired from a voxel in the {region} (centre {c}).",
        ])
        return t, rng.choice([f"{region} voxel", "MRS voxel"]), xyz, ("anchor", "roi", False)
    if target == "other":
        t = rng.choice([
            f"Slices are shown at {c}.",
            f"Crosshairs are placed at {c} in the {region}.",
            f"The region of maximal lesion overlap {w} was in the {region}.",
            f"Simulated sources were placed at {c} in the {region}.",
            f"For example, the voxel at {c} is assigned to the {region} with {rng.randint(40, 90)}% probability.",
            f"Image coordinates {w} are in {rng.choice(['MNI', 'Talairach'])} space.",
        ])
        name = ("slice position" if "Slices" in t or "Crosshairs" in t else "lesion overlap" if "lesion" in t else "simulated source" if "Simulated" in t
                else "example voxel" if "example" in t else "image coordinates")
        return t, name, xyz, ("other", None, False)
    if target == "hard_anchor_prior":
        t = rng.choice([
            f"A {rng.choice([6, 8, 10])}-mm sphere was centred on the {region} coordinate reported by {narr(ct)} {w} and used as the ROI.",
            f"The seed was the {region} peak from a previous study {cite(rng, numbered=True)} {w}.",
        ])
        kind = "seed" if "seed" in t else "roi"
        return t, f"{region} {'seed' if kind == 'seed' else 'ROI'}", xyz, ("anchor", kind, True)
    # hard negative result: carries a reference / other / localization cue
    k, v, s = sp.stat_text(rng)
    t = rng.choice([
        f"Consistent with {narr(ct)}, the contrast activated the {region} {sp.wrap(c + '; ' + s)}.",
        f"Figure {rng.randint(1, 5)} shows the {region} cluster ({c}; {s}) on axial slices.",
        f"Electrode-level effects were strongest over the {region} source ({c}; {s}).",
        f"As previously reported {cite(rng, numbered=True)}, activity in the {region} {w} increased with load ({s}).",
    ])
    return t, rng.choice(["task > baseline", "A > B", f"{region} effect"]), xyz, ("result", None, False)


def _split(text):
    out = []
    for x in re.split(r"(?<=[.!?])\s+", text):
        if out and re.search(r"(\bFigs?|\bSupp?l?|\bTables?|\bal|\be\.g|\bi\.e|\bvs|\bcf|\bFig|\bNo)\.$", out[-1]):
            out[-1] += " " + x
        else:
            out.append(x)
    return out


_COORD = re.compile(r"[-−–]?\d+(?:\.\d+)?\s*[,;/ ]\s*[-−–]?\d+(?:\.\d+)?\s*[,;/ ]\s*[-−–]?\d+")


def prose_case(seed, target, rng, regions):
    region = rng.choice(regions)
    sp._PASSAGE_SPACE[0] = rng.choice(["MNI", "MNI", "TAL", None])
    sent, name, xyz, truth = prose_sentence(target, rng, region)
    real = seed.get("text") or ""
    append = rng.random() < 0.5
    if append:  # the real passage and its real result sets stay; the new set joins them
        text = f"{real} {sent}" if rng.random() < 0.6 else f"{sent} {real}"
        # Only position, statistic and cluster: a dataset row's points also carry its role fields.
        sets = [{"name": s.get("name"), "points": [{"xyz": xyz_of(p), "stat": p.get("stat") if isinstance(p, dict) else None,
                                                    "cluster": p.get("cluster") if isinstance(p, dict) else None} for p in s["points"]]}
                for s in seed["sets"]] + [{"name": name, "points": [{"xyz": xyz}]}]
        truths = [("result", None, False)] * len(seed["sets"]) + [truth]
    else:  # keep the real passage's sentences without coordinates around the new one
        plain = [x for x in _split(real) if not _COORD.search(x)][:2]
        text = " ".join(plain[:1] + [sent] + plain[1:])
        sets = [{"name": name, "points": [{"xyz": xyz}]}]
        truths = [truth]
    unit = {"origin": "text", "article_id": f"synth:{seed['article_id']}", "heading": seed.get("heading"),
            "text": text, "before": seed.get("before"), "after": seed.get("after"), "sets": sets,
            "title": seed.get("title"), "abstract": seed.get("abstract")}
    return unit, truths, len(sets) - 1 if append else 0, sent


def generate(pool, plan, rng, out_dir, max_per_article=3, id_prefix="s4", seed=41):
    """Write units.jsonl and truth.jsonl for `plan` into out_dir; returns the count per origin."""
    regions = sorted({" ".join(m.group(1).split()).lower() for u in pool["text"] for m in sp.REGION_RX.finditer(u.get("text") or "")
                      if 2 <= len(m.group(1).split()) <= 5}) or ["left insula"]
    out_dir.mkdir(parents=True, exist_ok=True)
    count, used, per_art = collections.Counter(), set(), collections.Counter()

    def draw(origin, target):
        for _ in range(200):
            chosen = pick_seed(pool[origin], rng, MODALITY.get(target))
            if chosen["unit_id"] not in used and per_art[chosen["article_id"]] < max_per_article:
                used.add(chosen["unit_id"])
                per_art[chosen["article_id"]] += 1
                return chosen
        raise SystemExit(f"seed pool exhausted for {origin}:{target}")

    with (out_dir / "units.jsonl").open("w") as uf, (out_dir / "truth.jsonl").open("w") as tf:
        for origin, targets in plan.items():
            jobs = [(t, t.startswith("hard")) for t, n in targets.items() for _ in range(n)]
            rng.shuffle(jobs)
            for target, hard in jobs:
                uid = f"{id_prefix}:{origin}:{seed}:{count[origin]}"
                count[origin] += 1
                chosen = draw(origin, target)
                if origin == "table":
                    unit, truths = table_case(chosen, "hard" if hard else target, rng)
                    focus = list(range(len(truths)))
                else:
                    unit, truths, f, sent = prose_case(chosen, target, rng, regions)
                    focus = [f] if f or len(truths) == 1 else [len(truths) - 1]
                unit = {"unit_id": uid, **unit, "dataset": "synthetic-v4", "labels_from": "dataset", "stratum": "synthetic"}
                # The builders the encoder and the labeller read the unit with must find every set.
                for ctx in labeling.contexts(unit):
                    assert ctx.points, (uid, "a set the context builder finds no points for")
                uf.write(json.dumps(unit, ensure_ascii=False) + "\n")
                for si, (role, kind, prior) in enumerate(truths):
                    bad = role_error({"role": role, "anchor_kind": kind, "from_prior_study": prior})
                    assert bad is None, (uid, bad)
                    tf.write(json.dumps({"unit_id": uid, "set_id": f"{uid}#{si}", "role": role, "anchor_kind": kind,
                                         "from_prior_study": prior, "target": target, "hard_negative": hard,
                                         "synthetic_set": si in focus, "seed_unit": chosen["unit_id"],
                                         **({"new_sentence": sent} if origin == "text" and si in focus else {})}) + "\n")
    return dict(count)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("out_dir", type=Path)
    ap.add_argument("--plan", type=Path, required=True,
                    help='JSON {"table": {"reference": n, "localization": n, ..., "hard_result": n}, "text": {...}}')
    ap.add_argument("--units", type=Path, nargs="+", required=True, help="real labelling units (build_sets.py output)")
    ap.add_argument("--labels", type=Path, nargs="+", required=True, help="labelling job directories (label_sets.py output)")
    ap.add_argument("--seed", type=int, default=41)
    ap.add_argument("--max-per-article", type=int, default=3)
    ap.add_argument("--id-prefix", default="s4")
    a = ap.parse_args()
    pool = seeds(load_units(a.units), labeling.latest_labels(a.labels))
    print(generate(pool, json.loads(a.plan.read_text()), random.Random(a.seed), a.out_dir, a.max_per_article, a.id_prefix, a.seed))


if __name__ == "__main__":
    main()
