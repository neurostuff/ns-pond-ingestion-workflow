"""Synthetic labelling units for the sparse set roles, seeded from real units.

    python experiments/role_classifier/synth/role_units.py OUT_DIR --plan plan.json \
        --units table_units.jsonl prose_units.jsonl --labels LABEL_DIR [LABEL_DIR ...] \
        --id-map slug_dbids.json [--seed 41]

Extends `synth_prose.py` (the nu-prose generator, which overweights hard cases) to
study_schema's v4 roles and to tables. Each case starts from a REAL unit whose sets
the labeller called `result`, from an article `export.splits` puts in train (never a
val, test, hand or gold article), and is rewritten into one target role with that
role's cues (cue_analysis.py):

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

Set names, captions and footers are not written for the target (`Names`). Each class's names
take the shapes (citation, "<region> seed", "P1", contrast, ...) of the real labelled names
of that class at the real rates; a name is one of the class's own real names at the share
they hold, else a real name of that shape from any class that carries no other class's cue
word ("prior study" stays with reference). A caption or footer
is the caption or footer of a real labelled set of the class, else a real one of
any class that names no cue (`family`). So a name, caption or footer tells a classifier
what a real one does (`namecheck.py`). A table
has as many rows as its seed, and every region's hemisphere agrees with the sign of its x.

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

sys.path.insert(0, str(Path(__file__).resolve().parent))  # synth_prose: a sibling script

import leakcheck  # noqa: E402
import synth_prose as sp  # noqa: E402
from ingestion_workflow.services.set_roles import export, labeling  # noqa: E402
from ingestion_workflow.services.set_roles.labels import role_error  # noqa: E402


def set_index(set_id):
    return int(set_id.rsplit("#", 1)[1])


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

def train_units(units, labels, id_map=None):
    """The real units the export trains on: a seed or a name from val or test would leak into training."""
    split = export.splits(list(units.values()), labels, id_map)
    return {uid: u for uid, u in units.items()
            if split[uid] == "train" and not labeling.held_out(u) and not synthetic(u) and u.get("dataset") != "wild"}


def seeds(units, labels, id_map=None):
    """Real train units every one of whose labelled sets is `result`, by origin."""
    roles = collections.defaultdict(dict)
    for sid, lab in labels.items():
        roles[lab["unit_id"]][set_index(sid)] = lab["role"]
    train = train_units(units, labels, id_map)
    out = {"table": [], "text": []}
    for uid, rs in roles.items():
        u = train.get(uid)
        if not u:
            continue
        if len(rs) == len(u["sets"]) and set(rs.values()) == {"result"}:
            if u["origin"] == "table" and not u.get("table_serialised"):
                continue
            out[u["origin"]].append(u)
    return out


def pick_seed(pool, rng, want=None):
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


# -- set names ---------------------------------------------------------------------

#: What a name looks like, first match wins. Only the shape may follow the class; the
#: rest of a name is drawn from every class's real names of that shape.
SHAPES = (
    ("citation", re.compile(r"\bet al\b|\(\d{4}[a-z]?\)|,\s*\d{4}[a-z]?\b|\[\d+(?:\s*[,\u2013-]\s*\d+)*\]")),
    ("figure", re.compile(r"^fig(?:ure|\.)?\b|\bfigure display\b", re.I)),
    ("seed", re.compile(r"\bseeds?\b", re.I)),
    ("target", re.compile(r"\btargets?\b|\bsites?\b|\bstimulat|\bvertex\b", re.I)),
    ("roi", re.compile(r"\bROIs?\b|\bregions? of interest\b|\bvoxels?\b|\bmasks?\b|\bspheres?\b", re.I)),
    ("network", re.compile(r"\bnetworks?\b|\bnodes?\b|\bcomponents?\b|^(?:DMN|DAN|VAN|FPN|CEN|SN|SAL)\b", re.I)),
    ("numbered", re.compile(r"^[A-Za-z]{1,12}[\s_-]?\d{1,3}$")),
    ("contrast", re.compile(r">|<|\bvs\.?(?!\w)|\bversus\b|\bminus\b|\bmain effect\b|\binteraction\b|\bcorrelat", re.I)),
)
#: Pseudo-counts of the origin's class-balanced mix (`Names.base`) behind each class's own: real
#: text localizations are 5 sets, and a class with none (table `other`) is named like the average class.
ALPHA = 2
#: A cue word is in at least CUE_MIN of one class's real names, with CUE_SHARE of its class-balanced rate.
CUE_MIN, CUE_SHARE = 2, 0.6
_CUE_TAIL = re.compile(r"\b(?:seeds?|ROIs?|targets?|sites?|voxels?|spheres?|masks?)\b.*$", re.I)


def shape(name):
    name = (name or "").strip()
    return next((k for k, rx in SHAPES if rx.search(name)), "other") if name else "unnamed"


def role_class(role, anchor_kind):
    return f"anchor:{anchor_kind}" if role == "anchor" else role


def _with_region(name, region):
    """'PCC seed' -> '<region> seed': keep the shape's word, take the region from the unit."""
    m = _CUE_TAIL.search(name)
    return f"{region} {m.group(0)}" if m else name


#: The most one real table's caption or footer may supply of a class's synthetic tables, so
#: no string is repeated often enough to be learned as one. Past it a draw takes the class's
#: least-used real table, so a class of a few tables draws them evenly: falling back to the
#: cue-free pool instead lent sparse classes result-like captions, and a model of them then
#: missed real result captions (as smoothing captions did).
SOURCE_CAP = 0.05


class Names:
    """Set names, captions and footers drawn the way the real labelled sets of each origin and
    class have them.

    `units` are train units only (`train_units`): nothing comes from an evaluation article.
    """

    def __init__(self, units, labels):
        self.pool = collections.defaultdict(lambda: collections.defaultdict(list))  # origin -> shape -> [(name, class)]
        self.shapes = collections.defaultdict(collections.Counter)  # (origin, class) -> shape counts
        self.texts = collections.defaultdict(list)  # (field, class) -> (text, source unit) per real set; class None: cue-free
        self.drawn = collections.defaultdict(collections.Counter)  # (field, class) -> source unit -> times drawn
        tables = {}  # unit id -> whether its caption starts "Table 2."
        self.left_out = collections.Counter()  # real names not drawn from, by reason
        words = collections.defaultdict(lambda: collections.defaultdict(collections.Counter))
        for sid, lab in labels.items():
            u, i = units.get(lab["unit_id"]), set_index(sid)
            if not u or i >= len(u["sets"]) or not lab.get("role"):
                continue
            cls = role_class(lab["role"], lab.get("anchor_kind"))
            if u["origin"] == "table":
                # Once per labelled set: the role model reads a caption once per set, so a caption's
                # share of a class's draws is its share of the class's real sets.
                for field in ("caption", "footer"):
                    raw = " ".join((u.get(field) or "").split())
                    text = _TABLE_LABEL.sub("", raw)
                    if leakcheck.label_syntax(text):
                        self.left_out[f"{field} reads as a label"] += 1
                        continue
                    self.texts[(field, cls)].append((text, u["unit_id"]))
                    if family(text) in ("generic", "empty"):
                        self.texts[(field, None)].append((text, u["unit_id"]))
                    if field == "caption" and text:
                        tables[u["unit_id"]] = bool(_TABLE_LABEL.match(raw))
            name = " ".join((u["sets"][i].get("name") or "").split())
            if leakcheck.label_name(name) or leakcheck.label_syntax(name):
                self.left_out["reads as a label"] += 1  # a real set named "Reference", say
                continue
            k = shape(name)
            self.pool[u["origin"]][k].append((name, cls))
            self.shapes[(u["origin"], cls)][k] += 1
            for t in _words(name):
                words[u["origin"]][t][cls] += 1
        self.prefixed = sum(tables.values()) / len(tables) if tables else 0.5  # the share of real captions that start "Table 2."
        self.cues = {origin: _cues(w, {c: sum(n.values()) for (o, c), n in self.shapes.items() if o == origin})
                     for origin, w in words.items()}

    def base(self, origin):
        """The shape mix of an origin's classes, each class weighing the same: the majority `result`
        class would otherwise lend its shapes to every sparse class."""
        return _balanced([n for (o, _), n in self.shapes.items() if o == origin])

    def mix(self, origin, cls):
        """{shape: weight} for one class: its real names' shapes plus ALPHA of the origin's (`base`)."""
        base = self.base(origin)
        if not base:
            raise SystemExit(f"no labelled {origin} set names to draw from")
        own = self.shapes[(origin, cls)]
        return {k: own[k] + ALPHA * base[k] for k in sorted(base)}

    def cue_classes(self, origin, name):
        """The classes whose cue words `name` carries."""
        cues = self.cues.get(origin, {})
        return {cues[t] for t in _words(name) if t in cues}

    def draw(self, origin, cls, rng, n=1, region=None, citations=()):
        """n names for the sets of one unit, in one shape (the sets of a table are named alike).

        A name is any class's real name of that shape that carries no other class's cue word, so
        "prior study" stays a reference name as it is in real sets, and no name is copied from
        the class's own few real names often enough to be learned as a string. A citation-shaped name is the
        unit's own citation when it has one, and a seed, target or ROI name takes the unit's
        region, so the name agrees with the text it sits in.
        """
        mix = self.mix(origin, cls)
        k = rng.choices(list(mix), weights=list(mix.values()))[0]
        if k == "citation":
            # Every class's citation names are written the same way, so their form tells nothing.
            return [citations[i % len(citations)] for i in range(n)] if citations else [study_label(rng) for _ in range(n)]
        every = self.pool[origin][k]
        own = [x for x, c in every if c == cls]
        if own and rng.random() < self.shapes[(origin, cls)][k] / mix[k]:
            pool = own
        else:
            pool = [x for x, c in every if not self.cue_classes(origin, x) - {cls}] or [x for x, _ in every]
        if k == "numbered":
            stem = re.match(r"^(.*?)\d+$", rng.choice(pool)).group(1)
            return [f"{stem}{i + 1}" for i in range(n)]
        names = rng.sample(pool, n) if len(pool) >= n else [rng.choice(pool) for _ in range(n)]
        if region and k in ("seed", "target", "roi"):
            names = [_with_region(x, region) for x in names]
        return [x or None for x in names]

    def text(self, field, cls, rng, label):
        """A real caption or footer for a table of one class, under the table's own `label`: the
        caption of one of the class's real labelled sets, so each caption is drawn for each class at
        the rate real sets of that class carry it (one real table's caption serves every class it
        holds), else (a class with no real tables, table `other`) any class's real one that names
        no cue family (`family`). Unlike names, captions are not smoothed: lending a sparse class
        result-like captions made a model of them miss real result captions."""
        rng = random.Random(rng.random())  # one draw of the caller's, so the cap leaves its later draws (names) as they were
        used = self.drawn[(field, cls)]
        room = max(1, SOURCE_CAP * (sum(used.values()) + 1))
        pool = self.texts[(field, cls)] or self.texts[(field, None)]
        open_ = [x for x in pool if used[x[1]] < room]
        if pool and not open_:
            least = min(used[x[1]] for x in pool)
            open_ = [x for x in pool if used[x[1]] == least]
        text, source = rng.choice(open_) if open_ else ("", None)
        used[source] += 1
        return f"{label}. {text}" if field == "caption" and text and rng.random() < self.prefixed else text

    def max_share(self):
        """{(field, class): the largest share of the class's draws one real source supplied}."""
        return {k: max(c.values()) / sum(c.values()) for k, c in self.drawn.items() if c}


def _words(name):
    return set(re.findall(r"[a-z]{3,}", (name or "").lower()))


def _balanced(counters):
    """The mean of the counters' shares, each counter weighing the same."""
    out = collections.Counter()
    counters = [c for c in counters if sum(c.values())]
    for c in counters:
        total = sum(c.values())
        for k, v in c.items():
            out[k] += v / total / len(counters)
    return dict(out)


def _cues(words, sizes):
    """{word: class} for the words real names tie to one class: in CUE_MIN or more of the class's
    names, and CUE_SHARE or more of the word's class-balanced rate (its rate within each class, so
    the many `result` names do not outvote a sparse class)."""
    out = {}
    for w, by in words.items():
        rate = {c: v / sizes[c] for c, v in by.items()}
        top = max(sorted(rate), key=rate.get)
        if by[top] >= CUE_MIN and rate[top] >= CUE_SHARE * sum(rate.values()):
            out[w] = top
    return out


#: Cue families of a table caption or footer, first match wins; "generic" when none matches and
#: "empty" for no text. Only generic and empty ones are lent to another class (`Names.text`).
FAMILIES = (
    ("statistics", re.compile(r"\bp\s*[<=≤]|\bfwe\b|\bfdr\b|uncorrected|corrected|threshold|\bclusters?\b|\bk\s*[=>]|signific|\b[tz][ -]?(?:values?|scores?)\b", re.I)),
    ("reference", re.compile(r"previous|prior|earlier|reported|literature|studies|meta-?analy|original publication|cited", re.I)),
    ("localization", re.compile(r"electrode|contact|channel|optode|sensor|dipole|implant|ieeg|seeg|ecog|fnirs|nirs", re.I)),
    ("stimulation", re.compile(r"stimulat|tms|tdcs|tacs|neuronavigation|\bcoil\b|\btargets?\b", re.I)),
    ("anchor", re.compile(r"\bseeds?\b|\brois?\b|regions? of interest|\bnodes?\b|parcel|network|sphere|voxel|\bmrs\b|spectroscopy", re.I)),
    ("other", re.compile(r"slice|lesion|simulat|atlas|display|crosshair|figure", re.I)),
)


def family(text):
    text = (text or "").strip()
    return next((k for k, rx in FAMILIES if rx.search(text)), "generic") if text else "empty"


_TABLE_LABEL = re.compile(r"^(?:Table|Tab\.)\s*[A-Z]?\d+[A-Za-z]?\s*[.:|-]?\s*", re.I)

#: The class a table target is labelled with.
TARGET_CLASS = {"reference": "reference", "localization": "localization", "anchor_roi_mrs": "anchor:roi", "seed": "anchor:seed",
                "node": "anchor:node", "other": "other", "stimulation_target": "anchor:stimulation_target", "hard": "result"}

#: A hemisphere word: left/right/lh/rh, or a capital L or R standing alone ("L IFG", "Precentral_L").
_HEMI_WORD = re.compile(r"\b(left|right|lh|rh)\b", re.I)
_HEMI_LETTER = re.compile(r"(?<![A-Za-z])([LR])(?![A-Za-z])")
MIDLINE = 4  # |x| in mm a lateral region keeps, so a 2-mm redraw cannot move it across the midline


def hemisphere(text):
    """'left', 'right', or None when `text` names neither or both."""
    found = {("left" if w.lower() in ("left", "lh") else "right") for w in _HEMI_WORD.findall(text or "")}
    found |= {("left" if w == "L" else "right") for w in _HEMI_LETTER.findall(text or "")}
    return found.pop() if len(found) == 1 else None


def side(region, xyz):
    """xyz with x on the hemisphere `region` names; real papers put a left region at x < 0."""
    h = hemisphere(region)
    if h is None:
        return list(xyz)
    x = max(abs(xyz[0]), MIDLINE)
    return [-x if h == "left" else x, *xyz[1:]]


def sizes(total, n):
    """`total` rows split over n sets, at least 2 each."""
    return [max(2, total // n + (i < total % n)) for i in range(n)]


class Positions:
    """The triples already in one unit: the context builders find a set's rows and sentence by its
    triples, so a triple two sets share gives each the other's rows."""

    def __init__(self, taken=()):
        self.taken = {tuple(t) for t in taken}

    def fresh(self, xyz, rng):
        xyz = list(xyz)
        while tuple(xyz) in self.taken:
            xyz = jitter(xyz, rng, 2)
        self.taken.add(tuple(xyz))
        return xyz


# -- tables ------------------------------------------------------------------------

def _table(header, rows):
    # A body cell that starts with `#` reads as a header cell (table_context.split_table); a row-spanning
    # label is `^n:label` and a continuation `~`, as in the serialisation nspond_tables writes.
    return "\n".join([" | ".join(header)] + [" | ".join(str(c) for c in r) for r in rows])


def table_case(seed, target, rng, names):
    """(unit fields, truth) for one table rewritten into `target`.

    The table has as many coordinate rows as its seed (a real table), split over its sets, and
    every row's x lies on the hemisphere its region names.
    """
    space, space_hdr = rng.choice(SPACE_HDR)
    paired = real_rows(seed) or [(r, sp.realistic_xyz(rng)) for r in FALLBACK_REGIONS]
    regions = [r for r, _ in paired]
    pos = Positions()
    tl = seed.get("table_label") or f"Table {rng.randint(1, 5)}"
    sw = SPACE_WORD[space]
    total = len(paired)

    def draw_rows(n, mm=6):
        src = [rng.choice(paired) for _ in range(n)]
        return src, [pos.fresh(side(reg, jitter(p, rng, mm)), rng) for reg, p in src]

    if target == "reference":
        studies = [study_label(rng) for _ in range(rng.randint(2, 4))]
        layout = rng.choice(["study_col", "ref_col", "grouped"])
        rows, sets = [], []
        for st, name, k in zip(studies, names.draw("table", "reference", rng, len(studies), citations=studies),
                               sizes(total, len(studies))):
            src, spts = draw_rows(k, 8)
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
        citing = [rng.choice([
            f"{tl} lists the coordinates reported by previous studies ({studies[0].replace(" (", ", ").rstrip(")")}) for comparison.",
            f"Our peaks were within 8 mm of those reported previously ({tl}).",
            f"The location of this effect agrees with earlier reports ({studies[-1].replace(" (", ", ").rstrip(")")}; see {tl})."])]
        truth = [("reference", None, True)] * len(sets)
    elif target == "localization":
        kind = rng.choice(["contacts", "contacts", "optodes"])
        labels = names.draw("table", "localization", rng, rng.randint(1, 3))
        rows, sets = [], []
        for label, k in zip(labels, sizes(total, len(labels))):
            src, spts = draw_rows(k)
            sets.append({"name": label, "points": [[*p, None, None, None] for p in spts]})
            for ci, p in enumerate(spts):
                el = f"{rng.choice('LR')}{rng.choice('ABCHT')}{rng.randint(1, 12)}" if kind == "contacts" else f"S{rng.randint(1, 16)}-D{rng.randint(1, 16)}"
                rows.append([f"^{len(spts)}:{label or ''}" if ci == 0 else "~", el, *p, src[ci][0]])
        header = [f"#{rng.choice(['Patient', 'Subject', 'Case'])}", "#" + {"contacts": "Contact", "optodes": "Channel"}[kind],
                  f"#<3:{space_hdr}", f"#{rng.choice(['Anatomical label', 'AAL label', 'Region (Harvard-Oxford)', 'Location'])}"]
        citing = [f"The location of each {'contact' if kind == 'contacts' else 'channel'} is given in {tl}."]
        truth = [("localization", None, False)] * len(sets)
    elif target == "anchor_roi_mrs":
        labels = names.draw("table", "anchor:roi", rng, rng.randint(1, 3))
        rows, sets = [], []
        for label, k in zip(labels, sizes(total, len(labels))):
            src, spts = draw_rows(k)
            sets.append({"name": label, "points": [[*p, None, None, None] for p in spts]})
            for ci, p in enumerate(spts):
                rows.append([f"^{len(spts)}:{label or ''}" if ci == 0 else "~", src[ci][0], *p, rng.choice(["20 x 20 x 20", "30 x 30 x 30", "25 x 25 x 25"])])
        header = [f"#{rng.choice(['Voxel', 'Placement'])}", "#Region", f"#<3:{space_hdr}", "#Voxel size (mm)"]
        citing = [f"The location of each MRS voxel is given in {tl}."]
        truth = [("anchor", "roi", False)] * len(sets)
    elif target in ("seed", "node"):
        labels = names.draw("table", f"anchor:{target}", rng, rng.randint(1, 3))
        rows, sets = [], []
        for label, k in zip(labels, sizes(total, len(labels))):
            src, spts = draw_rows(k)
            sets.append({"name": label, "points": [[*p, None, None, None] for p in spts]})
            for ci, p in enumerate(spts):
                rows.append([f"^{len(spts)}:{label or ''}" if ci == 0 else "~", src[ci][0], *p, rng.choice(["8", "6", "10"]) if target == "seed" else rng.choice(["DMN", "FPN", "Salience", "Dorsal attention"])])
        header = ["#Seed" if target == "seed" else "#Network", "#Region", f"#<3:{space_hdr}", "#Radius (mm)" if target == "seed" else "#Module"]
        citing = [f"{tl} lists the {'seeds' if target == 'seed' else 'nodes'} of the {'connectivity' if target == 'seed' else 'network'} analysis."]
        truth = [("anchor", target, False)] * len(sets)
    elif target == "other":
        kind = rng.choice(["slices", "simulated", "lesion", "example"])
        labels = names.draw("table", "other", rng, rng.randint(1, 3))
        rows, sets = [], []
        for label, k in zip(labels, sizes(total, len(labels))):
            src, spts = draw_rows(k)
            sets.append({"name": label, "points": [[*p, None, None, None] for p in spts]})
            for ci, p in enumerate(spts):
                rows.append([f"^{len(spts)}:{label or ''}" if ci == 0 else "~", src[ci][0], *p])
        header = ["#" + {"slices": "Panel", "simulated": "Simulated source", "lesion": "Patient", "example": "Voxel"}[kind], "#Region", f"#<3:{space_hdr}"]
        citing = [{"slices": f"{tl} gives the slice positions used for display.", "simulated": f"The simulated source positions are listed in {tl}.",
                   "lesion": f"See {tl} for each patient's lesion centre.", "example": f"{tl} gives worked examples of the atlas lookup."}[kind]]
        truth = [("other", None, False)] * len(sets)
    elif target == "stimulation_target":
        labels = names.draw("table", "anchor:stimulation_target", rng, rng.randint(1, 2))
        rows, sets = [], []
        for label, k in zip(labels, sizes(total, len(labels))):
            src, spts = draw_rows(k)
            sets.append({"name": label, "points": [[*p, None, None, None] for p in spts]})
            for ci, p in enumerate(spts):
                rows.append([f"^{len(spts)}:{label or ''}" if ci == 0 else "~", src[ci][0], *p, rng.choice(["Neuronavigation", "fMRI-guided", "Beam F3", "Individual peak"])])
        header = ["#Site", "#Region", f"#<3:{space_hdr}", "#Targeting method"]
        citing = [f"The stimulation targets are listed in {tl}."]
        truth = [("anchor", "stimulation_target", False)] * len(sets)
    else:  # hard negative: a result table whose citing sentence carries a prior-study cue
        rows, sets = [], []
        for s in seed["sets"][:3]:
            # The seed's own names and points: real result names, so they follow the result class's.
            pts = [xyz_of(p) for p in s["points"][:5]]
            reg = [rng.choice([r for r in regions if hemisphere(r) in (None, "left" if p[0] < 0 else "right")] or regions) for p in pts]
            spts = [pos.fresh(side(r, p), rng) for r, p in zip(reg, pts)]
            stat = [p[4] if not isinstance(p, dict) and len(p) > 4 else round(rng.uniform(3.2, 7.5), 2) for p in s["points"][:5]]
            sets.append({"name": s.get("name"), "points": [[*p, "T", st, None] for p, st in zip(spts, stat)]})
            for ci, (r, p, st) in enumerate(zip(reg, spts, stat)):
                rows.append([f"^{len(spts)}:{s.get('name') or ''}" if ci == 0 else "~", r, *p, st])
        header = ["#Contrast", "#Region", f"#<3:{space_hdr}", "#t"]
        citing = [f"{tl} lists the peaks of this effect, consistent with {cite(rng)}."]
        truth = [("result", None, False)] * len(sets)
    cls = TARGET_CLASS[target if target in TARGET_CLASS else "hard"]
    caption, footer = names.text("caption", cls, rng, tl), names.text("footer", cls, rng, tl)
    sub = ["~"] * (header.index(next(h for h in header if h.startswith("#<3:")))) + ["#x", "#y", "#z"]
    serial = _table(header, [sub + ["~"] * (len(rows[0]) - len(sub))] + rows) if rows else ""
    unit = {"origin": "table", "article_id": f"synth:{seed['article_id']}", "source": seed.get("source"),
            "table_id": f"synth-{rng.randint(0, 10**9)}", "table_label": tl, "title": seed.get("title"),
            "abstract": seed.get("abstract"), "caption": caption,
            "footer": footer, "table_serialised": serial, "citing": citing, "sets": sets}
    return unit, truth


# -- prose -------------------------------------------------------------------------

def prose_sentence(target, rng, region, pos):
    """(sentence, xyz, truth, the sentence's author-year citation or None) of one prose set.

    `pos` holds the triples the passage already has. x lies on the hemisphere `region` names.
    """
    xyz = pos.fresh(side(region, sp.realistic_xyz(rng)), rng)
    c = sp.coord_text(xyz, rng)
    w = sp.wrap(c)
    ct = cite(rng)

    def own(t):
        return narr(ct) if not ct.startswith("[") and narr(ct) in t else None
    if target == "reference":
        t = rng.choice([
            f"This peak lies close to the {region} coordinate reported by {narr(ct)} {w}.",
            f"{narr(ct)} reported a similar effect in the {region} at {c}.",
            f"In the meta-analysis by {narr(ct)}, the {region} cluster peaked at {c}.",
            f"Previous work located the {region} at {c} {ct}, about {rng.randint(4, 12)} mm from our peak.",
            f"The {region} coordinate of an earlier study {cite(rng, numbered=True)} {w} falls within our cluster.",
            f"For comparison, the canonical {region} location {w} was taken from {narr(ct)}.",
        ])
        return t, xyz, ("reference", None, True), own(t)
    if target == "localization":
        t = rng.choice([
            f"Electrode {rng.choice('LR')}{rng.choice('ABHT')}{rng.randint(1, 10)} was located in the {region} at {c}.",
            f"In a typical recording site in the {region} {w}, responses emerged at about {rng.randint(120, 300)} ms.",
            f"Dipoles were placed at {c} in the {region}, following the source model.",
            f"Depth-electrode contacts in the {region} had a mean position of {c}.",
            f"The fNIRS optode over the {region} projected to {c}.",
            f"Subdural contacts over the {region} were localized on the post-implant CT at {c}.",
        ])
        return t, xyz, ("localization", None, False), own(t)
    if target == "anchor_roi_mrs":
        t = rng.choice([
            f"The MRS voxel was centred on the {region} (mean position {c}).",
            f"A {rng.choice([20, 25, 30])} mm isotropic spectroscopy voxel was placed in the {region} {w}.",
            f"1H-MRS spectra were acquired from a voxel in the {region} (centre {c}).",
        ])
        return t, xyz, ("anchor", "roi", False), own(t)
    if target == "other":
        t = rng.choice([
            f"Slices are shown at {c}.",
            f"Crosshairs are placed at {c} in the {region}.",
            f"The region of maximal lesion overlap {w} was in the {region}.",
            f"Simulated sources were placed at {c} in the {region}.",
            f"For example, the voxel at {c} is assigned to the {region} with {rng.randint(40, 90)}% probability.",
            f"Image coordinates {w} are in {rng.choice(['MNI', 'Talairach'])} space.",
        ])
        return t, xyz, ("other", None, False), own(t)
    if target == "hard_anchor_prior":
        t = rng.choice([
            f"A {rng.choice([6, 8, 10])}-mm sphere was centred on the {region} coordinate reported by {narr(ct)} {w} and used as the ROI.",
            f"The seed was the {region} peak from a previous study {cite(rng, numbered=True)} {w}.",
        ])
        return t, xyz, ("anchor", "seed" if t.startswith("The seed") else "roi", True), own(t)
    # hard negative result: carries a reference / other / localization cue
    k, v, s = sp.stat_text(rng)
    t = rng.choice([
        f"Consistent with {narr(ct)}, the contrast activated the {region} {sp.wrap(c + '; ' + s)}.",
        f"Figure {rng.randint(1, 5)} shows the {region} cluster ({c}; {s}) on axial slices.",
        f"Electrode-level effects were strongest over the {region} source ({c}; {s}).",
        f"As previously reported {cite(rng, numbered=True)}, activity in the {region} {w} increased with load ({s}).",
    ])
    return t, xyz, ("result", None, False), own(t)


def _split(text):
    out = []
    for x in re.split(r"(?<=[.!?])\s+", text):
        if out and re.search(r"(\bFigs?|\bSupp?l?|\bTables?|\bal|\be\.g|\bi\.e|\bvs|\bcf|\bFig|\bNo)\.$", out[-1]):
            out[-1] += " " + x
        else:
            out.append(x)
    return out


_COORD = re.compile(r"[-−–]?\d+(?:\.\d+)?\s*[,;/ ]\s*[-−–]?\d+(?:\.\d+)?\s*[,;/ ]\s*[-−–]?\d+")


def prose_case(seed, target, rng, regions, names):
    region = rng.choice(regions)
    sp._PASSAGE_SPACE[0] = rng.choice(["MNI", "MNI", "TAL", None])
    append = rng.random() < 0.5
    pos = Positions(xyz_of(p) for s in seed["sets"] for p in s["points"]) if append else Positions()
    sent, xyz, truth, cited = prose_sentence(target, rng, region, pos)
    name = names.draw("text", role_class(truth[0], truth[1]), rng, region=region, citations=[cited] if cited else ())[0]
    real = seed.get("text") or ""
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


def faults(unit):
    """Why the context builders would misread a unit: a set with no points, a table set whose rows
    are not its points, or a triple two sets share."""
    out, owner = [], {}
    for i, s in enumerate(unit["sets"]):
        for p in s["points"]:
            t = tuple(xyz_of(p))
            if owner.setdefault(t, i) != i:
                out.append(f"sets {owner[t]} and {i} share {list(t)}")
    for i, ctx in enumerate(labeling.contexts(unit)):
        if not ctx.points:
            out.append(f"set {i}: no points")
        elif unit["origin"] == "table" and len(ctx.rows) != len(ctx.points):
            out.append(f"set {i}: {len(ctx.rows)} rows for {len(ctx.points)} points")
    return out


def generate(pool, plan, rng, out_dir, names, max_per_article=3, id_prefix="s4", seed=41, tries=20):
    """Write units.jsonl and truth.jsonl for `plan` into out_dir; returns the count per origin.

    A case the context builders would misread is redrawn from another seed, and each one
    redrawn is recorded with its reason in rejected.jsonl.
    """
    regions = sorted({" ".join(m.group(1).split()).lower() for u in pool["text"] for m in sp.REGION_RX.finditer(u.get("text") or "")
                      if 2 <= len(m.group(1).split()) <= 5}) or ["left insula"]
    rich = {"table": [u for u in pool["table"] if len(real_rows(u)) >= 3] or pool["table"], "text": pool["text"]}
    out_dir.mkdir(parents=True, exist_ok=True)
    count, used, per_art = collections.Counter(), set(), collections.Counter()

    def draw(origin, target):
        free = [u for u in rich[origin] if u["unit_id"] not in used and per_art[u["article_id"]] < max_per_article]
        if not free:
            raise SystemExit(f"seed pool exhausted for {origin}:{target}")
        chosen = pick_seed(free, rng, MODALITY.get(target))
        used.add(chosen["unit_id"])
        per_art[chosen["article_id"]] += 1
        return chosen

    with (out_dir / "units.jsonl").open("w") as uf, (out_dir / "truth.jsonl").open("w") as tf, \
            (out_dir / "rejected.jsonl").open("w") as rf:
        for origin, targets in plan.items():
            jobs = [(t, t.startswith("hard")) for t, n in targets.items() for _ in range(n)]
            rng.shuffle(jobs)
            for target, hard in jobs:
                uid = f"{id_prefix}:{origin}:{seed}:{count[origin]}"
                count[origin] += 1
                for _ in range(tries):
                    chosen = draw(origin, target)
                    if origin == "table":
                        unit, truths = table_case(chosen, "hard" if hard else target, rng, names)
                        focus = list(range(len(truths)))
                    else:
                        unit, truths, f, sent = prose_case(chosen, target, rng, regions, names)
                        focus = [f] if f or len(truths) == 1 else [len(truths) - 1]
                    unit = {"unit_id": uid, **unit, "dataset": "synthetic-v4", "labels_from": "dataset", "stratum": "synthetic"}
                    bad = faults(unit)
                    if not bad:
                        break
                    rf.write(json.dumps({"unit_id": uid, "target": target, "seed_unit": chosen["unit_id"], "faults": bad}) + "\n")
                else:
                    raise SystemExit(f"{uid}: no seed gave a unit the context builders read right in {tries} tries")
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
    ap.add_argument("--id-map", type=Path, required=True,
                    help="slug id -> database id (units/slug_dbids.json): export.splits splits by database id, so "
                         "without it a seed can come from an article the export puts in val or test")
    ap.add_argument("--seed", type=int, default=41)
    ap.add_argument("--max-per-article", type=int, default=3)
    ap.add_argument("--id-prefix", default="s4")
    a = ap.parse_args()
    units, labels = load_units(a.units), labeling.latest_labels(a.labels)
    id_map = json.loads(a.id_map.read_text())
    pool, names = seeds(units, labels, id_map), Names(train_units(units, labels, id_map), labels)
    print(generate(pool, json.loads(a.plan.read_text()), random.Random(a.seed), a.out_dir, names, a.max_per_article,
                   a.id_prefix, a.seed))
    for (field, cls), share in sorted(names.max_share().items(), key=str):
        print(f"max source share {field} {cls}: {share:.3f}")


if __name__ == "__main__":
    main()
