"""The synthetic unit generator (experiments/role_classifier/synth) builds units the repo's own
context builders read, labels them by construction, takes its seeds and names from train
articles only, names sets the way real sets are named, and leaks no label into what it renders."""

import itertools
import json
import random
import sys
import zlib
from pathlib import Path

import pytest

SYNTH = Path(__file__).resolve().parents[3] / "experiments" / "role_classifier" / "synth"
sys.path.insert(0, str(SYNTH))

import leakcheck  # noqa: E402
import namecheck  # noqa: E402
import rewrite  # noqa: E402
import role_units  # noqa: E402
import synth_prose  # noqa: E402
from ingestion_workflow.services import prose_passages  # noqa: E402
from ingestion_workflow.services.set_roles import export, labeling, prose_context, table_context  # noqa: E402
from ingestion_workflow.services.set_roles.labels import ANCHOR_KINDS, COORDINATE_ROLES  # noqa: E402


def _table_text(tag=0):
    # Tables with the same text are one article to export.splits, so each unit's t values differ.
    return "\n".join(
        ["#Region | #<3:MNI coordinates | #t", "~ | #x | #y | #z | ~"]
        + [
            f"Left inferior frontal gyrus | {-40 + i} | {20 + i} | {10 - i} | 4.{tag}{i}"
            for i in range(4)
        ]
        + [f"Right anterior insula | {38 + i} | {18 - i} | {2 + i} | 5.{tag}{i}" for i in range(4)]
    )


TABLE = _table_text()
PASSAGE = (
    "Participants viewed faces. The contrast activated the left insula (-34, 18, 2; z = 4.5). "
    "Activity in the right amygdala peaked at 22, -4, -16 (z = 4.1). No other region survived."
)
#: Real-looking names per class: a few carry the class's cue, as real names do; most are names
#: every class has.
GENERIC = [
    "A > B",
    "faces > houses",
    "Controls",
    "Encoding",
    "Left hemisphere",
    "main effect of load",
    "Visual",
]
REAL_NAMES = {
    "result": GENERIC + ["fALFF", "Cluster 1", "Smith et al. (2010)", "PCC seed", "vmPFC ROI"],
    "reference": GENERIC + ["Smith et al. (2010)", "Chen and Kim, 2015", "Rossi et al. [12]"],
    "localization": GENERIC + ["P1", "P2", "Dipole coordinates (mm)"],
    "anchor:roi": GENERIC + ["vmPFC ROI", "dmPFC ROI", "Muhlau et al. (2006)"],
    "anchor:seed": GENERIC + ["PCC seed", "amygdala seed", "Default-Mode Network"],
    "anchor:stimulation_target": GENERIC + ["left DLPFC target", "vertex control site", "ADHD-1"],
    "anchor:node": GENERIC + ["Default mode network", "Salience network"],
    "other": GENERIC + ["figure display"],
}
TABLE_PLAN = {
    "reference": 2,
    "localization": 2,
    "anchor_roi_mrs": 1,
    "other": 2,
    "stimulation_target": 1,
    "seed": 1,
    "node": 1,
    "hard_result": 2,
}
TEXT_PLAN = {
    "reference": 2,
    "localization": 2,
    "anchor_roi_mrs": 1,
    "other": 2,
    "hard_result": 2,
    "hard_anchor_prior": 3,
}
RNG_SEEDS = (0, 1, 2, 3, 4, 5)


def _flat(text):
    return " ".join(text.split())


def _articles(split, n, prefix):
    """n article ids export.split_of puts in `split`."""
    ids = (f"{prefix}{i}" for i in itertools.count())
    return list(itertools.islice((a for a in ids if export.split_of(a) == split), n))


def _table(uid, article, names):
    return {
        "unit_id": uid,
        "origin": "table",
        "article_id": article,
        "source": "pubget",
        "title": f"Study of MRS and EEG {uid}",
        "abstract": "Abstract.",
        "table_label": "Table 1",
        "caption": "Table 1. Peaks.",
        "footer": "",
        "citing": [],
        "table_serialised": _table_text(zlib.crc32(uid.encode())),
        "sets": [{"name": n, "points": [[-40, 20, 10, "T", 4.0, None]]} for n in names],
    }


def _prose(uid, article, names):
    return {
        "unit_id": uid,
        "origin": "text",
        "article_id": article,
        "title": "Study",
        "abstract": "Abstract.",
        "heading": "Results",
        "text": PASSAGE,
        "before": "",
        "after": "",
        "sets": [
            {
                "name": n,
                "points": [
                    {
                        "xyz": [-34 + i, 18, 2],
                        "stat": ["Z", 4.5],
                        "role": "result",
                        "analysis": "x",
                    }
                ],
            }
            for i, n in enumerate(names)
        ],
    }


def _label(labels, u, i, cls, model="gpt-6.1-sol"):
    role, _, kind = cls.partition(":")
    labels[f"{u['unit_id']}#{i}"] = {
        "unit_id": u["unit_id"],
        "role": role,
        "anchor_kind": kind or None,
        "from_prior_study": role == "reference",
        "model": model,
    }


def _real():
    """(units, labels): all-result seeds and named sets of every class."""
    units, labels = {}, {}

    def add(u, classes, model="gpt-6.1-sol"):
        units[u["unit_id"]] = u
        for i, cls in enumerate(classes):
            _label(labels, u, i, cls, model)

    for i, a in enumerate(_articles("train", 40, "a")):
        add(_table(f"t{i}", a, ["A > B"]), ["result"])
        add(_prose(f"p{i}", a, ["faces"]), ["result"])
    for i, a in enumerate(_articles("train", 20, "d")):
        # Two real analyses reporting the same peak: appended to, their sets share a triple.
        u = _prose(f"dup{i}", a, ["faces", "houses"])
        u["sets"][1]["points"][0]["xyz"] = u["sets"][0]["points"][0]["xyz"]
        add(u, ["result", "result"])
    named = iter(_articles("train", 800, "n"))
    for cls, names in REAL_NAMES.items():
        for j, name in enumerate(names * 3):
            make = _table if j % 2 else _prose
            add(make(f"n-{cls}-{j}", next(named), [name]), [cls])
    return units, labels


def _eval_units():
    """All-result units no seed may come from, by why."""
    val, test = _articles("val", 1, "e"), _articles("test", 1, "e")
    out = {
        "held-out prose": {**_prose("x-held", "x-held", ["faces"]), "dataset_split": "test"},
        "hand prose": {
            **_prose("x-hand", "x-hand", ["faces"]),
            "base_row": {"label_source": "hand"},
        },
        "wild": {**_table("x-wild", "x-wild", ["A > B"]), "dataset": "wild"},
        "synthetic": {
            **_table("x-synth", "x-synth", ["A > B"]),
            "dataset": "synthetic-v4",
            "labels_from": "dataset",
        },
        "val article": _table("x-val", val[0], ["A > B"]),
        "test article": _prose("x-test", test[0], ["faces"]),
        # an article export.split_of puts in train, made test by a gold label on its other unit
        "gold label in the article": _table("x-gold", _articles("train", 1, "g")[0], ["A > B"]),
    }
    return out


@pytest.fixture(scope="module")
def real():
    units, labels = _real()
    for u in _eval_units().values():
        units[u["unit_id"]] = u
        _label(labels, u, 0, "result")
    gold = _prose("x-gold-sib", units["x-gold"]["article_id"], ["faces"])
    units[gold["unit_id"]] = gold
    _label(labels, gold, 0, "result", model="gpt-6-astra")
    return units, labels


@pytest.fixture(scope="module")
def generated(tmp_path_factory, real):
    units, labels = real
    pool, names = (
        role_units.seeds(units, labels),
        role_units.Names(role_units.train_units(units, labels), labels),
    )
    out = []
    for s in RNG_SEEDS:
        d = tmp_path_factory.mktemp(f"synth{s}")
        counts = role_units.generate(
            pool,
            {"table": TABLE_PLAN, "text": TEXT_PLAN},
            random.Random(s),
            d,
            names,
            max_per_article=1,
            seed=s,
        )
        assert counts == {"table": sum(TABLE_PLAN.values()), "text": sum(TEXT_PLAN.values())}
        out.append(d)

    def rows(name):
        return [json.loads(line) for d in out for line in (d / name).read_text().splitlines()]

    return rows("units.jsonl"), rows("truth.jsonl"), rows("rejected.jsonl")


def test_units_are_read_by_the_v4_context_builders(generated):
    units, _, _ = generated
    for u in units:
        assert role_units.faults(u) == []
        for ctx in labeling.contexts(u):
            if u["origin"] == "text":
                assert (
                    isinstance(ctx, prose_context.ProseSetContext)
                    and ctx.local
                    and _flat(ctx.local) in _flat(ctx.passage)
                )
            else:
                assert isinstance(ctx, table_context.TableSetContext) and len(ctx.rows) == len(
                    ctx.points
                )
    assert prose_context.PROSE_CONTEXT_VERSION == 4


#: target -> (role, anchor_kind, from_prior_study) of the set the generator built.
EXPECTED = {
    ("table", "reference"): ("reference", None, True),
    ("table", "localization"): ("localization", None, False),
    ("table", "anchor_roi_mrs"): ("anchor", "roi", False),
    ("table", "other"): ("other", None, False),
    ("table", "stimulation_target"): ("anchor", "stimulation_target", False),
    ("table", "seed"): ("anchor", "seed", False),
    ("table", "node"): ("anchor", "node", False),
    ("table", "hard_result"): ("result", None, False),
    ("text", "reference"): ("reference", None, True),
    ("text", "localization"): ("localization", None, False),
    ("text", "anchor_roi_mrs"): ("anchor", "roi", False),
    ("text", "other"): ("other", None, False),
    ("text", "hard_result"): ("result", None, False),
}


def test_truth_by_target(generated):
    units, truth, _ = generated
    origin = {u["unit_id"]: u["origin"] for u in units}
    seen = set()
    for t in truth:
        got = (t["role"], t["anchor_kind"], t["from_prior_study"])
        assert t["role"] in COORDINATE_ROLES and (t["anchor_kind"] in ANCHOR_KINDS) == (
            t["role"] == "anchor"
        )
        if not t["synthetic_set"]:  # a real result set kept in an appended passage
            assert got == ("result", None, False)
            continue
        key = (origin[t["unit_id"]], t["target"])
        if t["target"] == "hard_anchor_prior":
            kind = "seed" if t["new_sentence"].startswith("The seed") else "roi"
            assert got == ("anchor", kind, True), t
            seen.add((*key, kind))
        else:
            assert got == EXPECTED[key], t
            seen.add(key)
    assert (
        set(EXPECTED)
        | {("text", "hard_anchor_prior", "seed"), ("text", "hard_anchor_prior", "roi")}
        == seen
    )


def test_no_label_leaks_into_units_or_rendered_inputs(generated):
    units, _, _ = generated
    assert leakcheck.check(units) == (len(units), {})
    footers = {u.get("footer") for u in units if u["origin"] == "table"}
    assert not any("truth" in (f or "").lower() for f in footers)


def _unit(units, origin):
    return json.loads(json.dumps(next(u for u in units if u["origin"] == origin)))


LEAKS = {
    "set named with its role": (
        "table",
        lambda u: u["sets"][0].update(name="reference (prior study)"),
    ),
    "set named role: kind": ("table", lambda u: u["sets"][0].update(name="anchor: roi")),
    "proposal hint in the text": (
        "text",
        lambda u: u.update(text=u["text"] + " [PROPOSED] role: localization"),
    ),
    "role = in the heading": ("text", lambda u: u.update(heading="role = other")),
    "unit-level role key": ("table", lambda u: u.update(role="reference")),
    "set-level role key": ("table", lambda u: u["sets"][0].update(role="reference")),
    "point-level role key": ("text", lambda u: u["sets"][0]["points"][0].update(role="reference")),
    "proposal hint in a caption": ("table", lambda u: u.update(caption="[PROPOSED] other")),
    "role field in a citing sentence": (
        "table",
        lambda u: u.update(citing=['{"anchor_kind": "seed"}']),
    ),
    "no labels_from": ("table", lambda u: u.pop("labels_from")),
}


@pytest.mark.parametrize("leak", sorted(LEAKS))
def test_leakcheck_catches(generated, leak):
    units, _, _ = generated
    origin, plant = LEAKS[leak]
    u = _unit(units, origin)
    assert leakcheck.leaks(u) == []
    plant(u)
    assert leakcheck.leaks(u)
    assert leakcheck.check([u])[1]


def test_leakcheck_reads_what_the_builders_render(generated, monkeypatch):
    units, _, _ = generated
    u = _unit(units, "table")
    render = labeling.render
    monkeypatch.setattr(
        leakcheck.labeling,
        "render",
        lambda unit: (lambda s, p, n: (s, p + "\n[PROPOSED] reference", n))(*render(unit)),
    )
    assert leakcheck.leaks(u) == ["rendered input"]


def test_cue_words_are_not_leaks(generated):
    units, _, _ = generated
    u = _unit(units, "table")
    u["footer"] = (
        "Ground-truth positions of the reference electrodes; localization accuracy was 4 mm."
    )
    u["sets"][0]["name"] = "Reference electrodes"
    assert leakcheck.leaks(u) == []


def test_seeds_come_from_train_articles_only(real, generated):
    units, labels = real
    pool = role_units.seeds(units, labels)
    allowed = {u["unit_id"] for origin in pool.values() for u in origin}
    banned = {u["unit_id"] for u in _eval_units().values()} | {"x-gold-sib"}
    assert banned <= set(units) and not allowed & banned
    assert {u["unit_id"] for u in role_units.train_units(units, labels).values()}.isdisjoint(
        banned
    )
    _, truth, _ = generated
    assert {t["seed_unit"] for t in truth} <= allowed


def test_table_sets_never_share_a_triple(real):
    units, _ = real
    names = role_units.Names(role_units.train_units(*real), real[1])
    # Two body rows: every set draws its rows from the same two triples.
    seed = {**units["t0"], "table_serialised": "\n".join(TABLE.splitlines()[:4])}
    for target, s in itertools.product(
        ["reference", "localization", "anchor_roi_mrs", "seed", "other", "stimulation_target"],
        range(6),
    ):
        unit, _ = role_units.table_case(seed, target, random.Random(s), names)
        assert role_units.faults({**unit, "unit_id": "u"}) == [], (target, s)


def test_a_seed_whose_sets_share_a_triple_is_redrawn_and_recorded(generated):
    units, truth, rejected = generated
    assert rejected and all(
        r["seed_unit"].startswith("dup") and "share" in r["faults"][0] for r in rejected
    )
    redrawn = {r["unit_id"] for r in rejected}
    assert redrawn <= {u["unit_id"] for u in units}
    assert all(role_units.faults(u) == [] for u in units if u["unit_id"] in redrawn)


def test_faults_finds_a_shared_triple():
    u = _table("u", "a", ["A", "B"])
    assert role_units.faults(u) and "share" in role_units.faults(u)[0]


def test_new_prose_set_never_reuses_a_seed_triple(monkeypatch):
    monkeypatch.setattr(role_units.sp, "realistic_xyz", lambda rng: [-34, 18, 2])
    for target in ("reference", "localization", "other", "hard_anchor_prior", "hard_result"):
        _, xyz, _, _ = role_units.prose_sentence(
            target, random.Random(0), "left insula", role_units.Positions([[-34, 18, 2]])
        )
        assert xyz != [-34, 18, 2]


def test_names_follow_the_real_shapes_and_tell_no_more_than_real_names(real, generated):
    units, labels = real
    gen_units, truth, _ = generated
    result = namecheck.compare(gen_units, truth, role_units.train_units(units, labels), labels)
    assert set(result) == {"table", "text"}
    assert namecheck.too_telling(result) == [], {
        o: (r["real"], r["generated"]) for o, r in result.items()
    }
    # A class with no real names (table `other`) is named like the origin as a whole.
    names = role_units.Names(role_units.train_units(units, labels), labels)
    mix, every = names.mix("table", "no such class"), names.shapes[("table", None)]
    assert mix == pytest.approx(
        {k: role_units.ALPHA * v / sum(every.values()) for k, v in every.items()}
    )


def test_a_real_name_that_reads_as_a_label_is_left_out_and_counted():
    u = _table(
        "u", _articles("train", 1, "r")[0], ["Other", "Reference (prior study)", "Controls"]
    )
    labels = {}
    for i in range(3):
        _label(labels, u, i, "other")
    names = role_units.Names({"u": u}, labels)
    assert [n for k in names.pool["table"].values() for n in k] == ["Controls"]
    assert names.left_out == {"reads as a label": 2}


def test_namecheck_flags_names_that_give_the_class_away(real, generated):
    units, labels = real
    gen_units, truth, _ = generated
    telling = json.loads(json.dumps(gen_units))
    by_id = {u["unit_id"]: u for u in telling}
    for t in truth:
        by_id[t["unit_id"]]["sets"][role_units.set_index(t["set_id"])]["name"] = (
            f"{role_units.role_class(t['role'], t['anchor_kind'])} set"
        )
    result = namecheck.compare(telling, truth, role_units.train_units(units, labels), labels)
    assert namecheck.too_telling(result) == ["table", "text"], {
        o: (r["real"], r["generated"]) for o, r in result.items()
    }


def test_rewrite_refuses_a_rewrite_that_leaks(generated):
    units, truth, _ = generated
    u = _unit(units, "table")

    def keep(caption):
        return lambda *a: (
            {"caption": caption, "footer": u["footer"], "citing": u["citing"]},
            None,
        )

    new, why = rewrite.rewrite(u, None, keep(u["caption"]))
    assert new is not None and why is None
    new, why = rewrite.rewrite(u, None, keep(u["caption"] + " Role: reference."))
    assert new is None and "text field" in why


def test_synth_prose_uses_the_repo_coordinate_finder():
    assert synth_prose.pc is prose_passages
    assert not (SYNTH / "prose_coords.py").exists()
