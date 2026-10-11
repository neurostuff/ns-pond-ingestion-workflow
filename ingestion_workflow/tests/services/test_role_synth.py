"""The synthetic unit generator (experiments/role_classifier/synth) builds units the repo's own
context builders read, labels them by construction, and leaks no label into what they render."""

import json
import random
import sys
from pathlib import Path

import pytest

SYNTH = Path(__file__).resolve().parents[3] / "experiments" / "role_classifier" / "synth"
sys.path.insert(0, str(SYNTH))

import leakcheck  # noqa: E402
import role_units  # noqa: E402

from ingestion_workflow.services.set_roles import labeling, prose_context, table_context  # noqa: E402
from ingestion_workflow.services.set_roles.labels import ANCHOR_KINDS, COORDINATE_ROLES  # noqa: E402

TABLE = "\n".join(
    ["#Region | #<3:MNI coordinates | #t", "~ | #x | #y | #z | ~"]
    + [f"Left inferior frontal gyrus | {-40 + i} | {20 + i} | {10 - i} | 4.{i}" for i in range(4)]
    + [f"Right anterior insula | {38 + i} | {18 - i} | {2 + i} | 5.{i}" for i in range(4)]
)
PASSAGE = (
    "Participants viewed faces. The contrast activated the left insula (-34, 18, 2; z = 4.5). "
    "Activity in the right amygdala peaked at 22, -4, -16 (z = 4.1). No other region survived."
)


def _flat(text):
    return " ".join(text.split())


def _real_units(n=8):
    units, labels = {}, {}
    for i in range(n):
        t = {"unit_id": f"t{i}", "origin": "table", "article_id": f"a{i}", "source": "pubget", "title": f"Study {i} of MRS and EEG",
             "abstract": "Abstract.", "table_label": "Table 1", "caption": "Table 1. Peaks.", "footer": "", "citing": [],
             "table_serialised": TABLE, "sets": [{"name": "A > B", "points": [[-40, 20, 10, "T", 4.0, None]]}]}
        p = {"unit_id": f"p{i}", "origin": "text", "article_id": f"b{i}", "title": "Study", "abstract": "Abstract.",
             "heading": "Results", "text": PASSAGE, "before": "", "after": "",
             "sets": [{"name": "faces", "points": [{"xyz": [-34, 18, 2], "stat": ["Z", 4.5], "role": "result", "analysis": "x"}]}]}
        for u in (t, p):
            units[u["unit_id"]] = u
            labels[f"{u['unit_id']}#0"] = {"unit_id": u["unit_id"], "role": "result"}
    return units, labels


@pytest.fixture(scope="module")
def generated(tmp_path_factory):
    out = tmp_path_factory.mktemp("synth")
    units, labels = _real_units()
    pool = role_units.seeds(units, labels)
    plan = {
        "table": {"reference": 1, "localization": 1, "anchor_roi_mrs": 1, "other": 1, "stimulation_target": 1, "hard_result": 1},
        "text": {"reference": 1, "localization": 1, "anchor_roi_mrs": 1, "other": 1, "hard_result": 1, "hard_anchor_prior": 1},
    }
    counts = role_units.generate(pool, plan, random.Random(3), out, max_per_article=1)
    rows = lambda name: [json.loads(line) for line in (out / name).read_text().splitlines()]  # noqa: E731
    return counts, rows("units.jsonl"), rows("truth.jsonl"), out


def test_units_are_read_by_the_v4_context_builders(generated):
    counts, units, _, _ = generated
    assert counts == {"table": 6, "text": 6}
    for u in units:
        for ctx in labeling.contexts(u):
            assert ctx.points
            if u["origin"] == "text":
                assert isinstance(ctx, prose_context.ProseSetContext) and ctx.local and _flat(ctx.local) in _flat(ctx.passage)
            else:
                assert isinstance(ctx, table_context.TableSetContext) and len(ctx.rows) == len(ctx.points)
    assert prose_context.PROSE_CONTEXT_VERSION == 4


def test_no_label_leaks_into_rendered_inputs(generated):
    _, units, _, out = generated
    n, found = leakcheck.check(units)
    assert n == len(units) and found == {}
    # real dataset points carry role fields; the seed's must have been stripped
    assert all(set(p) <= {"xyz", "stat", "cluster"} for u in units if u["origin"] == "text" for s in u["sets"] for p in s["points"])


def test_leakcheck_fails_when_a_label_is_present(generated):
    _, units, _, _ = generated
    leaky = json.loads(json.dumps(units[-1]))
    leaky["sets"][0]["role"] = "reference"
    with pytest.raises(AssertionError):
        leakcheck.check([leaky])


def test_truth_uses_study_schema_roles(generated):
    _, _, truth, _ = generated
    for t in truth:
        assert t["role"] in COORDINATE_ROLES and t["role"] != "display"
        assert (t["anchor_kind"] in ANCHOR_KINDS) == (t["role"] == "anchor")


@pytest.mark.parametrize("origin", ["table", "text"])
def test_mrs_voxel_is_an_roi_anchor(generated, origin):
    _, units, truth, _ = generated
    ids = [t for t in truth if t["target"] == "anchor_roi_mrs" and t["unit_id"].split(":")[1] == origin]
    assert ids and all((t["role"], t["anchor_kind"], t["from_prior_study"]) == ("anchor", "roi", False) for t in ids)


def test_prior_study_anchor_keeps_the_anchor_role(generated):
    _, _, truth, _ = generated
    t = next(t for t in truth if t["target"] == "hard_anchor_prior")
    assert (t["role"], t["anchor_kind"], t["from_prior_study"]) == ("anchor", "roi", True) or t["anchor_kind"] == "seed"
