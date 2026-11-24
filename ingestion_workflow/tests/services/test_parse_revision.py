"""A paper's v23 coordinate parse as a revision of the v21 parse Neurostore holds.

Each pair under `data/parse_revision/<article>/` is the parse uploaded to production
Neurostore with v21 (from the 2026-11-24 pre-sync corpus backup) and the parse sync wrote
from v23 the same day.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from study_schema import layouts
from study_schema.jsonschema import load as load_json_schema
from study_schema.models import paper_parse as pp

from ingestion_workflow.services.parse_revision import (
    as_revision,
    directions,
    loose_name,
    revise,
)

DATA = Path(__file__).resolve().parents[1] / "data" / "parse_revision"


def _pair(article):
    return tuple(json.loads((DATA / article / f"{v}.coordinate_parse.json").read_text())
                 for v in ("v21", "v23"))


def _names(parse):
    return {a["key"]: a["name"] for a in parse["analyses"]}


def _verdicts(old, new):
    """{old or added name: (verdict, [new names])}."""
    before, after = _names(old), _names(new)
    return {(before.get(v["key"]) or after[v["key"]]):
            (v["verdict"], sorted(after[k] for k in v["replaced_by"]))
            for v in revise(old, new).verdicts}


def test_section_prefixed_names_are_edits_and_seed_groups_are_splits():
    """65bsuvrjsxxa's table: v23 names each analysis under its section ("ICC — HW > OB")
    and splits "OB > HW" by seed; every point lands in exactly one replacement."""
    old, new = _pair("65bsuvrjsxxa")
    found = _verdicts(old, new)
    assert found["ICC HW > OB"] == ("edit", ["ICC — HW > OB"])
    assert found["fALFF OB > HW"] == ("edit", ["fALFF — OB > HW"])
    assert found["OB > HW"] == ("split", ["L. INS seed — OB > HW", "R. DLPFC seed — OB > HW"])
    rev = revise(old, new)
    assert rev.carries_all and not rev.added


def test_a_group_split_by_comparison_is_a_split():
    old, new = _pair("36c6nlhdbfh4")
    found = _verdicts(old, new)
    assert found["Contrast: Positive–Neutral"] == (
        "split", ["Contrast: Positive–Neutral — MDD < BPD", "Contrast: Positive–Neutral — MDD < HC"])
    assert found["Contrast: Negative–Neutral"][0] == "split"
    assert len(found["Contrast: Negative–Neutral"][1]) == 5


def test_two_rois_read_as_one_are_a_merge():
    old, new = _pair("4fwdcdbdxyw5")
    found = _verdicts(old, new)
    assert found["left amygdala ROI"] == ("merge", ["amygdala ROI"])
    assert found["right amygdala ROI"] == ("merge", ["amygdala ROI"])
    merged = next(k for k, n in _names(new).items() if n == "amygdala ROI")
    assert sorted(revise(old, new).derived_from[merged]) == sorted(
        k for k, n in _names(old).items() if n in ("left amygdala ROI", "right amygdala ROI"))


def test_a_corrected_statistic_is_an_edit():
    """The key counts each point's statistic: v23 reads r where v21 read p, on the same
    points under the same name."""
    old, new = _pair("5opj6qqh4kis")
    (verdict,) = [v for v in revise(old, new).verdicts if v["reason"].startswith(
        "the same coordinates and name")]
    assert verdict["verdict"] == "edit"


def test_coordinates_two_old_analyses_share_are_not_given_to_either():
    """4l3oivy5q5on: two contrasts print the same peak, and v23 reads one "VMPFC ROI" at it.
    Neither contrast is that ROI, so both are omitted and the ROI is added."""
    old, new = _pair("4l3oivy5q5on")
    rev = revise(old, new)
    assert dict(rev.counts()) == {"omit": 2, "add": 1}
    assert not rev.carries_all


def test_a_name_whose_contrast_runs_the_other_way_is_not_an_edit():
    old, new = _pair("65bsuvrjsxxa")
    flipped = json.loads(json.dumps(new))
    for analysis in flipped["analyses"]:
        if analysis["name"] == "ICC — HW > OB":
            analysis["name"] = "ICC — HW < OB"
    assert _verdicts(old, flipped)["ICC HW > OB"][0] == "omit"
    assert directions("A > B (n = 3)") == ">" and directions("A ≤ B") == "<"
    assert loose_name("IFG (AC>DD)") == loose_name("IFG AC > DD")
    assert loose_name("A > B") != loose_name("A < B")


@pytest.mark.parametrize("article", sorted(p.name for p in DATA.iterdir() if p.is_dir()))
def test_the_revision_accounts_for_every_analysis_as_neurostore_and_study_schema_check(article):
    """study_schema's `check_revision`, but for its text rule: a v23 parse read from a text
    with a supplement appended still revises the v21 one. Neurostore checks no text."""
    old, new = _pair(article)
    revision = as_revision(new, old["parse_id"], revise(old, new))
    schema = load_json_schema("coordinate-parse")
    import jsonschema

    assert not list(jsonschema.validators.validator_for(schema)(schema).iter_errors(revision))
    problems = layouts.check_revision(pp.CoordinateParse.model_validate(old),
                                      pp.CoordinateParse.model_validate(revision))
    assert [p for p in problems if p != "the revision addresses a different text"] == []
    assert {a["key"] for a in revision["analyses"]} == {a["key"] for a in new["analyses"]}
