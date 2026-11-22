"""normalization.structure on real records: one round-1 analysis per behaviour
(data/structure_records.json).

Each fixture is a production extraction record trimmed to one analysis (the terms its cells
name, the conditions and groups their levels carry, the parse analysis's t/z values), on which
Claude's label (`claude`) and the assembler agree, or differ only by a definition James settled
(2026-11-20).
"""
import copy
import json
from pathlib import Path

import pytest
from ingestion_workflow.normalization import structure as S

FIXTURES = Path(__file__).parent / "data" / "structure_records.json"
CASES = {f["case"]: f for f in json.loads(FIXTURES.read_text())}


def one(case, parse="fixture"):
    f = CASES[case]
    return S.assemble(f["record"], f["parse"] if parse == "fixture" else parse)[0]


def test_condition_vs_condition_keeps_the_record_sides():
    s = one("condition_vs_condition")  # "invalid suffixes > valid suffixes", two accents each
    assert s["kind"] == "condition_vs_condition"
    assert s["target"]["conditions"] == ["cond_a1_invalid", "cond_a2_invalid"]
    assert s["baseline"]["conditions"] == ["cond_a1_valid", "cond_a2_valid"]
    assert s["baseline_type"] == "active_condition"


def test_fixation_baseline():
    s = one("condition_vs_fixation")
    assert (s["kind"], s["baseline_type"]) == ("condition_vs_baseline", "fixation")
    assert s["baseline"]["conditions"] == ["cond_fixation"]


def test_a_record_records_read_would_swap_keeps_its_direction():
    # records.read turns this record round (its higher side looks like a baseline); the paper
    # says the record is right: "retention versus manipulation"
    s = one("baseline_higher_kept")
    assert s["target"]["conditions"] == ["cond_retention"]
    assert s["baseline"]["conditions"] == ["cond_manipulation"]


def test_negative_statistics_never_turn_the_record_round():
    # the 27B extraction already orders the sides by the reported sign
    f = CASES["condition_vs_condition"]
    parse = copy.deepcopy(f["parse"])
    for pt in parse["analyses"][0]["points"]:
        for x in pt["values"]:
            x["value"] = -abs(x["value"])
    s = S.assemble(f["record"], parse)[0]
    assert s["statistics"] == "negative"
    assert s["target"]["conditions"] == ["cond_a1_invalid", "cond_a2_invalid"]


def test_statistics_sign_is_reported():
    s = one("negative_statistics")  # "Negative activations", every t below zero
    assert s["statistics"] == "negative"
    assert s["target"]["conditions"] == ["cond_storage"]


def test_group_by_condition_names_healthy_controls():
    s = one("group_by_condition")
    assert s["kind"] == "group_by_condition"
    assert s["groups"] == {"target": ["patients"], "baseline": ["healthy_controls"]}
    assert s["target"]["conditions"] == ["cond_positive_reward"]
    assert s["baseline"]["conditions"] == ["cond_neutral_feedback"]


def test_a_group_difference_on_a_condition_contrast_is_group_by_condition():
    # James, 2026-11-20: cells on a group term and a condition term are group_by_condition
    # (Claude's v2 label said group_difference); controls > methamphetamine abusers on iI > cI
    s = one("group_difference_defined_as_interaction")
    assert CASES["group_difference_defined_as_interaction"]["claude"]["kind"] == "group_difference"
    assert s["kind"] == "group_by_condition"
    assert s["groups"] == {"target": ["controls"], "baseline": ["ma_abusers"]}


def test_a_group_difference_on_one_condition_holds_the_condition():
    # young > older on one condition side only (no lower condition): g1 > g2 | a
    s = one("group_difference_one_condition")
    assert s["kind"] == "group_difference"
    assert s["groups"] == {"target": ["young_adults"], "baseline": ["older_adults"]}
    assert s["held"] == ["cond_8", "cond_minus2", "cond_minus6"]
    assert s["target"]["conditions"] == s["baseline"]["conditions"] == []
    assert s["baseline_type"] == "none"


def test_correlation_sign_is_the_covariate_cells():
    # a negative association with perceived social threat, reported with positive t values
    s = one("correlation_negative")
    assert (s["kind"], s["sign"], s["statistics"]) == ("correlation", "negative", "positive")
    assert s["covariates"]


@pytest.mark.parametrize("case", ["conjunction", "parametric", "condition_interaction"])
def test_kind_from_details_and_effect(case):
    assert one(case)["kind"] == case


def test_connectivity_is_a_measure_and_keeps_its_comparison():
    # a PPI from left DLPFC: genotype x oral contraceptive x WM load (2-back > 1-back)
    s = one("connectivity")
    assert (s["measure"], s["kind"]) == ("connectivity", "group_by_condition")
    assert s["connectivity"] == {
        "method": "ppi",
        "seeds": [{"id": "reg_dlpfc", "name": "left DLPFC"}],
        "modulatory_input": "WM load",
    }
    assert s["target"]["conditions"] == ["cond_2back"]
    assert s["baseline"]["conditions"] == ["cond_1back"]


def test_a_connectivity_correlation_is_a_correlation():
    # gPPI from the left amygdala (cocaine > neutral cues) against CTQ, within cocaine users
    s = one("connectivity_correlation")
    assert (s["measure"], s["kind"], s["sign"]) == ("connectivity", "correlation", "positive")
    assert s["connectivity"]["method"] == "gppi"
    assert s["connectivity"]["seeds"] == [{"id": "reg_amygdala_l", "name": "left amygdala"}]
    assert s["restricted_to"] == ["grp_cu"]


def test_activation_has_no_connectivity():
    s = one("condition_vs_condition")
    assert (s["measure"], s["connectivity"]) == ("activation", None)


def test_one_group_level_restricts_rather_than_compares():
    s = one("restricted_to_one_group")
    assert s["kind"] == "condition_vs_baseline"
    assert s["restricted_to"] == ["grp_pp"]
    assert s["groups"] == {"target": [], "baseline": []}
    assert s["baseline_type"] == "implicit"


def test_every_kind_is_declared():
    for f in CASES.values():
        for s in S.assemble(f["record"], f["parse"]):
            assert s["kind"] in S.KINDS
            assert s["measure"] in S.MEASURES
