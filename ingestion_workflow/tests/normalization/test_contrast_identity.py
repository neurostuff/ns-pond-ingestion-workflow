"""Contrast identity, assembled from each analysis's structure and its task's design.

Real 27B records with task-design v4's answers for their tasks (`data/design_records.json`).
"""
import json
from pathlib import Path

from ingestion_workflow.normalization import api, design, identity, structure

DATA = json.loads((Path(__file__).parent / "data" / "design_records.json").read_text())["studies"]


def found(study):
    record = DATA[study]["record"]
    designs = {t: design.read(a["content"]) for t, a in DATA[study]["answers"].items()}
    kinds = {c["id"]: c["kind"] for d in designs.values() for c in d["conditions"] if c["kind"]}
    return identity.identities(record, structure.assemble(record, None, kinds=kinds), designs)


def test_a_condition_contrast_manipulates_the_axis_its_sides_differ_on():
    got = found("6SKTGteXbbBs")["a_tbl0005_1"]
    assert got["kind"] == "condition_vs_condition" and got["manipulated"] == ["delay"]
    assert (got["target"], got["baseline"]) == ({"delay": ["immediate"]}, {"delay": ["delayed"]})
    assert {"axis": "phase", "value": "anticipation"} in got["shared"]
    assert got["key"] == "v2|condition_vs_condition|activation|hedtsk_monetary_incentive_delay|active_condition"
    assert got["canonical"] == "hedtsk_monetary_incentive_delay: delay=immediate > delay=delayed"
    assert got["complete"] and got["task_id"] == "tsk_mid"


def test_the_sides_are_never_turned_round():
    a, b = found("AhxosLhYA5YG")["a_t3_1"], found("AhxosLhYA5YG")["a_t3_1_2"]
    assert a["key"] == b["key"]
    assert a["target"] == b["baseline"] and a["baseline"] == b["target"]
    assert a["canonical"] != b["canonical"]


def test_a_group_by_condition_contrast_keeps_its_condition_axis():
    got = found("ya2RC6UYbibC")["a_prose_1"]
    assert got["kind"] == "group_by_condition" and got["manipulated"] == ["load"]
    assert got["canonical"].endswith("x (load=2 > load=0)")


def test_the_key_is_the_identity_less_its_manipulated_axes():
    got = found("6SKTGteXbbBs")["a_tbl0005_1"]
    assert got["key"] == identity.key_of({**got, "manipulated": ["something else"]})


def test_without_a_design_the_identity_has_no_task_and_is_incomplete():
    record = DATA["ya2RC6UYbibC"]["record"]
    got = identity.identities(record, structure.assemble(record), {})["a_prose_1"]
    assert (got["task"], got["family"], got["complete"]) == (None, None, False)
    assert got["key"] == "v2|group_by_condition|activation|none|active_condition"
    assert got["manipulated"] == []


def test_a_family_stands_in_for_a_task_the_model_left_null():
    record = DATA["6SKTGteXbbBs"]["record"]
    answer = json.loads(DATA["6SKTGteXbbBs"]["answers"]["tsk_mid"]["content"])
    designs = {"tsk_mid": design.read(json.dumps({**answer, "task": None}))}
    got = identity.identities(record, structure.assemble(record), designs)["a_tbl0005_1"]
    assert got["key"].split("|")[3] == "family:reward_and_value_choice"


def test_the_models_condition_kinds_override_the_records():
    # The record calls the vigilance blocks a control state; v4 reads them as a task of their own.
    record = DATA["AhxosLhYA5YG"]["record"]
    (answer,) = [design.read(a["content"]) for a in DATA["AhxosLhYA5YG"]["answers"].values()]
    kinds = {c["id"]: c["kind"] for c in answer["conditions"]}
    assert kinds["cond_vigilance"] == "task_state"
    (before,) = [x for x in structure.assemble(record) if x["analysis"] == "a_t3_1"]
    (after,) = [x for x in structure.assemble(record, kinds=kinds) if x["analysis"] == "a_t3_1"]
    assert before["baseline_type"] == "low_level_control"
    assert after["baseline_type"] == "active_condition"
    assert found("AhxosLhYA5YG")["a_t3_1"]["baseline_type"] == "active_condition"


def test_prompts_are_one_per_task_with_conditions():
    for study, held in DATA.items():
        assert set(api.prompts(held["record"], None, "task-design-v4")) == set(held["answers"])
