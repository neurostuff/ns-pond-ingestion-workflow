"""The `normalized` sidecars written into real records validate against the extraction record.

The records are three 27B records (`data/design_records.json`), each with task-design v4's answer for its task
as beast's server gave it (no text, so no excerpt in the prompt).
"""

import copy
import json
from importlib.resources import files
from pathlib import Path

import pytest

pytest.importorskip("pondie")
jsonschema = pytest.importorskip("jsonschema")

from ingestion_workflow.normalization import api, design, sidecars  # noqa: E402

DATA = json.loads((Path(__file__).parent / "data" / "design_records.json").read_text())["studies"]
MODEL, VERSION = "task-design-v4", "design-000000000000"


@pytest.fixture(scope="module")
def validator():
    schema = json.loads(
        files("study_schema").joinpath("jsonschema/extraction-record.schema.json").read_text()
    )
    return jsonschema.validators.validator_for(schema)(schema)


def errors(v, r):
    return {(tuple(e.absolute_path), e.message) for e in v.iter_errors(r)}


def answers(study):
    return {t: a["content"] for t, a in DATA[study]["answers"].items()}


def written(study, version=VERSION, given=None):
    r = copy.deepcopy(DATA[study]["record"])
    api.finish(r, given or answers(study), None, MODEL, version)
    return r


def entries(wrapper):
    return {e["vocabulary"]: e for e in wrapper.get("normalized") or []}


def test_the_answers_are_the_ones_for_these_prompts():
    import hashlib

    for study, held in DATA.items():
        for tid, prompt in api.prompts(held["record"], None, MODEL).items():
            assert hashlib.sha256(prompt.encode()).hexdigest() == held["answers"][tid]["prompt_sha256"]


@pytest.mark.parametrize("study", sorted(DATA))
def test_sidecars_add_no_schema_error(study, validator):
    r = written(study)
    assert json.dumps(r).count('"normalized"') > 0
    assert errors(validator, r) <= errors(validator, DATA[study]["record"])


def test_task_entries_say_what_the_model_answered():
    r = written("ya2RC6UYbibC")
    (task,) = design.tasks_of(r)
    got = entries(task["name"])
    assert got["astra canonical task"]["value"] == "hedtsk_n_back"
    assert got["astra canonical task"]["label"] == design.inventory()["hedtsk_n_back"]["name"]
    assert got["astra canonical task"]["method"] == MODEL
    assert got["astra canonical task"]["also_read"] == ["description", "conditions"]
    assert got["astra canonical task"]["mapping_tool"] == f"ingestion_workflow.normalization.design {VERSION}"
    assert got["astra task family"]["value"] == "working_memory_span"
    assert got["ns-pond task confidence"]["value"] == "high"
    assert got["ns-pond task factor"]["value"] == "load"
    conditions = {c["local_id"]: entries(c["name"]) for c in task["conditions"]}
    assert conditions["cond_2back"]["condition atom: load"]["value"] == "2"
    assert conditions["cond_0back"]["condition atom: load"]["value"] == "0"
    assert conditions["cond_fixation"]["ns-pond condition kind"]["value"] == "fixation"
    assert conditions["cond_fixation"]["ns-pond condition kind"]["label"] == "fixation"


def test_contrast_entries_hold_the_assembled_identity():
    r = written("ya2RC6UYbibC")
    a = next(a for a in r["analyses"] if a["local_id"] == "a_prose_1")
    got = entries(a["name"])
    assert got["ns-pond contrast key v2"]["value"] == \
        "v2|group_by_condition|activation|hedtsk_n_back|active_condition"
    assert got["ns-pond contrast key v2"]["label"].startswith("hedtsk_n_back: (")
    assert got["ns-pond contrast key v2"]["label"].endswith("x (load=2 > load=0)")
    assert got["ns-pond contrast manipulated"]["value"] == "load"
    assert got["ns-pond contrast kind"]["value"] == "group_by_condition"
    assert got["ns-pond contrast key v2"]["also_read"] == ["effect", "tasks"]


def test_a_task_with_no_answer_says_why():
    r = written("ya2RC6UYbibC", given={"tsk_wm": api.NoAnswer("no answer: cut at the length limit")})
    (task,) = design.tasks_of(r)
    (entry,) = task["name"]["normalized"]
    assert (entry["status"], entry["method"]) == ("unmapped", "no answer: cut at the length limit")
    assert all("normalized" not in c["name"] for c in task["conditions"])
    a = next(a for a in r["analyses"] if a["local_id"] == "a_prose_1")
    assert entries(a["name"])["ns-pond contrast key v2"]["value"] == \
        "v2|group_by_condition|activation|none|active_condition"


def test_an_answer_that_is_not_json_says_so():
    r = written("ya2RC6UYbibC", given={"tsk_wm": '{"factors": ['})
    (task,) = design.tasks_of(r)
    (entry,) = task["name"]["normalized"]
    assert entry["method"].startswith("no answer: not a JSON object")


def test_a_rerun_replaces_its_own_entries_only():
    r = written("ya2RC6UYbibC")
    (task,) = design.tasks_of(r)
    task["name"]["normalized"].append(
        {"vocabulary": "other", "status": "mapped", "value": "x", "mapping_tool": "someone 1"})
    api.finish(r, answers("ya2RC6UYbibC"), None, MODEL, "design-111111111111")
    tools = [e["mapping_tool"] for e in task["name"]["normalized"]]
    assert "someone 1" in tools
    assert not any(VERSION in t for t in tools)
    assert sum("design-111111111111" in t for t in tools) == len(tools) - 1
