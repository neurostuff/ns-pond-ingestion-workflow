"""The `normalized` sidecars written into real records validate against the extraction record."""

import copy
import json
from importlib.resources import files
from pathlib import Path

import pytest

pytest.importorskip("pondie")
jsonschema = pytest.importorskip("jsonschema")

from ingestion_workflow.normalization import records, sidecars  # noqa: E402
from ingestion_workflow.normalization.taskcat.identity import CardBuilder  # noqa: E402

DATA = json.loads((Path(__file__).parent / "data" / "astra_records.json").read_text())
FIT = "fit-000000000000"


@pytest.fixture(scope="module")
def validator():
    schema = json.loads(
        files("study_schema").joinpath("jsonschema/extraction-record.schema.json").read_text()
    )
    return jsonschema.validators.validator_for(schema)(schema)


def errors(v, r):
    return {(tuple(e.absolute_path), e.message) for e in v.iter_errors(r)}


def written(study):
    r = copy.deepcopy(DATA[study]["record"])
    build = DATA[study]["build"]
    final = {(x["study"], x["task"]): x for x in build["task_labels"]}
    rows, _ = records.read(r, study, CardBuilder(), records.schemas(), final)
    sidecars.write_tasks(r, {t: x for (_s, t), x in final.items()}, FIT)
    sidecars.write_conditions(r, rows["conditions"], FIT)
    some = next(k for k in rows["contrasts"] if k["target"] and k["baseline"])
    assigned = {
        f"{study}/{k['analysis']}": None
        for k in rows["contrasts"]
        if k["target"] and k["baseline"]
    }
    assigned[f"{study}/{some['analysis']}"] = {
        "set": "working_memory#1",
        "label": "2-back > 0-back",
        "orient": -1,
        "effective_sign": -1,
        "similarity": 0.71,
        "n_members": 9,
    }
    sidecars.write_contrasts(r, rows["contrasts"], assigned, FIT, 0.6)
    return r, some


@pytest.mark.parametrize("study", sorted(DATA))
def test_sidecars_add_no_schema_error(study, validator):
    r, _ = written(study)
    assert json.dumps(r).count('"normalized"') > 0
    assert errors(validator, r) <= errors(validator, DATA[study]["record"])


def test_entries_say_what_was_read_and_by_which_fit():
    study = sorted(DATA)[0]
    r, some = written(study)
    label = {x["task"]: x for x in DATA[study]["build"]["task_labels"]}
    for t in r["tasks"]:
        got = {e["vocabulary"]: e for e in t["name"]["normalized"]}
        row = label[t["local_id"]]
        assert got["astra canonical task"].get("value") == row["canonical"]
        assert (
            got["astra canonical task"]["mapping_tool"]
            == f"ingestion_workflow.normalization.tasks {FIT}"
        )
        assert got["astra task family"].get("value") == row["family"]
    a = next(a for a in r["analyses"] if a["local_id"] == some["analysis"])
    got = {e["vocabulary"]: e for e in a["name"]["normalized"]}
    assert got[f"ns-pond contrast set {FIT}"]["value"] == "working_memory#1"
    assert got[f"ns-pond contrast set orientation {FIT}"]["value"] == "reversed"
    assert got["astra contrast key"]["value"] == some["key"]
    assert got["astra contrast key"]["also_read"] == ["effect", "tasks"]


def test_a_rerun_replaces_its_own_entries_only():
    study = sorted(DATA)[0]
    r, _ = written(study)
    name = r["tasks"][0]["name"]
    name["normalized"].append(
        {"vocabulary": "other", "status": "mapped", "value": "x", "mapping_tool": "someone 1"}
    )
    final = {x["task"]: x for x in DATA[study]["build"]["task_labels"]}
    sidecars.write_tasks(r, final, "fit-111111111111")
    tools = [e["mapping_tool"] for e in name["normalized"]]
    assert "someone 1" in tools
    assert not any(FIT in t for t in tools)
    assert sum("fit-111111111111" in t for t in tools) == 2
