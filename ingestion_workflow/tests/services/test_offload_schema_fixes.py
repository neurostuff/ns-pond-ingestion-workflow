"""The fixes for a 27B record written to an older schema than study_schema's.

Each record is a real one the production import refused on 2026-11-06 (run
ns-27b-e-d7d2347b1330), the smallest of those each fix saved; f2j36xnh2r7e is the smallest
no fix saves. Legacy siblings are dropped first, as import does.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest
from ingestion_workflow.pipeline.stages.offloaded import drop_siblings
from ingestion_workflow.services.offload.importer import _schema_problem
from ingestion_workflow.services.offload.schema_fixes import fix

DATA = Path(__file__).resolve().parents[1] / "data" / "offload"
RECORDS = json.loads((DATA / "schema-refused-records.json").read_text())
VALID = json.loads((DATA / "26klegjq7mhi.extraction.json").read_text())


def refused(article_id):
    record = copy.deepcopy(RECORDS[article_id])
    drop_siblings(record)
    assert _schema_problem(record)
    return record


def changes(before, after, path=""):
    """Every path where `after` differs from `before`."""
    if isinstance(before, dict) and isinstance(after, dict):
        out = [f"{path}/{k}" for k in before.keys() ^ after.keys()]
        return out + [p for k in before.keys() & after.keys()
                      for p in changes(before[k], after[k], f"{path}/{k}")]
    if isinstance(before, list) and isinstance(after, list) and len(before) == len(after):
        return [p for i, (b, a) in enumerate(zip(before, after))
                for p in changes(b, a, f"{path}/{i}")]
    return [] if before == after else [path]


def test_a_field_the_schema_no_longer_has_is_dropped():
    record = refused("dhaj42u2kyja")
    fixed, fired = fix(record)
    assert fired == ["drop_extra"] and _schema_problem(fixed) is None
    assert changes(record, fixed) == ["/model_estimations/0/source_definition"]


def test_an_id_written_as_an_object_becomes_the_id():
    record = refused("me7qqodzmq5a")
    fixed, fired = fix(record)
    assert fired == ["id_from_object"] and _schema_problem(fixed) is None
    for path in changes(record, fixed):
        node_before, node_after = record, fixed
        for step in path.strip("/").split("/"):
            step = int(step) if step.isdigit() else step
            node_before, node_after = node_before[step], node_after[step]
        assert node_after == node_before["local_id"]


def test_a_required_reference_takes_the_one_candidate_the_record_has():
    record = refused("5bqhpxh2brgm")
    fixed, fired = fix(record)
    assert fired == ["only_candidate"] and _schema_problem(fixed) is None
    (only,) = [m["local_id"] for m in fixed["model_estimations"]] or [None]
    assert {a["model_estimation"] for a in fixed["analyses"]} == {only}


def test_a_value_the_schema_does_not_allow_is_cleared_where_it_may_be_null():
    record = refused("h5745hs3o36p")
    assert record["groups"][0]["sample_source"]["value"] == "pooled from three independent cohorts"
    fixed, fired = fix(record)
    assert fired == ["clear_optional"] and _schema_problem(fixed) is None
    assert sorted(changes(record, fixed)) == ["/groups/0/sample_source/extraction_status",
                                              "/groups/0/sample_source/value"]
    assert fixed["groups"][0]["sample_source"]["extraction_status"] == "not_reported"


def _with_lists(**values):
    """26klegjq7mhi's record (valid as kept) with its MRI's list values set to `values`."""
    record = copy.deepcopy(VALID)
    drop_siblings(record)
    assert _schema_problem(record) is None
    for name, value in values.items():
        record["acquisitions"][0][name] = {"extraction_status": "extracted", "value": value,
                                           "value_source": "generated",
                                           "evidence": {"status": "not_found"}}
    return record


def test_a_number_written_as_a_string_in_a_list_value_becomes_the_number():
    record = _with_lists(acquisition_voxel_size_mm=[2, "2.5", 3], echo_time_seconds=[0.03, "0.05"])
    assert _schema_problem(record)
    fixed, fired = fix(record)
    assert fired == ["number_from_string"] and _schema_problem(fixed) is None
    mri = fixed["acquisitions"][0]
    assert mri["acquisition_voxel_size_mm"]["value"] == [2, 2.5, 3]
    assert mri["echo_time_seconds"]["value"] == [0.03, 0.05]


def test_a_list_value_is_cleared_whole_never_shortened():
    """Dropping the refused element would leave a vector of the wrong length that passes."""
    fixed, fired = fix(_with_lists(acquisition_voxel_size_mm=[2, "2.5 x 3", 3]))
    assert fired == ["clear_optional"] and _schema_problem(fixed) is None
    assert fixed["acquisitions"][0]["acquisition_voxel_size_mm"]["value"] is None
    assert fixed["acquisitions"][0]["acquisition_voxel_size_mm"]["extraction_status"] == \
        "not_reported"


def test_a_reference_list_loses_only_the_element_it_refuses():
    record = _with_lists()
    record["analyses"][0]["tasks"] = ["tsk_social", 5]
    fixed, fired = fix(record)
    assert fired == ["clear_optional"] and fixed["analyses"][0]["tasks"] == ["tsk_social"]


def test_a_second_list_beside_the_schema_s_own_is_merged_into_it():
    record = refused("k7hdanh6l63v")
    second = [t["local_id"] for t in record["tasks2"]]
    fixed, fired = fix(record)
    assert "merge_numbered_list" in fired and _schema_problem(fixed) is None
    assert "tasks2" not in fixed
    assert set(second) <= {t["local_id"] for t in fixed["tasks"]}


def test_a_required_value_with_no_candidate_leaves_the_record_refused():
    record = refused("f2j36xnh2r7e")
    assert fix(record) == (None, [])


@pytest.mark.parametrize("article_id", sorted(RECORDS))
def test_the_fixes_never_drop_an_analysis_or_a_coordinate_set(article_id):
    record = refused(article_id)
    fixed, _ = fix(record)
    if fixed is not None:
        for key in ("analyses", "coordinate_sets"):
            assert [x.get("local_id") for x in fixed.get(key) or []] == \
                [x.get("local_id") for x in record.get(key) or []]
