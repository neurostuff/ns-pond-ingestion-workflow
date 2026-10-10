"""The prose client's request, schema and clean-up."""

from __future__ import annotations

import json

from ingestion_workflow.clients.prose_coordinates import (
    ProseCoordinateClient,
    clean_answer,
    numbers_in,
    passage_body,
    written,
)
from ingestion_workflow.config import Settings
from ingestion_workflow.models.statistics import STATISTIC_KINDS
from ingestion_workflow.prompts.prose_coordinates import MAX_ANALYSES, MAX_POINTS, ROLES, TEMPLATE, schema
from ingestion_workflow.services.prose_passages import Passage

PASSAGE = Passage(text="Faces > houses in the FFA (40, -50, -20; t = 5.1).", before="We showed faces.",
                  after="Done.", heading="Results")


def test_the_schema_is_the_template_with_every_key_required_and_capped():
    s = schema()
    analyses = s["properties"]["analyses"]
    point = analyses["items"]["properties"]["points"]["items"]
    template_point = json.loads(TEMPLATE)["analyses"][0]["points"][0]
    assert point["required"] == list(template_point)
    assert point["additionalProperties"] is False
    assert point["properties"]["statistic"]["enum"] == [*STATISTIC_KINDS, None]
    assert point["properties"]["role"]["enum"] == [*ROLES, None]
    assert analyses["maxItems"] == MAX_ANALYSES
    assert analyses["items"]["properties"]["points"]["maxItems"] == MAX_POINTS
    assert analyses["items"]["properties"]["name"]["maxLength"]


def test_a_looping_answer_and_context_coordinates_are_dropped():
    point = {"x": 40, "y": -50, "z": -20, "statistic": "t", "value": 5.1, "cluster_size": None, "role": "result"}
    outside = {**point, "x": 12, "y": 14, "z": 16}
    answer = {"space": "MNI", "analyses": [
        {"name": "faces > houses", "measure": None, "points": [point, outside]},
        {"name": "faces > houses", "measure": None, "points": [point, outside]},
        {"name": "other", "measure": None, "points": [{**point, "role": "bogus"}]},
    ]}
    got = clean_answer(answer, PASSAGE.text)
    assert [a["name"] for a in got["analyses"]] == ["faces > houses", "other"]
    assert got["analyses"][0]["points"] == [{**point, "x": 40.0, "y": -50.0, "z": -20.0, "statistic": "T"}]
    assert got["analyses"][1]["points"][0]["role"] == "other"
    assert [a["unwritten"] for a in got["analyses"]] == [1, 0]


def test_a_named_contrast_the_model_gave_no_point_is_kept():
    """"No significant activation for PO > PC": the contrast was run and is kept, empty."""
    answer = {"space": "MNI", "analyses": [{"name": "PO > PC", "measure": None, "points": []}]}
    got = clean_answer(answer, PASSAGE.text)
    assert [(a["name"], a["points"], a["unwritten"]) for a in got["analyses"]] == [("PO > PC", [], 0)]
    assert got["omitted"] == []


def test_a_named_analysis_whose_every_point_is_unwritten_is_omitted_not_kept_empty():
    """Peaks read from the context around the passage: kept empty, they would read as n.s."""
    point = {"x": 40, "y": -50, "z": -20, "statistic": "t", "value": 5.1,
             "cluster_size": None, "role": "result"}
    nowhere = {**point, "x": 12, "y": 14, "z": 16}
    answer = {"space": "MNI", "analyses": [
        {"name": "PC > PO", "measure": None, "points": [nowhere]},
        {"name": "Patients > Controls", "measure": None, "points": [{"x": "n/a", "y": 1, "z": 2}]},
        {"name": None, "measure": None, "points": [nowhere, {**nowhere, "x": 2}]},
        {"name": None, "measure": None, "points": []},
    ]}
    got = clean_answer(answer, PASSAGE.text)
    assert got["analyses"] == []
    assert got["omitted"] == [
        {"name": "PC > PO", "unwritten": 1, "reason": "points not in passage"},
        {"name": "Patients > Controls", "unwritten": 1, "reason": "points not in passage"},
        {"name": None, "unwritten": 2, "reason": "points not in passage"},
        {"name": None, "unwritten": 0, "reason": "no name and no points"}]


def test_a_point_made_of_numbers_that_are_not_a_coordinate_is_dropped():
    text = "thinning in the superior midfrontal cortex (BA 8, 32, 34) and the precentral region (BA 9, 47)."
    assert not written((47, 47, 47), numbers_in(text))
    assert not written((32, 34, 34), numbers_in(text))
    assert not written((2.96, 2.96, 2.96), numbers_in("higher [t(14) = 2.96, P < 0.05] in precuneus"))
    # signs dropped, a minus glued to a digit, a statistic or a list joiner between
    assert written((-6, -8, 22), numbers_in("left ventral striatum (MNI -6–8 22)"))
    assert written((-30, 22, -8), numbers_in("(Left: −30, 22, −8, max z: 3.11)"))
    assert written((36, -75, 33), numbers_in("(−30, −81, 30 and 36, −75, 33)"))
    assert written((-48, -34, 42), numbers_in("left IPL (−48, −34, and 42)"))


def test_the_document_carries_the_context_asked_for():
    assert passage_body(PASSAGE, "P") == PASSAGE.text
    assert passage_body(PASSAGE, "H").startswith("Section: Results\n\n")
    wide = passage_body(PASSAGE, "HW")
    assert "Context before: We showed faces." in wide and f"Passage: {PASSAGE.text}" in wide


def test_the_request_constrains_decoding_to_the_schema(tmp_path):
    settings = Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c", llm_api_key="x",
                        prose_model="nu-base", prose_api_base="http://127.0.0.1:8313/v1")
    request = ProseCoordinateClient(settings).request(PASSAGE, title="T", abstract="A")
    assert request["model"] == "nu-base"
    assert request["extra_body"]["structured_outputs"] == {"json": schema()}
    assert request["extra_body"]["chat_template_kwargs"]["template"] == TEMPLATE
    assert "use the context to name" in request["messages"][0]["content"]
