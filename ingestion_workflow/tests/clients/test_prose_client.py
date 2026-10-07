"""The prose client's request, schema and clean-up."""

from __future__ import annotations

import json

from ingestion_workflow.clients.prose_coordinates import (
    ProseCoordinateClient,
    clean_answer,
    passage_body,
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
