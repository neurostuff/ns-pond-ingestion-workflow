"""nu-prose-v2: the prompt it was trained on, and what is kept of its answer."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from ingestion_workflow.clients.prose_coordinates import (
    ProseCoordinateClient,
    clean_answer,
)
from ingestion_workflow.config import Settings
from ingestion_workflow.prompts.prose_coordinates import (
    INSTRUCTION,
    PROSE_PROMPT_VERSION,
    TEMPLATE,
    WITH_CONTEXT,
    schema,
)
from ingestion_workflow.services.prose_passages import Passage

FIXTURE = Path(__file__).parents[1] / "data" / "prose" / "nu_prose_v2_prompt.json"
TRAINED = json.loads(FIXTURE.read_text(encoding="utf-8"))
FIELDS = ["x", "y", "z", "statistic", "value", "cluster_size"]


def _point(x, y, z, statistic="T", value=4.1, **extra):
    return {"x": x, "y": y, "z": z, "statistic": statistic, "value": value,
            "cluster_size": None, **extra}


def test_the_prompt_and_template_are_the_ones_the_model_was_trained_on():
    assert INSTRUCTION == TRAINED["instruction"]
    assert WITH_CONTEXT == TRAINED["with_context"]
    assert TEMPLATE == TRAINED["template"]
    # Answers read under another prompt are stale: the version moves with it.
    assert PROSE_PROMPT_VERSION == TRAINED["prompt_version"]


def test_the_request_asks_for_the_trained_fields(tmp_path):
    point = schema()["properties"]["analyses"]["items"]["properties"]["points"]["items"]
    assert list(point["properties"]) == point["required"] == FIELDS
    assert point["additionalProperties"] is False
    settings = Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c",
                        prose_model="nu-prose-v2", prose_api_base="http://localhost:8340/v1")
    passage = Passage(text="Faces > houses (40, -50, -20).", before="We showed faces.")
    request = ProseCoordinateClient(settings).request(passage, title="T", abstract="A")
    content = request["messages"][0]["content"]
    assert content.startswith(TRAINED["instruction"] + TRAINED["with_context"] + "\n\nTitle: T")
    assert request["extra_body"]["chat_template_kwargs"]["template"] == TRAINED["template"]


def test_an_answer_is_parsed_point_by_point():
    text = "Faces > houses in the FFA (40, -50, -20; t = 5.1, 120 voxels), PPA (-30, -40, -10)."
    points = [{**_point(40, -50, -20, "t", 5.1), "cluster_size": 120},
              _point(-30, -40, -10, None, None)]
    answer = {"space": "TAL", "analyses": [
        {"name": "faces > houses", "measure": "voxels", "points": points}]}
    got = clean_answer(answer, text)
    (analysis,) = got["analyses"]
    assert (got["space"], analysis["name"], analysis["measure"]) == (
        "TAL", "faces > houses", "voxels")
    assert analysis["points"] == [
        {"x": 40.0, "y": -50.0, "z": -20.0, "statistic": "T", "value": 5.1,
         "cluster_size": 120},
        {"x": -30.0, "y": -40.0, "z": -10.0, "statistic": None, "value": None,
         "cluster_size": None}]


def test_the_clean_step_keeps_every_point_of_a_set_for_resolve_to_merge():
    """nmb-112: nu-prose-v2 gives each peak three times, as its t, Z and p."""
    case = json.loads((FIXTURE.parent / "nmb_112.json").read_text(encoding="utf-8"))
    (analysis,) = clean_answer(case["answer"], case["text"])["analyses"]
    assert [(p["x"], p["y"], p["z"], p["statistic"]) for p in analysis["points"]] == [
        (x, y, z, s) for x, y, z in [(-12, -2, -16), (-60, -42, 4), (64, -30, -10)]
        for s in ("T", "Z", "P")]


def test_a_point_repeated_across_sets_stays_in_each():
    """Two contrasts may report the same peak: each keeps it."""
    text = "Talairach peak x, y, z: 50, -50, 15 for the interaction, the groups and the effect."
    names = ["Affect x Group interaction", "libertarians > determinists", "main effect of Affect"]
    answer = {"space": "TAL", "analyses": [
        {"name": name, "measure": None, "points": [_point(50, -50, 15)]} for name in names]}
    got = clean_answer(answer, text)
    assert [a["name"] for a in got["analyses"]] == names
    assert [[(p["x"], p["y"], p["z"]) for p in a["points"]] for a in got["analyses"]] == [
        [(50, -50, 15)]] * 3


def _answering(tmp_path, *, content, finish_reason="stop", choices=True):
    """A prose client whose server answers `content` once."""
    from types import SimpleNamespace

    settings = Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c",
                        prose_model="nu-prose-v2", prose_api_base="http://localhost:8340/v1")
    client = ProseCoordinateClient(settings)
    choice = SimpleNamespace(message=SimpleNamespace(content=content), finish_reason=finish_reason)
    response = SimpleNamespace(choices=[choice] if choices else [])
    client.client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(
        create=lambda **kw: response)))
    return client


@pytest.mark.parametrize("content,finish_reason,choices", [
    pytest.param(None, "stop", True, id="no content"),
    pytest.param("", "stop", True, id="empty"),
    pytest.param("  \n", "stop", True, id="whitespace"),
    pytest.param(None, "stop", False, id="no choice"),
    pytest.param('{"space": "MNI", "analyses": [{"name": "a > b", "points": [', "length", True,
                 id="cut off at the token limit"),
    pytest.param('{"space": "MNI", "analyses": []}', "length", True, id="length, even if whole"),
    pytest.param("not json", "stop", True, id="unparseable"),
    pytest.param("null", "stop", True, id="null"),
    pytest.param('{"space": null}', "stop", True, id="no analyses list"),
])
def test_only_a_whole_answer_with_its_analyses_is_a_reading(tmp_path, content, finish_reason,
                                                             choices):
    """Nothing, a cut-off answer or anything not an object with its list of analyses fails the
    read; it never stands for "the passage holds no coordinate"."""
    client = _answering(tmp_path, content=content, finish_reason=finish_reason, choices=choices)
    with pytest.raises(ValueError):
        client.read(Passage(text="Faces > houses (40, -50, -20)."))


def test_an_empty_list_of_analyses_is_the_reading_that_finds_nothing(tmp_path):
    client = _answering(tmp_path, content='{"space": null, "analyses": []}')
    assert client.read(Passage(text="Faces > houses (40, -50, -20).")) == {
        "space": None, "analyses": []}
