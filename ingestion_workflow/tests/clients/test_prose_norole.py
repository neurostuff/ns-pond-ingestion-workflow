"""The role-free prose model: the prompt it was trained on, and what is kept of its answer."""

from __future__ import annotations

import json
from pathlib import Path

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

FIXTURE = Path(__file__).parents[1] / "data" / "prose" / "nu_prose_norole_prompt.json"
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


def test_the_request_asks_for_no_role(tmp_path):
    point = schema()["properties"]["analyses"]["items"]["properties"]["points"]["items"]
    assert list(point["properties"]) == point["required"] == FIELDS
    assert point["additionalProperties"] is False
    settings = Settings(data_root=tmp_path / "d", cache_root=tmp_path / "c",
                        prose_model="nu-prose-norole", prose_api_base="http://localhost:8340/v1")
    passage = Passage(text="Faces > houses (40, -50, -20).", before="We showed faces.")
    request = ProseCoordinateClient(settings).request(passage, title="T", abstract="A")
    content = request["messages"][0]["content"]
    assert content.startswith(TRAINED["instruction"] + TRAINED["with_context"] + "\n\nTitle: T")
    template = json.loads(request["extra_body"]["chat_template_kwargs"]["template"])
    assert "role" not in template["analyses"][0]["points"][0]


def test_a_role_free_answer_is_parsed_point_by_point():
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
    assert got["stray_roles"] == 0


def test_a_stray_role_is_ignored_and_counted():
    """Not a filter either: a set the old model would have called a figure stays a set."""
    text = "Slices shown at (0, -52, 26) and (4, 30, 22)."
    points = [_point(0, -52, 26, None, None, role="figure"),
              _point(4, 30, 22, None, None, role="prior_study")]
    answer = {"space": "MNI", "analyses": [
        {"name": "figure display", "measure": None, "points": points}]}
    got = clean_answer(answer, text)
    (analysis,) = got["analyses"]
    # Nothing of the role reaches the set: not a key, not its name.
    assert (sorted(analysis), analysis["name"]) == (
        ["measure", "name", "points", "unwritten"], "figure display")
    assert [list(p) for p in analysis["points"]] == [FIELDS, FIELDS]
    assert [(p["x"], p["y"], p["z"]) for p in analysis["points"]] == [
        (0.0, -52.0, 26.0), (4.0, 30.0, 22.0)]
    assert got["stray_roles"] == 2


def test_the_clean_step_keeps_every_point_of_a_set_for_resolve_to_merge():
    """nmb-112: the role-free model gives each peak three times, as its t, Z and p."""
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
