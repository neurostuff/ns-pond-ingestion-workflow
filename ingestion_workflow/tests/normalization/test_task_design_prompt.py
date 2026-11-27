"""The task-design prompt is the one v4 was trained on, byte for byte.

Fixtures: five task-design v4 val rows (round 4 `ft-v4/val_ft.jsonl`), each with its labelling item, the
training prompt's task part, the record the item was built from (cut to what the item reads) and the parse text
in its excerpt windows (`experiments/normalization_v2/make_prompt_fixtures.py`). The whole val split was checked
the same way over the production catalog (`prompt_parity.py`): 259 of 260 identical, the other's sync has since
failed, so it has no text to cut an excerpt from.
"""
import hashlib
import json
from pathlib import Path

import pytest
from ingestion_workflow.normalization import design

FIXTURES = json.loads((Path(__file__).parent / "data" / "task_design_prompts.json").read_text("utf-8"))
ROWS = FIXTURES["rows"]


def blanked(held):
    chars = [" "] * held["length"]
    for start, part in held["windows"]:
        chars[start:start + len(part)] = part
    return "".join(chars)


def test_prefix_is_the_training_prefix():
    raw = design.prefix("task-design-v4").encode("utf-8")
    assert hashlib.sha256(raw).hexdigest() == design.MODELS["task-design-v4"][1]
    assert raw.startswith(b"Label this task from a neuroimaging paper's extraction record by its design.")
    assert raw.endswith(b"\n\nTHE TASK")


def test_template_is_the_training_template():
    assert design.template() == FIXTURES["template"]


def test_an_unknown_served_model_has_no_prompt():
    with pytest.raises(ValueError, match="no prompt for served model"):
        design.prefix("task-design-v5")


@pytest.mark.parametrize("row", ROWS, ids=[r["id"] for r in ROWS])
def test_item_gives_the_training_prompt(row):
    assert design.prompt_of(row["item"]) == design.prefix("task-design-v4") + "\n" + row["document"]


@pytest.mark.parametrize("row", ROWS, ids=[r["id"] for r in ROWS])
def test_record_gives_the_training_item(row):
    task = row["record"]["tasks"][0]
    assert design.item_of(row["record"], task, blanked(row["text"])) == row["item"]


def test_fixtures_cover_every_part_of_the_document():
    covered = {f for r in ROWS for f in r["features"]}
    assert {"terms", "no terms", "analyses", "excerpt", "stimulus list"} <= covered


def test_no_text_means_no_excerpt():
    row = next(r for r in ROWS if "excerpt" in r["features"])
    item = design.item_of(row["record"], row["record"]["tasks"][0], None)
    assert item["excerpt"] == "" and item["conditions"] == row["item"]["conditions"]


def test_request_is_the_serving_request():
    body = design.request("PROMPT", "task-design-v4")
    assert body["messages"] == [{"role": "user", "content": "PROMPT"}]
    assert body["temperature"] == 0.0 and body["max_completion_tokens"] == 4096
    assert body["chat_template_kwargs"] == {"template": FIXTURES["template"], "enable_thinking": False}
    assert "response_format" not in body


@pytest.mark.parametrize("row", ROWS, ids=[r["id"] for r in ROWS])
def test_answer_reads_as_the_scorer_read_it(row):
    got, scored = design.read(row["answer"]), row["scored"]
    assert (got["task"], got["family"], got["task_confidence"]) == \
        (scored["task"], scored["family"], scored["task_confidence"])
    assert [(f["axis"], f["levels"]) for f in got["factors"]] == \
        [(f["axis"], f["levels"]) for f in scored["factors"]]
    assert {(x["axis"], x["value"]) for x in got["context"]} == {(x["axis"], x["value"]) for x in scored["context"]}
    for mine, theirs in zip(got["conditions"], scored["conditions"]):
        assert (mine["id"], mine["kind"]) == (theirs["id"], theirs["kind"])
        assert {(a["axis"], a["value"]) for a in mine["atoms"]} == {(a["axis"], a["value"]) for a in theirs["atoms"]}


def test_answer_off_the_lists_reads_as_none():
    got = design.read(json.dumps({"task": "not_a_task", "family": "nope", "task_confidence": "sure",
                                  "conditions": [{"id": "c1", "kind": "odd", "atoms": [{"axis": "Load", "value": "2-back"}]}]}))
    assert got["task"] is None and got["task_off_inventory"] == "not_a_task"
    assert got["family"] is None and got["task_confidence"] is None
    assert got["conditions"] == [{"id": "c1", "kind": None, "atoms": [{"axis": "load", "value": "2_back"}],
                                  "expansion": None, "gloss": None}]


def test_an_answer_that_is_not_an_object_is_refused():
    with pytest.raises(ValueError):
        design.read("[1, 2]")
