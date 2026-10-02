"""The server launch, and the prompt, are derived rather than remembered.

Every assertion here stands for a run that was lost. A hand-copied template
declaring the statistic slot a number voided a day of benchmark figures; a
`pkill` pattern that missed the renamed workers left four cards held by a
server nothing could see; a window typed smaller than the client's turned the
output budget into 400s on the longest tables.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from ingestion_workflow.clients.coordinate_parsing import CoordinateParsingClient
from ingestion_workflow.services import extractor_server
from ingestion_workflow.services.extractor_server import (
    GIB,
    ServerMismatch,
    ServerPlan,
    plan_parallelism,
)


def _client() -> CoordinateParsingClient:
    client = CoordinateParsingClient.__new__(CoordinateParsingClient)
    client.default_model = "nu-v21"
    return client


# -- the prompt cannot be retyped ----------------------------------------

def test_the_grammar_schema_is_derived_from_the_template():
    """Written beside the template instead of from it, the schema declared the
    statistic slot a free string while the template declared an enum. The
    model answered `"T=5.53"`, which the grammar allowed, so the type and the
    value fused into the type slot and every statistic was dropped in
    silence."""
    template = json.loads(CoordinateParsingClient.NATIVE_TEMPLATE)
    schema = CoordinateParsingClient.native_schema()

    point = schema["properties"]["analyses"]["items"]["properties"]["points"]
    slot = point["items"]["prefixItems"][3]
    assert slot == {"enum": [*template["analyses"][0]["points"][0][3], None]}
    assert "T" in slot["enum"] and "D" in slot["enum"]
    # a fused composite is not a member of the set
    assert "T=5.53" not in slot["enum"]

    assert schema["properties"]["space"] == {"enum": ["MNI", "TAL", None]}
    # the short point, with no statistic tail, is still legal
    assert point["items"]["minItems"] == 3


def test_the_request_carries_the_template_the_client_declares():
    """A benchmark that rebuilds the call measures a prompt production never
    sends. There is one builder, so there is one prompt."""
    request = _client().native_request("Title: x\n\nrow")
    sent = request["extra_body"]["chat_template_kwargs"]["template"]
    assert sent == CoordinateParsingClient.NATIVE_TEMPLATE
    assert request["temperature"] == 0.0
    # unconstrained unless asked: production does not constrain
    assert "structured_outputs" not in request["extra_body"]


def test_constrained_decoding_binds_through_structured_outputs():
    """vLLM 0.28 accepts `guided_json` in extra_body, raises nothing, and
    applies no constraint -- asked to reply with the word banana under a
    coordinate schema it still replies banana."""
    request = _client().native_request("doc", constrain=True)
    assert "guided_json" not in request["extra_body"]
    assert request["extra_body"]["structured_outputs"]["json"] == (
        CoordinateParsingClient.native_schema()
    )


# -- the launch is computed ----------------------------------------------

def test_the_weights_are_sharded_only_as_far_as_they_must_be():
    """Tensor parallelism makes every layer boundary a collective, so it is
    paid only when the weights do not otherwise fit. 7.6 GiB on four 8 GiB
    cards is two shards and two copies -- which is what was being typed."""
    assert plan_parallelism(int(7.6 * GIB), 8 * GIB, 4) == (2, 2)
    # room to spare: no sharding, four copies
    assert plan_parallelism(int(2.0 * GIB), 8 * GIB, 4) == (1, 4)
    # a card that could hold it alone still gets one copy per card
    assert plan_parallelism(int(2.0 * GIB), 8 * GIB, 1) == (1, 1)


def test_weights_that_cannot_fit_refuse_to_launch():
    """Rather than starting a server that dies on allocation several minutes
    later, with the failure buried in an engine-core traceback."""
    with pytest.raises(ServerMismatch):
        plan_parallelism(int(40 * GIB), 8 * GIB, 2)


def _plan(**kwargs) -> ServerPlan:
    base = dict(
        weights=Path("/weights/v21-merged"), served_name="nu-v21",
        host="127.0.0.1", port=8312,
        max_model_len=CoordinateParsingClient.CONTEXT_WINDOW,
        tensor_parallel=2, data_parallel=2, gpus=[0, 1, 2, 3],
        max_num_seqs=48, max_num_batched_tokens=8192,
        vllm_bin=Path("/venv/bin/vllm"),
    )
    base.update(kwargs)
    return ServerPlan(**base)


def test_the_served_window_is_the_window_the_client_subtracts_from():
    """The client computes its output budget as what is left of the window
    after the document. A server narrower than that refuses the request with a
    400 rather than truncating it, and does so on the longest tables."""
    command = _plan().command()
    window = command[command.index("--max-model-len") + 1]
    assert int(window) == CoordinateParsingClient.CONTEXT_WINDOW


def test_a_tight_card_is_promised_a_batch_it_can_keep():
    """vLLM reserves CUDA graph memory against the batch it is promised and
    takes the KV cache from the remainder, so the default promise on a card
    with 2 GiB free leaves nothing and the engine refuses to start: "No
    available memory for the cache blocks". That cost one comparison run."""
    from ingestion_workflow.services.extractor_server import plan_batching

    seqs, batched = plan_batching(2 * GIB, 16384)
    assert (seqs, batched) == (48, 8192)
    # room to spare: no reason to hold back
    assert plan_batching(20 * GIB, 16384) == (256, 16384)


def test_the_venv_holding_ninja_is_put_on_the_path():
    """The engine core compiles at startup and dies with a bare ENOENT when
    ninja is missing, several screens above anything that names it."""
    env = _plan().environment()
    assert env["PATH"].startswith("/venv/bin:")
    assert env["CUDA_VISIBLE_DEVICES"] == "0,1,2,3"


def test_stopping_matches_the_renamed_workers():
    """vLLM renames its workers to `VLLM::Worker_TP0`, so a pattern matching
    the launch command line finds the launcher and leaves four cards held."""
    import ingestion_workflow.services.extractor_server as module

    seen = []

    class Done:
        returncode = 1

    def fake_run(argv, **kwargs):
        seen.append(argv)
        return Done()

    original = module.subprocess.run
    module.subprocess.run = fake_run
    try:
        extractor_server.stop(grace=0)
    finally:
        module.subprocess.run = original

    patterns = [argv[-1] for argv in seen]
    assert "VLLM::" in patterns
    assert "vllm serve" in patterns


def test_a_server_serving_something_else_is_refused():
    """The one check that would have caught a benchmark pointed at the
    checkpoint left over from the previous run."""
    with pytest.raises(ServerMismatch, match="nu-v21"):
        extractor_server._assert_serves(
            _plan(), {"data": [{"id": "nu-v19", "max_model_len": 16384}]}
        )


def test_a_server_with_a_narrower_window_is_refused():
    with pytest.raises(ServerMismatch, match="window"):
        extractor_server._assert_serves(
            _plan(), {"data": [{"id": "nu-v21", "max_model_len": 4096}]}
        )


# -- one closed set, read by three layers --------------------------------

def test_the_template_the_reader_and_the_store_hold_the_same_kinds():
    """They held three different versions of one set. The template offered
    six, the reader accepted eight, the store accepted six again -- so a
    model trained to report Cohen's d was forbidden it by its prompt, and
    where it said D anyway `PointsValue` raised and took the table with it."""
    from ingestion_workflow.models.statistics import (
        ALLOWED_STATISTIC_KINDS,
        STATISTIC_KINDS,
    )
    from ingestion_workflow.services.nuextract_payload import _STAT_KINDS

    offered = json.loads(CoordinateParsingClient.NATIVE_TEMPLATE)
    offered = offered["analyses"][0]["points"][0][3]
    assert tuple(offered) == STATISTIC_KINDS
    assert _STAT_KINDS == frozenset(STATISTIC_KINDS)
    assert set(STATISTIC_KINDS) <= ALLOWED_STATISTIC_KINDS


def test_every_kind_the_model_may_answer_can_be_stored():
    """The round trip the benchmark broke: the extractor answered D, the
    reader passed it through, and the store rejected it."""
    from ingestion_workflow.models.analysis import PointsValue
    from ingestion_workflow.models.statistics import STATISTIC_KINDS

    for kind in STATISTIC_KINDS:
        assert PointsValue(value=1.0, kind=kind).kind == kind


def test_the_named_effect_sizes_normalise_to_their_letters():
    """The prompted path reports words, not letters, and `cohen` contains
    neither z nor t -- it used to land in OTHER."""
    from ingestion_workflow.models.statistics import normalize_statistic_kind

    assert normalize_statistic_kind("Cohen's d") == "D"
    assert normalize_statistic_kind("Hedges' g") == "G"
    assert normalize_statistic_kind("t") == "T"
