"""The set-role decision model (`set_roles.decision`), loaded and run as the roles stage runs it."""

from __future__ import annotations

import json
import os
import threading

import pytest

pytest.importorskip("torch")

from ingestion_workflow.clients.model_server import ModelUnavailable  # noqa: E402
from ingestion_workflow.services.set_roles import decide, decision  # noqa: E402
from ingestion_workflow.services.set_roles.decision import Card, DecisionModel  # noqa: E402
from ingestion_workflow.tests.decision_model import write_model  # noqa: E402

TEXTS = ["[ORIGIN] table [NAME] patients", "[ORIGIN] text [NAME] amygdala seed",
         "[ORIGIN] table [NAME] Lee et al. [ROWS] -42 | 18 | 24", "[NAME] controls"]


def _load(root, **kw):
    source, why = decision.state(str(root))
    assert why is None, why
    return DecisionModel(str(root), source=source, device="cpu", **kw)


def test_a_usable_model_is_named_by_its_card_and_a_digest_of_its_files(tmp_path):
    write_model(tmp_path / "m")
    source, why = decision.state(str(tmp_path / "m"))
    assert why is None
    name, digest = source.split("+")
    assert name == "role-test@v1" and len(digest) == 12
    # Retrained heads in the same directory are a new source.
    heads = tmp_path / "m" / decision.HEADS_FILE
    heads.write_bytes(heads.read_bytes() + b" ")
    assert decision.state(str(tmp_path / "m"))[0] != source


def test_a_weights_only_rewrite_into_the_same_directory_is_a_new_source(tmp_path):
    root = write_model(tmp_path / "m")
    before = decision.state(str(root))[0]
    weights = root / "encoder" / "model.safetensors"
    stat = weights.stat()
    os.utime(weights, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000_000))
    assert decision.state(str(root))[0] != before


@pytest.mark.parametrize("card, why", [
    ({"roles": ["result", "anchor", "localization", "reference", "display"]},
     "its roles ['result', 'anchor', 'localization', 'reference', 'display'] are not"),
    ({"sources": ["this_study", "prior_study"]}, "its sources ['this_study', 'prior_study']"),
    ({"context_versions": {"table": 2, "text": 3}},
     "it was trained on context versions text 3, and this code builds text 4"),
    ({"context_versions": {"table": 2}},
     "it was trained on context versions text None, and this code builds text 4"),
])
def test_a_model_whose_answers_or_inputs_are_not_this_code_s_is_refused(tmp_path, card, why):
    write_model(tmp_path / "m", **card)
    source, reason = decision.state(str(tmp_path / "m"))
    assert source is None
    assert reason.startswith(f"the role model at {tmp_path / 'm'} cannot be used: ")
    assert why in reason


def test_no_model_or_no_card_is_a_reason_not_an_error(tmp_path):
    assert decision.state(None) == (None, "no role model is configured (role_model)")
    assert decision.state(str(tmp_path)) == (
        None, f"the role model at {tmp_path} has no decision.json")


def test_without_torch_the_model_cannot_be_used_and_says_which_extra(tmp_path, monkeypatch):
    write_model(tmp_path / "m")
    real = decision.importlib.util.find_spec
    monkeypatch.setattr(decision.importlib.util, "find_spec",
                        lambda name: None if name == "torch" else real(name))
    assert decision.state(str(tmp_path / "m"))[1] == (
        f"the role model at {tmp_path / 'm'} needs torch: install the `roles` extra")


def _probs(card, **given):
    out = {"coordinates": [0.1, 0.9], "space": [0.2, 0.8], "role": [0.6, 0.1, 0.1, 0.1, 0.1],
           "kind": [0.1, 0.7, 0.1, 0.1], "source": [0.8, 0.1, 0.1]}
    return {**out, **given}


def test_each_head_answers_its_most_probable_value_as_the_model_was_scored(tmp_path):
    card = Card.read(write_model(tmp_path / "m"))
    p = decision.answer(card, _probs(card))
    assert (p.role, p.anchor_kind, p.space, p.source) == ("result", None, "template", "this_study")
    assert p.confidence == pytest.approx(0.6)
    p = decision.answer(card, _probs(card, role=[0.1, 0.6, 0.1, 0.1, 0.1], space=[0.5001, 0.4999],
                                     source=[0.1, 0.1, 0.8]))
    assert (p.role, p.anchor_kind, p.space, p.source) == ("anchor", "seed", "other",
                                                          "atlas_or_database")
    # Not coordinates: no role, kind, space or source, and the confidence is that it is not.
    p = decision.answer(card, _probs(card, coordinates=[0.7, 0.3]))
    assert (p.role, p.anchor_kind, p.space, p.source) == (None, None, None, None)
    assert p.confidence == pytest.approx(0.7)


@pytest.mark.parametrize("favour, recorded, held", [
    ({"coordinates": True, "space": "template", "role": "anchor", "kind": "node",
      "source": "atlas_or_database"},
     {"role": "anchor", "anchor_kind": "node", "from_prior_study": False, "space": "template",
      "coordinate_source": "atlas_or_database"}, False),
    ({"coordinates": True, "space": "template", "role": "anchor", "kind": "seed",
      "source": "prior_study"},
     {"role": "anchor", "anchor_kind": "seed", "from_prior_study": True, "space": "template",
      "coordinate_source": "prior_study"}, False),
    # A rat seed: an anchor, in another frame, and held.
    ({"coordinates": True, "space": "other", "role": "anchor", "kind": "seed",
      "source": "this_study"},
     {"role": "anchor", "anchor_kind": "seed", "from_prior_study": False, "space": "other",
      "coordinate_source": "this_study"}, True),
    # A quoted peak is a prior study's whatever the source head says.
    ({"coordinates": True, "space": "template", "role": "reference", "source": "this_study"},
     {"role": "reference", "anchor_kind": None, "from_prior_study": True, "space": "template",
      "coordinate_source": "this_study"}, True),
    ({"coordinates": False},
     {"role": None, "anchor_kind": None, "from_prior_study": None, "space": None,
      "coordinate_source": None}, True),
])
def test_the_model_s_answer_is_the_recorded_role(tmp_path, favour, recorded, held):
    model = _load(write_model(tmp_path / "m", favour))
    try:
        (prediction,) = model.predict(["[ORIGIN] table [NAME] seed"])
        made = decide(prediction, source=model.source, origin="table",
                      evidence=["Seeds from Lee et al."])
    finally:
        model.stop("done")
    metadata = made.to_metadata()
    assert {k: metadata[k] for k in recorded} == recorded
    assert made.uploaded is not held
    assert metadata["prior_study_evidence"] == ([{"text": "Seeds from Lee et al."}]
                                                if recorded["from_prior_study"] else [])
    assert metadata["role_source"] == model.source
    assert metadata["role_confidence"] == pytest.approx(1.0, abs=1e-6)


def test_sets_asked_from_many_threads_share_batches_and_each_gets_its_own_answer(tmp_path):
    model = _load(write_model(tmp_path / "m"), batch_size=16)
    try:
        alone = [model.predict([t])[0] for t in TEXTS]
        # Hold the model on a first batch while every caller queues its sets.
        gate, sizes, forward = threading.Event(), [], model._forward

        def held(texts):
            sizes.append(len(texts))
            gate.wait(10)
            return forward(texts)

        model._forward = held
        results = {}
        first = threading.Thread(target=lambda: results.setdefault("first",
                                                                   model.predict(["[NAME] x"])))
        first.start()
        while not sizes:
            pass
        callers = [threading.Thread(target=lambda i=i: results.setdefault(i, model.predict(
            [TEXTS[i], TEXTS[(i + 1) % 4]]))) for i in range(4)]
        for c in callers:
            c.start()
        while model._queue.qsize() < 8:
            pass
        gate.set()
        for c in [first, *callers]:
            c.join(10)
    finally:
        model.stop("done")
    # The four callers' eight sets went through in one batch, not one per caller.
    assert sizes == [1, 8]
    for i in range(4):
        got = [(p.role, p.coordinates_probability) for p in results[i]]
        want = [(alone[j].role, alone[j].coordinates_probability) for j in (i, (i + 1) % 4)]
        assert got == [(r, pytest.approx(c, abs=1e-5)) for r, c in want]


def test_a_stopped_model_runs_nothing_more(tmp_path):
    model = _load(write_model(tmp_path / "m"))
    error = model.stop("the roles stage stopped: KeyboardInterrupt: ")
    assert isinstance(error, ModelUnavailable)
    with pytest.raises(ModelUnavailable, match="KeyboardInterrupt"):
        model.predict(TEXTS)
    # The first reason stays.
    assert str(model.stop("later")) == "the roles stage stopped: KeyboardInterrupt: "


def test_a_failure_in_the_model_stops_it_for_every_caller(tmp_path):
    model = _load(write_model(tmp_path / "m"))

    def broken(texts):
        raise RuntimeError("CUDA error: an illegal memory access")

    model._forward = broken
    with pytest.raises(ModelUnavailable, match="failed: RuntimeError: CUDA error"):
        model.predict(TEXTS)
    with pytest.raises(ModelUnavailable, match="CUDA error"):
        model.predict(TEXTS[:1])


def test_the_exported_card_is_read_back_as_written(tmp_path):
    root = write_model(tmp_path / "m")
    raw = json.loads((root / decision.CARD).read_text())
    card = Card.read(root)
    assert (card.name, card.version, card.max_length) == (raw["name"], raw["version"],
                                                          raw["max_length"])
    assert dict(card.context_versions) == decision.CONTEXT_VERSIONS
