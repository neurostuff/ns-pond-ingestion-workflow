"""The fine-tuned extractor's path through create_analyses.

The document is reproduced from the trainer, so these tests pin its exact
shape. A field reordered or a label renamed is an input the model has never
seen, and nothing downstream would report it -- the answers would just get
quietly worse.
"""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from ingestion_workflow.clients.coordinate_parsing import CoordinateParsingClient
from ingestion_workflow.services.create_analyses import CreateAnalysesService


def _service(**settings):
    base = {"llm_native_schema": True, "llm_model": "nu-v19"}
    base.update(settings)
    service = CreateAnalysesService.__new__(CreateAnalysesService)
    service.settings = SimpleNamespace(**base)
    return service


def _bundle(title="A title", abstract="An abstract."):
    return SimpleNamespace(article_metadata=SimpleNamespace(title=title, abstract=abstract))


def _table(caption="Cap", footer="Foot"):
    return SimpleNamespace(caption=caption, footer=footer)


def test_the_document_is_exactly_what_the_trainer_built():
    """Reproduced from gen_eval.py's native_document(): label, colon, space;
    a blank line; then the serialised table."""
    got = _service()._build_document(_bundle(), _table(), "#A | #B\n1 | 2")
    assert got == (
        "Title: A title\n"
        "Abstract: An abstract.\n"
        "Caption: Cap\n"
        "Footer: Foot\n"
        "\n"
        "#A | #B\n1 | 2"
    )


def test_absent_fields_are_omitted_not_sent_empty():
    """Training rows omitted them, so an empty `Caption: ` line is a shape the
    model has not seen."""
    got = _service()._build_document(
        _bundle(abstract=None), _table(caption=None, footer=None), "T")
    assert got == "Title: A title\n\nT"
    assert "Caption:" not in got and "Abstract:" not in got


def test_a_missing_title_still_sends_the_line():
    """The title line is unconditional in the trainer -- only its value goes
    empty."""
    got = _service()._build_document(_bundle(title=None, abstract=None),
                                     _table(caption=None, footer=None), "T")
    assert got.startswith("Title: \n")


def test_the_document_never_states_the_space():
    """`Space:` was an input in earlier versions and is a target now. Feeding
    it back would hand the model the answer it is being scored on."""
    got = _service()._build_document(_bundle(), _table(), "T")
    assert "Space:" not in got


def test_the_schema_is_the_positional_form_the_model_was_trained_on():
    template = json.loads(CoordinateParsingClient.NATIVE_TEMPLATE)
    assert list(template) == ["space", "analyses"]
    analysis = template["analyses"][0]
    assert list(analysis) == ["name", "measure", "points"]
    # a point is a fixed tuple, not an object
    assert analysis["points"][0] == [
        "number", "number", "number",
        ["T", "Z", "F", "P", "R", "B"], "number", "integer",
    ]


def test_the_native_path_is_off_by_default():
    """Every hosted model needs the prompted rules. Defaulting to the native
    schema would silently send them a fifty-token template."""
    from ingestion_workflow.config import Settings

    assert Settings.model_fields["llm_native_schema"].default is False
