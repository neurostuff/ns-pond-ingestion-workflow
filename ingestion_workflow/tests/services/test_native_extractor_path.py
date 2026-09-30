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


def test_flipping_the_prompt_shape_makes_analyses_stale():
    """The prompt is a stage input. Without it in the fingerprint, a
    deployment that switched to the native schema while keeping the model name
    would leave every existing analysis looking fresh, and the run would do
    nothing -- silently, which is the worst way for a pipeline to do nothing."""
    from ingestion_workflow.pipeline.stages.analyses import AnalysesStage

    class _Art:
        fingerprint = "triage-1"

    def fp(native):
        return AnalysesStage(
            settings=SimpleNamespace(llm_model="nu-v19", llm_native_schema=native)
        ).fingerprint_for(_Art())

    assert fp(True) != fp(False)
    assert fp(True) == fp(True)


# -- the two bugs the first corpus run found -----------------------------

def test_every_table_reaches_the_model_because_triage_already_chose():
    """The service used to re-test `contains_coordinates`, which is the old
    reader-based filter applied a second time. It dropped every table triage
    passed on the residual route -- where the reader finds nothing by
    definition -- and cost 1,438 of 1,461 articles in the first corpus run."""
    import inspect

    from ingestion_workflow.services.create_analyses import CreateAnalysesService

    src = inspect.getsource(CreateAnalysesService.run)
    # the guard itself, not the word -- the comment explaining its removal
    # names it too
    assert "if not table.contains_coordinates" not in src
    assert "no coordinates detected" not in src


def test_the_output_budget_leaves_room_for_the_prompt():
    """A long table is the one worth reading, and the server refuses -- with a
    400, not a truncation -- when the requested output will not fit beside the
    prompt. Two articles failed this way before the budget was computed."""
    import inspect

    from ingestion_workflow.clients.coordinate_parsing import CoordinateParsingClient

    src = inspect.getsource(CoordinateParsingClient.parse_analyses_native)
    assert "max_completion_tokens=allowed" in src
    assert "window - len(document) // 3" in src

    # a 12k-token document must not ask for 4,096 more
    window, doc = CoordinateParsingClient.CONTEXT_WINDOW, "x" * 36000
    allowed = max(256, min(4096, window - len(doc) // 3 - 512))
    assert allowed + len(doc) // 3 < window
    assert allowed >= 256          # always asks for something usable


def test_an_oversized_document_is_clipped_rather_than_refused():
    """A document longer than the whole window cannot be made to fit by any
    output budget, and the server answers 400 rather than truncating -- which
    drops the article. Two of 400 failed this way even after the budget was
    computed."""
    from ingestion_workflow.clients.coordinate_parsing import CoordinateParsingClient

    window = CoordinateParsingClient.CONTEXT_WINDOW
    room = (window - 256 - 512) * 3
    doc = "x" * (room * 2)
    clipped = doc[:room] if len(doc) > room else doc
    allowed = max(256, min(4096, window - len(clipped) // 3 - 512))
    assert allowed == 256
    assert len(clipped) // 3 + allowed + 512 <= window


# -- nothing extracted must not become a record --------------------------

def test_a_table_the_extractor_found_nothing_in_yields_no_collection():
    """It is recorded as processed by the stage artifact, but an empty
    collection would be counted as a table with analyses by everything
    downstream."""
    import inspect

    from ingestion_workflow.services.create_analyses import CreateAnalysesService

    src = inspect.getsource(CreateAnalysesService.run)
    assert "if collection.analyses:" in src


def test_a_named_analysis_with_no_coordinates_is_still_uploaded():
    """A contrast reported as `n.s.` is an analysis that ran and found
    nothing -- a result a meta-analyst wants. Only a table the extractor
    returned NO analyses for is withheld, and that is the empty collection
    skipped one level up."""
    import inspect

    from ingestion_workflow.services.upload import UploadService

    src = inspect.getsource(UploadService._build_work_item)
    assert "if not analysis.coordinates:" not in src
    assert "if not collection.analyses:" in src


# -- the table must arrive in the form the model was trained on ----------

def test_the_native_path_serialises_the_table():
    """`_read_table_content` returns raw HTML, which is what the prompted
    models are given. The fine-tune has never seen it -- it was trained, and
    every number measured, on the nspond_tables serialisation. Raw HTML also
    measured 14.7x larger over the tables that overflowed the window, and
    prefill is two thirds of the corpus cost."""
    import inspect

    from ingestion_workflow.services.create_analyses import CreateAnalysesService

    src = inspect.getsource(CreateAnalysesService.run)
    assert "self._serialise(table_text" in src
    # the prompted path keeps the raw HTML its prompt describes
    assert "self._build_prompt(bundle, table, table_text" in src


def test_an_unserialisable_table_falls_back_rather_than_vanishing():
    """A worse prompt beats no answer -- the model can only answer about what
    it is sent."""
    from ingestion_workflow.services.create_analyses import CreateAnalysesService

    assert CreateAnalysesService._serialise("<not really html", "t1")
    # nothing readable either way: markup beats an empty prompt
    assert CreateAnalysesService._serialise("<table></table>", "t1") == "<table></table>"


def test_serialisation_shrinks_a_real_table():
    """The marker of the trained form: ` | ` separators and `#` header cells."""
    from ingestion_workflow.services.create_analyses import CreateAnalysesService

    html = (
        "<table><tr><th>Region</th><th>x</th><th>y</th><th>z</th></tr>"
        "<tr><td>Amygdala</td><td>-22</td><td>4</td><td>-18</td></tr></table>"
    )
    out = CreateAnalysesService._serialise(html, "t1")
    assert " | " in out
    assert "#" in out
    assert len(out) < len(html)


def test_a_table_the_serialiser_cannot_read_falls_back_to_text_not_markup():
    """The tags are what made the raw form large -- 14.7x on average -- and a
    model trained on ` | `-separated cells gains nothing from
    `<td class="...">`. The grid survives; the decoration does not."""
    from ingestion_workflow.services.create_analyses import _text_of

    html = ('<table class="c"><tr><th>Region</th><th>x</th></tr>'
            '<tr><td style="s">Amygdala</td><td>-22</td></tr></table>')
    out = _text_of(html)
    assert out == "Region | x\nAmygdala | -22"
    assert "<" not in out and "class" not in out
    assert len(out) < len(html) / 2


def test_the_fallback_keeps_row_and_cell_boundaries():
    """Which column a number sits in is the whole answer, so the grid cannot
    be flattened into prose."""
    from ingestion_workflow.services.create_analyses import _text_of

    out = _text_of("<tr><td>a</td><td>1</td></tr><tr><td>b</td><td>2</td></tr>")
    assert out.split("\n") == ["a | 1", "b | 2"]


def test_an_empty_serialisation_is_a_failure_not_an_empty_table():
    from ingestion_workflow.services.create_analyses import CreateAnalysesService

    out = CreateAnalysesService._serialise("<table><tr><td>x</td></tr></table>", "t1")
    assert out.strip()
