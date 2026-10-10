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
    from ingestion_workflow.models.statistics import STATISTIC_KINDS

    assert analysis["points"][0] == [
        "number", "number", "number",
        list(STATISTIC_KINDS), "number", "integer",
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
    from ingestion_workflow.clients.coordinate_parsing import CoordinateParsingClient

    client = CoordinateParsingClient.__new__(CoordinateParsingClient)
    client.default_model = "nu"
    window = CoordinateParsingClient.CONTEXT_WINDOW

    # a 12k-token document must not ask for the full budget on top of itself
    doc = "x" * 36000
    allowed = client.native_request(doc)["max_completion_tokens"]
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
    assert "if not kept:" in src


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


# -- the same table twice -------------------------------------------------

def _tbl(tmp_path, name, html):
    p = tmp_path / name
    p.write_text(html, encoding="utf-8")
    return SimpleNamespace(table_id=name.split(".")[0], raw_content_path=str(p))


def test_a_table_repeated_under_two_ids_is_extracted_once(tmp_path):
    """ACE returns the tables its parser found AND scans the document for the
    rest; the extraction-time dedupe missed pairs because ACE rewrites the
    markup it keeps. 72.6% of articles with more than one passing table
    carried the same numbers twice, so the coordinates would upload twice."""
    from ingestion_workflow.services.create_analyses import CreateAnalysesService

    rows = "<tr><td>Insula</td><td>33</td><td>20</td><td>-7</td></tr>"
    a = _tbl(tmp_path, "2.html", "<table><tbody>%s</tbody></table>" % rows)
    b = _tbl(tmp_path, "html-table-2.html",
             '<table>\n <tbody>%s</tbody>\n</table>' % rows.replace("<td>", '<td class="c">'))
    svc = CreateAnalysesService.__new__(CreateAnalysesService)
    drop = svc._redundant([a, b], "slug")
    assert len(drop) == 1
    assert drop < {"2", "html-table-2"}


def test_two_genuinely_different_tables_are_both_kept(tmp_path):
    from ingestion_workflow.services.create_analyses import CreateAnalysesService

    a = _tbl(tmp_path, "1.html",
             "<table><tbody><tr><td>A</td><td>1</td><td>2</td><td>3</td></tr></tbody></table>")
    b = _tbl(tmp_path, "2.html",
             "<table><tbody><tr><td>B</td><td>9</td><td>8</td><td>7</td></tr></tbody></table>")
    svc = CreateAnalysesService.__new__(CreateAnalysesService)
    assert svc._redundant([a, b], "slug") == set()


def test_a_table_with_too_few_numbers_is_never_dropped(tmp_path):
    """Two captions must not collapse into one."""
    from ingestion_workflow.services.create_analyses import CreateAnalysesService

    a = _tbl(tmp_path, "1.html", "<table><tbody><tr><td>Results</td></tr></tbody></table>")
    b = _tbl(tmp_path, "2.html", "<table><tbody><tr><td>Results</td></tr></tbody></table>")
    svc = CreateAnalysesService.__new__(CreateAnalysesService)
    assert svc._redundant([a, b], "slug") == set()


# -- the coordinate space -------------------------------------------------

def _collection(table_space, model_space):
    from ingestion_workflow.models import CoordinateSpace, ParseAnalysesOutput
    from ingestion_workflow.services.create_analyses import CreateAnalysesService

    svc = CreateAnalysesService.__new__(CreateAnalysesService)
    svc.settings = SimpleNamespace(llm_native_schema=True)
    table = SimpleNamespace(space=table_space, table_id="t1", table_number=1,
                            caption="", footer="", metadata={})
    coll = svc._build_collection(ParseAnalysesOutput(analyses=[]), table, None,
                                 "t1", "t1", "slug", model_space=model_space)
    return coll.coordinate_space, CoordinateSpace


def test_a_space_the_extraction_read_beats_the_model():
    """It came from the article, not from this one table."""
    got, Space = _collection(None, "TAL")
    assert got is Space.TALAIRACH
    got, Space = _collection(Space.MNI, "TAL")
    assert got is Space.MNI


def test_the_extractions_OTHER_does_not_beat_a_model_that_knew():
    """`OTHER` is the enum saying it does not know, and it is truthy, so it
    used to win the `or`. 19.3% of tables stored OTHER and 68% of those name
    MNI or Talairach in their own caption, footer or abstract -- which is
    exactly what the model reads."""
    from ingestion_workflow.models import CoordinateSpace

    got, _ = _collection(CoordinateSpace.OTHER, "MNI")
    assert got is CoordinateSpace.MNI
    got, _ = _collection(CoordinateSpace.OTHER, "TAL")
    assert got is CoordinateSpace.TALAIRACH


def test_neither_knowing_stays_OTHER():
    """Most articles never state a space anywhere the extractor can see."""
    from ingestion_workflow.models import CoordinateSpace

    got, _ = _collection(CoordinateSpace.OTHER, None)
    assert got is CoordinateSpace.OTHER
    got, _ = _collection(None, None)
    assert got is CoordinateSpace.OTHER


def test_changing_what_reaches_the_model_makes_the_corpus_stale():
    """The fingerprint named the prompt and the model but not the code that
    builds the table they read. A serialiser fix therefore left every existing
    analysis looking fresh: 1,679 articles kept a reading of tables the
    pipeline had dropped, and ~13,000 kept cells that had been fused."""
    import inspect

    from ingestion_workflow.pipeline.stages import analyses as mod

    src = inspect.getsource(mod.AnalysesStage.fingerprint_for)
    assert "EXTRACTION_VERSION" in src
    assert mod.EXTRACTION_VERSION

    class _Art:
        fingerprint = "triage-1"

    stage = mod.AnalysesStage(
        settings=SimpleNamespace(llm_model="nu-v19", llm_native_schema=True))
    before = stage.fingerprint_for(_Art())
    original = mod.EXTRACTION_VERSION
    try:
        mod.EXTRACTION_VERSION = original + ".next"
        assert stage.fingerprint_for(_Art()) != before
    finally:
        mod.EXTRACTION_VERSION = original
    assert stage.fingerprint_for(_Art()) == before


def test_an_article_with_no_analyses_anywhere_is_not_uploaded():
    """`summary.tables` counts collections, not analyses, so the upload gate
    passes an article whose every table came back empty -- 22 of 49,778 in the
    v19 corpus run. Uploading one creates a study saying the paper reports no
    coordinates, which is not what the extractor said."""
    import inspect

    from ingestion_workflow.services.upload import UploadService

    src = inspect.getsource(UploadService._build_work_item)
    assert "if not prepared_analyses and not getattr(" in src


def test_an_empty_article_is_skipped_rather_than_failed():
    """The stage did its job. A failure would be retried on every run, and
    would never succeed."""
    import inspect

    from ingestion_workflow.pipeline.stages.upload import UploadStage

    gather = inspect.getsource(UploadStage._gather)
    assert 'if not any((blob or {}).get("analyses") for blob in payload.values()):' in gather
    assert "return analyses, metadata, empty" in gather

    execute = inspect.getsource(UploadStage._execute)
    assert "status=Status.SKIPPED" in execute
    assert '"reason": "no analyses to upload"' in execute


def test_the_statistic_priority_is_one_rule_in_one_place():
    """It was written twice -- once per header cell in `fields.statistic_type`
    and once per document in `synth.statistic_named_by` -- which is why adding
    Cohen's d to one broke a test that used the other."""
    from nspond_tables.fields import STATISTIC_PRIORITY, best_of
    from nspond_tables.synth.trainset import statistic_named_by

    assert STATISTIC_PRIORITY == ("T", "Z", "D", "G", "F", "R", "B", "P")
    assert best_of({"P", "T"}) == "T"
    # the document reader resolves through the same rule, not its own copy
    assert statistic_named_by("#Region | #x | #y | #z | #t | #p (FWE)") == "T"
    assert statistic_named_by("#Region | #MNI | #p (FWE) | #Cohen's d") == "D"


def test_the_extractor_accepts_the_effect_size_kinds():
    from ingestion_workflow.services.nuextract_payload import ALLOWED_MEASURES  # noqa
    from ingestion_workflow.services import nuextract_payload

    assert nuextract_payload._STAT_KINDS == {"T", "Z", "D", "G", "F", "P", "R", "B"}


def test_the_prompt_stops_matching_a_letter_inside_a_word():
    """`contains "t"` matched Extent, Cluster and Talairach -- every table --
    so the rules discriminated nothing and the model fell back on a prior that
    the statistic is always T. It answered T in 199 of 200 parses."""
    import inspect

    from ingestion_workflow.services.create_analyses import CreateAnalysesService

    src = inspect.getsource(CreateAnalysesService._build_prompt)
    assert 'contains "t", "T"' not in src
    assert "is a COORDINATE column, never a Z statistic" in src
    assert "T, Z, D, G, F, R, B, P" in src


def test_one_contrast_reporting_both_directions_is_split_by_sign():
    """A positive and a negative statistic are different directions, and
    pooling them pools an increase with a decrease. 4.66% of corpus analyses
    carrying a statistic hold both -- 2,095 of 44,965, with 8,367 negative
    points among them."""
    from ingestion_workflow.models import Coordinate
    from ingestion_workflow.services.create_analyses import _by_direction

    def point(value):
        return Coordinate(x=1.0, y=2.0, z=3.0, statistic_value=value,
                          statistic_type="T")

    both = [point(4.2), point(-3.1), point(2.0)]
    out = _by_direction("Patients > controls", both)
    assert [n for n, _ in out] == ["Patients > controls",
                                   "Patients > controls (negative)"]
    assert [len(c) for _, c in out] == [2, 1]

    # one direction is left exactly as it was, name included
    for only in ([point(4.2), point(2.0)], [point(-4.2)], [point(None)], []):
        assert _by_direction("Main effect", only) == [("Main effect", only)]

    # A row with no statistic joins the positive half, tagged unsigned.
    out = _by_direction("A > B", [point(4.2), point(None), point(-3.1)])
    assert [[c.sign for c in half] for _, half in out] == [
        ["positive", "unsigned"], ["negative"]]


def test_the_sign_reads_the_statistic_and_not_the_name():
    """The contrast names the direction; the sign marks a sign-flipped
    statistic inside it. Reading the name would double-count the direction and
    mark every point of a `Deactivation` table, including ones whose statistic
    the paper printed as a positive magnitude."""
    import inspect

    from ingestion_workflow.services.coordinate_flags import point_sign

    params = list(inspect.signature(point_sign).parameters)
    assert params == ["statistic_value", "statistic_type"], params
    assert point_sign(-3.1) == "negative"
    assert point_sign(3.1) == "positive"
    assert point_sign(None) == "unsigned"


def test_the_output_budget_is_not_capped_below_what_the_window_affords():
    """92 of 871 benchmark tables came back severed, every one stopping
    exactly at the 4,096 cap -- while the median case held a 982-token
    document inside a 16,384-token window, so ~14,900 tokens sat unused.

    The subtraction protects the request from a 400. The cap was doing
    something else: discarding the long answers the subtraction exists for."""
    import inspect

    from ingestion_workflow.clients.coordinate_parsing import CoordinateParsingClient

    sig = inspect.signature(CoordinateParsingClient.native_request)
    assert sig.parameters["max_tokens"].default == 8192

    client = CoordinateParsingClient.__new__(CoordinateParsingClient)
    client.default_model = "nu"
    # a short document gets the whole raised cap, not the old 4,096
    assert client.native_request("x" * 300)["max_completion_tokens"] == 8192
    # and the window-aware subtraction still binds the long one
    long = "x" * (CoordinateParsingClient.CONTEXT_WINDOW * 2)
    assert client.native_request(long)["max_completion_tokens"] < 8192


def test_the_template_offers_every_kind_the_reader_accepts():
    """The template is the contract the model is given. It listed six kinds
    while `_STAT_KINDS` accepted eight, so a model trained to emit Cohen's d
    was forbidden it by its own prompt -- the same shape of contradiction that
    makes a model fuse a letter and a number into one slot."""
    import json

    from ingestion_workflow.clients.coordinate_parsing import CoordinateParsingClient
    from ingestion_workflow.services.nuextract_payload import _STAT_KINDS

    template = json.loads(CoordinateParsingClient.NATIVE_TEMPLATE)
    slot = template["analyses"][0]["points"][0][3]
    assert set(slot) == set(_STAT_KINDS), (set(slot) ^ set(_STAT_KINDS))


# -- the budget is set from the prompt's real size --------------------------

def _counting_client(per_char: float):
    """A client whose server tokenizer charges `per_char` tokens a character."""
    client = CoordinateParsingClient.__new__(CoordinateParsingClient)
    client.default_model = "nu"
    client._prompt_tokens = lambda request: int(len(request["messages"][0]["content"]) * per_char)
    return client


def test_the_budget_is_what_the_counted_prompt_leaves():
    client = _counting_client(1.0)
    window = CoordinateParsingClient.CONTEXT_WINDOW
    request = client.fit_to_window("x" * 9000)
    assert request["max_completion_tokens"] == window - 9000 - client.WINDOW_MARGIN


def test_a_prompt_that_fills_the_window_is_clipped_until_an_answer_fits():
    """Dense tables run past three characters a token: a document the estimate
    passed whole can fill the window on its own, which the server refuses."""
    client = _counting_client(1.0)
    window = CoordinateParsingClient.CONTEXT_WINDOW
    request = client.fit_to_window("x" * 30000)
    sent = len(request["messages"][0]["content"])
    assert sent < 30000
    assert sent + request["max_completion_tokens"] + client.WINDOW_MARGIN <= window
    assert request["max_completion_tokens"] >= 256


def test_without_a_token_count_the_estimate_stands():
    client = CoordinateParsingClient.__new__(CoordinateParsingClient)
    client.default_model = "nu"
    client._prompt_tokens = lambda request: None
    assert client.fit_to_window("x" * 9000) == client.native_request("x" * 9000)
