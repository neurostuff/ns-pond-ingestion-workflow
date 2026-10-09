"""The two flags a table decides for itself."""

from __future__ import annotations

from ingestion_workflow.services.coordinate_flags import (
    point_sign,
    reports_extent,
    subpeak_flags,
)


# -- point_sign -----------------------------------------------------------

def test_a_negative_statistic_is_negative():
    assert point_sign(-4.31) == "negative"
    assert point_sign(4.31) == "positive"


def test_no_statistic_is_unsigned():
    """Absence of evidence. A row with no statistic says nothing about
    direction; in a split it joins the positive half, and the tag says so."""
    assert point_sign(None) == "unsigned"


def test_zero_is_positive():
    """`< 0`, not `<= 0`: study_schema's PointSign puts zero with the positive
    half. A zero statistic is not a decrease."""
    assert point_sign(0.0) == "positive"


def test_an_unparseable_statistic_is_unsigned():
    """The field reaches here from a model, so it can hold anything. It must
    not raise in the middle of converting an article."""
    assert point_sign("n.s.") == "unsigned"
    assert point_sign(float("nan")) == "unsigned"


def test_a_p_value_or_an_f_has_no_direction():
    """Positive whichever way the contrast runs, so reading a sign off one
    would place every row of a p-only table in the positive half as if it
    were known to be there."""
    assert point_sign(0.001, "P") == "unsigned"
    assert point_sign(12.0, "F") == "unsigned"
    assert point_sign(-3.0, "t") == "negative"


def test_a_magnitude_only_table_yields_no_negatives():
    """Many papers print unsigned magnitudes and put the direction in the
    contrast name. Inferring a direction from an unsigned number would
    invent a result."""
    assert [point_sign(v, "T") for v in (3.1, 4.8, 2.2)] == ["positive"] * 3


# -- is_subpeak -----------------------------------------------------------

def test_a_row_without_extent_is_a_subpeak_when_others_have_one():
    """A table that reports extent prints it once per cluster, on the peak,
    and leaves it blank for the local maxima that follow."""
    assert subpeak_flags([218, None, None, 57]) == [False, True, True, False]


def test_a_table_that_never_reports_extent_has_no_subpeaks():
    """Otherwise every row of every table without a size column becomes a
    subpeak -- which is most of the corpus, and wrong. A table of peaks with
    no sizes is a table of peaks."""
    assert subpeak_flags([None, None, None]) == [False, False, False]


def test_a_table_that_reports_extent_on_every_row_has_no_subpeaks():
    assert subpeak_flags([12, 34, 56]) == [False, False, False]


def test_the_decision_is_taken_over_the_whole_analysis_not_per_row():
    """This is the reason the flag cannot be computed one point at a time:
    the same blank extent means subpeak in the first analysis and nothing in
    the second."""
    assert subpeak_flags([100, None]) == [False, True]
    assert subpeak_flags([None, None]) == [False, False]


def test_no_points_is_not_an_error():
    assert subpeak_flags([]) == []
    assert reports_extent([]) is False


def test_reports_extent_needs_only_one():
    assert reports_extent([None, None, 5]) is True
    assert reports_extent([None, None]) is False


# -- the retired flags ----------------------------------------------------

def test_a_payload_written_with_the_retired_flags_still_reads():
    """Every analysis and extraction stored before the flags were retired
    carries `is_deactivation` and `is_seed`. They are ignored, and the sign is
    derived from the statistic, so the old payload reads as a new one."""
    from ingestion_workflow.models import Coordinate, ExtractedTable

    old = {"x": 1.0, "y": 2.0, "z": 3.0, "space": "MNI", "statistic_value": -2.5,
           "statistic_type": "T", "cluster_size": None, "cluster_measure": None,
           "is_subpeak": False, "is_deactivation": True, "is_seed": True}
    coord = Coordinate.from_dict(old)
    assert coord.sign == "negative"
    assert not {"is_deactivation", "is_seed"} & set(coord.to_dict())

    table = ExtractedTable.from_dict(
        {"table_id": "t", "raw_content_path": "t.html", "coordinates": [old]})
    assert table.coordinates[0].sign == "negative"


def test_the_prompted_answer_survives_a_retired_flag():
    """The function schema no longer has the flags, but a model may still
    send one; an unexpected key would fail the whole table."""
    from types import SimpleNamespace

    from ingestion_workflow.clients.coordinate_parsing import CoordinateParsingClient

    arguments = ('{"analyses": [{"name": "a", "points": [{"coordinates": [1, 2, 3], '
                 '"is_deactivation": true, "is_seed": false}]}]}')
    message = SimpleNamespace(function_call=SimpleNamespace(arguments=arguments))
    response = SimpleNamespace(choices=[SimpleNamespace(message=message)])
    client = CoordinateParsingClient.__new__(CoordinateParsingClient)
    client.settings = None
    client.default_model = "m"
    client.client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(
        create=lambda **kwargs: response)))
    out = client.parse_analyses("prompt")
    assert [len(a.points) for a in out.analyses] == [1]

